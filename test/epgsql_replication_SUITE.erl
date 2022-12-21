-module(epgsql_replication_SUITE).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").
-include("epgsql.hrl").

-export([ all/0
        , init_per_suite/1
        , end_per_suite/1

        , connect_in_repl_mode/1
        , create_drop_replication_slot/1
        , replication_sync/1
        , replication_async/1
        , replication_async_active_n_socket/1
        , replication_sync_active_n_socket/1

          %% Callbacks
        , handle_x_log_data/4
        , handle_socket_passive/1
        ]).

init_per_suite(Config) ->
  [{module, epgsql} | Config].

end_per_suite(_Config) ->
  ok.

all() ->
  [ connect_in_repl_mode
  , create_drop_replication_slot
  , replication_async
  , replication_sync
  , replication_async_active_n_socket
  , replication_sync_active_n_socket
  ].

connect_in_repl_mode(Config) ->
  epgsql_ct:connect_only(
    Config,
    [ "epgsql_test_replication"
    , "epgsql_test_replication"
    , [{database, "epgsql_test_db1"}, {replication, "database"}]
    ]).

create_drop_replication_slot(Config) ->
  epgsql_ct:with_connection(
    Config,
    fun(C) ->
        create_replication_slot(Config, C),
        drop_replication_slot(Config, C)
    end,
    "epgsql_test_replication",
    [{replication, "database"}]).

replication_async(Config) ->
  replication_test_run(Config, self()).

replication_sync(Config) ->
  replication_test_run(Config, ?MODULE).

replication_async_active_n_socket(Config) ->
  replication_test_run(Config, self(), [{tcp_opts, [{active, 1}]}, {ssl_opts, [{active, 1}]}]).

replication_sync_active_n_socket(Config) ->
  replication_test_run(Config, ?MODULE, [{tcp_opts, [{active, 1}]}, {ssl_opts, [{active, 1}]}]).

replication_test_run(Config, Callback) ->
  replication_test_run(Config, Callback, []).

replication_test_run(Config, Callback, ExtOpts) ->
  Module = ?config(module, Config),
  {Queries, ReplicationMsgs} = gen_query_and_replication_msgs(lists:seq(100, 110)),
  epgsql_ct:with_connection(
    Config,
    fun(C) ->
        create_replication_slot(Config, C),
        %% new connection because main is in the replication mode
        epgsql_ct:with_connection(
          Config,
          fun(C2) ->
              ExpectedResult = lists:duplicate(length(Queries), {ok, 1}),
              Res = Module:squery(C2, lists:flatten(Queries)),
              ?assertEqual(ExpectedResult, Res)
          end),
        Module:start_replication(C, "epgsql_test", Callback, {C, self()}, "0/0"),
        ok = receive_replication_msgs(Module, ReplicationMsgs, C, [])
    end,
    "epgsql_test_replication",
    [{replication, "database"} | ExtOpts]),
  %% cleanup
  epgsql_ct:with_connection(
    Config,
    fun(C) -> drop_replication_slot(Config, C) end,
    "epgsql_test_replication",
    [{replication, "database"}]).

create_replication_slot(Config, Connection) ->
  Module = ?config(module, Config),
  {ok, Cols, Rows} =
    Module:squery(Connection,
                  "CREATE_REPLICATION_SLOT ""epgsql_test"" LOGICAL ""test_decoding"""),
  ?assertMatch([ #column{name = <<"slot_name">>}
               , #column{name = <<"consistent_point">>}
               , #column{name = <<"snapshot_name">>}
               , #column{name = <<"output_plugin">>}
               ],
               Cols),
  ?assertMatch([{<<"epgsql_test">>, _, _, <<"test_decoding">>}], Rows).

drop_replication_slot(Config, Connection) ->
  Module = ?config(module, Config),
  Result = Module:squery(Connection, "DROP_REPLICATION_SLOT ""epgsql_test"""),
  case ?config(version, ?config(pg_config, Config)) >= [13, 0] of
    true -> ?assertMatch({ok, _, _}, Result);
    false -> ?assertMatch([{ok, _, _}, {ok, _, _}], Result)
  end.

gen_query_and_replication_msgs(Ids) ->
  QInsFmt = "INSERT INTO test_table1 (id, value) VALUES (~b, '~s');",
  QDelFmt = "DELETE FROM test_table1 WHERE id = ~b;",
  RmInsFmt = "table public.test_table1: INSERT: id[integer]:~b value[text]:'~s'",
  RmDelFmt = "table public.test_table1: DELETE: id[integer]:~b",
  LongBin = base64:encode(crypto:strong_rand_bytes(254)),
  lists:foldl(
    fun(Id, {Qs, RMs}) ->
        QIns = lists:flatten(io_lib:format(QInsFmt, [Id, LongBin])),
        QDel = lists:flatten(io_lib:format(QDelFmt, [Id])),
        RmIns = iolist_to_binary(io_lib:format(RmInsFmt, [Id, LongBin])),
        RmDel = iolist_to_binary(io_lib:format(RmDelFmt, [Id])),
        {Qs ++ [QIns, QDel], RMs ++ [RmIns, RmDel]}
    end,
    {[], []},
    Ids).

receive_replication_msgs(Module, Pattern, Pid, ReceivedMsgs) ->
  receive
    {epgsql, Pid, {x_log_data, _StartLSN, _EndLSN, <<"BEGIN", _/binary>>}} ->
      receive_replication_msgs(Module, Pattern, Pid, [begin_msg | ReceivedMsgs]);
    {epgsql, Pid, {x_log_data, _StartLSN, _EndLSN, <<"COMMIT", _/binary>>}} ->
      ensure_no_socket_passive_msgs(Module, Pid),
      case lists:reverse(ReceivedMsgs) of
        [begin_msg, row_msg | _] -> ok;
        _ -> error_replication_messages_not_received
      end;
    {epgsql, Pid, {x_log_data, _StartLSN, _EndLSN, Msg}} ->
      [Msg | T] = Pattern,
      receive_replication_msgs(Module, T, Pid, [row_msg | ReceivedMsgs]);
    {epgsql, Pid, socket_passive} ->
      Module:activate(Pid),
      receive_replication_msgs(Module, Pattern, Pid, ReceivedMsgs)
  after
    60000 ->
      error_timeout
  end.

ensure_no_socket_passive_msgs(Module, Pid) ->
  receive
    {epgsql, Pid, socket_passive} ->
      Module:activate(Pid),
      ensure_no_socket_passive_msgs(Module, Pid)
  after
    100 ->
      ok
  end.

handle_x_log_data(StartLSN, EndLSN, Data, CbState) ->
  {C, Pid} = CbState,
  Pid ! {epgsql, C, {x_log_data, StartLSN, EndLSN, Data}},
  {ok, EndLSN, EndLSN, CbState}.

handle_socket_passive({C, _Pid} = CbState) ->
  spawn(fun() -> epgsql:activate(C) end),
  {ok, CbState}.
