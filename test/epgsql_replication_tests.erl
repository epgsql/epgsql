-module(epgsql_replication_tests).

-export([run_tests/0]).
-compile([export_all]).

-include_lib("eunit/include/eunit.hrl").
-include("epgsql.hrl").

connect_in_repl_mode_test(Module) ->
    epgsql_tests:connect_only(Module, ["epgsql_test_replication",
        "epgsql_test_replication",
        [{database, "epgsql_test_db1"}, {replication, "database"}]]).

create_drop_replication_slot_test(Module) ->
    epgsql_tests:with_connection(
        Module,
        fun(C) ->
            {ok, Cols, Rows} = Module:squery(C, "CREATE_REPLICATION_SLOT ""epgsql_test"" LOGICAL ""test_decoding"""),
            [#column{name = <<"slot_name">>}, #column{name = <<"consistent_point">>},
                #column{name = <<"snapshot_name">>}, #column{name = <<"output_plugin">>}] = Cols,
            [{<<"epgsql_test">>, _, _, <<"test_decoding">>}] = Rows,
            [{ok, _, _}, {ok, _, _}] = Module:squery(C, "DROP_REPLICATION_SLOT ""epgsql_test""")
        end,
        "epgsql_test_replication",
        [{replication, "database"}]).

replication_async_test(Module) ->
    replication_test_run(Module, self()).

replication_sync_test(Module) ->
    replication_test_run(Module, ?MODULE).

%% -- run all tests --

run_tests() ->
    Files = filelib:wildcard(filename:dirname(code:which(epgsql_replication_tests))
                             ++ "/*tests.beam"),
    Mods = [list_to_atom(filename:basename(F, ".beam")) || F <- Files],
    eunit:test(Mods, []).

all_test_() ->
    Tests =
        lists:map(
          fun({Name, _}) ->
                  {Name, fun(X) -> ?MODULE:Name(X) end}
          end,
          lists:filter(
            fun({Name, Arity}) ->
                    case {lists:suffix("_test", atom_to_list(Name)), Arity} of
                        {true, 1} -> true;
                        _ -> false
                    end
            end,
            ?MODULE:module_info(functions))),
    WithModule =
        fun(Module) ->
                lists:map(
                  fun({Name, Test}) ->
                          {lists:flatten(
                             io_lib:format("~s(~s)", [Name, Module])),
                           fun() -> Test(Module) end}
                  end,
                  Tests)
        end,
    [WithModule(epgsql)
    ].

%% -- internal functions --

replication_test_run(Module, Callback) ->
    epgsql_tests:with_connection(
        Module,
        fun(C) ->
            {ok, _, _} = Module:squery(C, "CREATE_REPLICATION_SLOT ""epgsql_test"" LOGICAL ""test_decoding"""),

            %% new connection because main id in a replication mode
            epgsql_tests:with_connection(
                Module,
                fun(C2) ->
                    [{ok, 1},{ok, 1}] = Module:squery(C2,
                        "insert into test_table1 (id, value) values (5, 'five');delete from test_table1 where id = 5;")
                end,
                "epgsql_test_db1"),

            Module:start_replication(C, "epgsql_test", Callback, {C, self()}, "0/0"),
            ok = receive_replication_msgs(
                [<<"table public.test_table1: INSERT: id[integer]:5 value[text]:'five'">>,
                    <<"table public.test_table1: DELETE: id[integer]:5">>], C, [])
        end,
        "epgsql_test_replication",
        [{replication, "database"}]),
    %% cleanup
    epgsql_tests:with_connection(
        Module,
        fun(C) ->
            [{ok, _, _}, {ok, _, _}] = Module:squery(C, "DROP_REPLICATION_SLOT ""epgsql_test""")
        end,
        "epgsql_test_replication",
        [{replication, "database"}]).

receive_replication_msgs(Pattern, Pid, ReceivedMsgs) ->
    receive
        {epgsql, Pid, {x_log_data, _StartLSN, _EndLSN, <<"BEGIN", _/binary>>}} ->
            receive_replication_msgs(Pattern, Pid, [begin_msg | ReceivedMsgs]);
        {epgsql, Pid, {x_log_data, _StartLSN, _EndLSN, <<"COMMIT", _/binary>>}} ->
            case lists:reverse(ReceivedMsgs) of
                [begin_msg, row_msg | _] -> ok;
                _ -> error_replication_messages_not_received
            end;
        {epgsql, Pid, {x_log_data, _StartLSN, _EndLSN, Msg}} ->
            [Msg | T] = Pattern,
            receive_replication_msgs(T, Pid, [row_msg | ReceivedMsgs])
    after
        60000 ->
            error_timeout
    end.

handle_x_log_data(StartLSN, EndLSN, Data, CbState) ->
    {C, Pid} = CbState,
    Pid ! {epgsql, C, {x_log_data, StartLSN, EndLSN, Data}},
    {ok, EndLSN, EndLSN, CbState}.