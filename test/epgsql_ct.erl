-module(epgsql_ct).
-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").
-include("epgsql.hrl").

-export([
    connection_data/1,
    connect/1,
    connect_only/2,
    with_connection/2,
    with_connection/3,
    with_connection/4,
    with_rollback/2,
    with_min_version/4,
    flush/0,
    create_replication_slot/2,
    drop_replication_slot/1,
    drop_replication_slot/2
]).

connection_data(Config) ->
    PgConfig = ?config(pg_config, Config),
    Host = ?config(host, PgConfig),
    Port = ?config(port, PgConfig),
    {Host, Port}.

connect(Config) ->
    connect(Config, "epgsql_test", []).

connect(Config, Username, Args) ->
    {Host, Port} = connection_data(Config),
    Module = ?config(module, Config),
    Args2 = [{port, Port}, {database, "epgsql_test_db1"} | Args],
    {ok, C} = Module:connect(Host, Username, Args2),
    C.

connect_only(Config, Args) ->
    {Host, Port} = connection_data(Config),
    Module = ?config(module, Config),
    TestOpts = [{port, Port}],
    case Args of
        [User, Opts]       -> Args2 = [User, TestOpts ++ Opts];
        [User, Pass, Opts] -> Args2 = [User, Pass, TestOpts ++ Opts];
        Opts               -> Args2 = [TestOpts ++ Opts]
    end,
    {ok, C} = apply(Module, connect, [Host | Args2]),
    Module:close(C),
    flush().

with_connection(Config, F) ->
    with_connection(Config, F, "epgsql_test", []).

with_connection(Config, F, Args) ->
    with_connection(Config, F, "epgsql_test", Args).

with_connection(Config, F, Username, Args) ->
    Module = ?config(module, Config),
    C = connect(Config, Username, Args),
    try
        F(C)
    after
        Module:close(C)
    end,
    flush().

with_rollback(Config, F) ->
    Module = ?config(module, Config),
    with_connection(
      Config,
      fun(C) ->
              try
                  Module:squery(C, "begin"),
                  F(C)
                  after
                      Module:squery(C, "rollback")
                  end
      end).

with_min_version(Config, Min, F, Args) ->
    PgConf = ?config(pg_config, Config),
    Ver = ?config(version, PgConf),

    case Ver >= Min of
        true ->
            epgsql_ct:with_connection(Config, F, Args);
        false ->
            ?debugFmt("skipping test requiring PostgreSQL >= ~p, but we have ~p ~p",
                      [Min, Ver, Config])
    end.

%% flush mailbox
flush() ->
    ?assertEqual([], flush([])).

flush(Acc) ->
    receive
        {'EXIT', _Pid, normal} -> flush(Acc);
        M                      -> flush([M | Acc])
    after
        0 -> lists:reverse(Acc)
    end.

create_replication_slot(Config, Connection) ->
  Module = ?config(module, Config),
  {ok, Cols, Rows} =
    Module:squery(Connection,
                  "CREATE_REPLICATION_SLOT ""epgsql_test"" LOGICAL ""test_decoding"""),
  ?assertMatch([#column{name = <<"slot_name">>},
                #column{name = <<"consistent_point">>},
                #column{name = <<"snapshot_name">>},
                #column{name = <<"output_plugin">>}
               ],
               Cols),
  ?assertMatch([{<<"epgsql_test">>, _, _, <<"test_decoding">>}], Rows).

drop_replication_slot(Config) ->
  epgsql_ct:with_connection(
    Config,
    fun(C) -> drop_replication_slot(Config, C) end,
    "epgsql_test_replication",
    [{replication, "database"}]).

drop_replication_slot(Config, Connection) ->
  Module = ?config(module, Config),
  Result = Module:squery(Connection, "DROP_REPLICATION_SLOT ""epgsql_test"""),
  case ?config(version, ?config(pg_config, Config)) >= [13, 0] of
    true -> ?assertMatch({ok, _, _}, Result);
    false -> ?assertMatch([{ok, _, _}, {ok, _, _}], Result)
  end.
