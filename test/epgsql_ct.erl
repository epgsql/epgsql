-module(epgsql_ct).
-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

-export([
    connect_only/2,
    with_connection/2,
    with_connection/3,
    with_connection/4,
    with_rollback/2,
    with_min_version/4,
    flush/0
]).

connect_only(Config, Args) ->
    #{pg_host := Host, pg_port := Port} = ?config(pg_config, Config),
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
    #{pg_host := Host, pg_port := Port} = ?config(pg_config, Config),
    Module = ?config(module, Config),
    Args2 = [{port, Port}, {database, "epgsql_test_db1"} | Args],
    {ok, C} = Module:connect(Host, Username, Args2),
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
    Module = ?config(module, Config),
    epgsql_ct:with_connection(Config, fun(C) ->
        {ok, Bin} = Module:get_parameter(C, <<"server_version">>),
        {ok, [{float, 1, Ver} | _], _} = erl_scan:string(binary_to_list(Bin)),
        case Ver >= Min of
            true  -> F(C);
            false -> ?debugFmt("skipping test requiring PostgreSQL >= ~.2f~n", [Min])
        end
    end, Args).

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
