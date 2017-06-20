%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Main Test Suite for the epgsql module
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
-module(epgsql_SUITE).

-export([ all/0
        , init_per_suite/1
        , end_per_suite/1
        , init_per_testcase/2
        , end_per_testcase/2
        ]).

-export([ timed_equery/1
        , timed_execute/1
        , timed_prepared_query/1
        , timed_squery/1
        ]).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Includes
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

-include("epgsql.hrl").

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% CT Callback Functions
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
-spec all() -> [atom()].
all() ->
  ExcludedFuns = [init_per_suite, end_per_suite, all, module_info],
  Exports = ?MODULE:module_info(exports),
  [F || {F, 1} <- Exports, not lists:member(F, ExcludedFuns)].

-spec init_per_suite([any()]) -> [any()].
init_per_suite(Config) ->
  {ok, Started} = application:ensure_all_started(epgsql),
  [{started, Started}|Config].

-spec end_per_suite([any()]) -> [any()].
end_per_suite(Config) ->
  [ok = application:stop(App) || App <- ?config(started, Config)],
  Config.

-spec init_per_testcase(atom(), [any()]) -> any().
init_per_testcase(_Testcase, Config) ->
  {ok, C} = connect(),
  [{connection, C} | Config].

-spec end_per_testcase(atom(), [any()]) -> any().
end_per_testcase(_TestCase, Config) ->
  C  = ?config(connection, Config),
  ok = disconnect(C),
  Config.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Test Cases
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
-spec timed_equery([any()]) -> ok.
timed_equery(Config) ->
  C     = ?config(connection, Config),
  ?assertMatch( {error, #error{ severity = error
                              , code     = <<"57014">>
                              , codename = query_canceled
                              }}
              , epgsql:timed_equery(C, "SELECT pg_sleep(2)", 1000)),
  ?assertMatch( {ok, _Cols, [{1}]}
              , epgsql:equery(C, "SELECT 1")).

-spec timed_execute([any()]) -> ok.
timed_execute(Config) ->
  C       = ?config(connection, Config),
  {ok, S} = epgsql:parse(C, "select pg_sleep($1)"),
  ok      = epgsql:bind(C, S, [2]),
  ?assertMatch( {error, #error{ severity = error
                              , code     = <<"57014">>
                              , codename = query_canceled
                              }}
              , epgsql:timed_execute(C, S, 1000)),
  ok      = epgsql:sync(C),
  ok      = epgsql:bind(C, S, [0]),
  ?assertEqual( {ok, [{<<>>}]}
              , epgsql:timed_execute(C, S, 1000)),
  ok      = epgsql:sync(C).

-spec timed_prepared_query([any()]) -> ok.
timed_prepared_query(Config) ->
  C       = ?config(connection, Config),
  {ok, _} = epgsql:parse(C, "sleep", "select pg_sleep(2)", []),
  ?assertMatch( {error, #error{ severity = error
                              , code     = <<"57014">>
                              , codename = query_canceled
                              }}
              , epgsql:timed_prepared_query(C, "sleep", [], 1000)),
  {ok, _}= epgsql:parse(C, "no_sleep", "select pg_sleep(0)", []),
  ?assertMatch( {ok, _, [{<<>>}]}
              , epgsql:timed_prepared_query(C, "no_sleep", [], 1000)).

-spec timed_squery([any()]) -> ok.
timed_squery(Config) ->
  C     = ?config(connection, Config),
  ?assertMatch( {error, #error{ severity = error
                              , code     = <<"57014">>
                              , codename = query_canceled
                              }}
              , epgsql:timed_squery(C, "SELECT pg_sleep(2)", 1000)),
  ?assertMatch( {ok, _, [{<<>>}]}
              , epgsql:timed_squery(C, "SELECT pg_sleep(0)", 1000)).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Helpers
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
-spec connect() -> {ok, epgsql:connection()}.
connect() ->
  Host     = "localhost",
  Port     = 10432,
  Database = "epgsql_test_db1",
  Username = "epgsql_test",
  Opts     = [{database, Database}, {port, Port}],
  {ok, _C} = epgsql:connect(Host, Username, Opts).

-spec disconnect(epgsql:connection()) -> ok.
disconnect(C) ->
  ok = epgsql:close(C).
