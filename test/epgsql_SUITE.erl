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

-export([ equery_timeout/1
        , execute_timeout/1
        , prepared_query_timeout/1
        , squery_timeout/1
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
-spec equery_timeout([any()]) -> ok.
equery_timeout(Config) ->
  C     = ?config(connection, Config),
  Error = timeout_error(),
  ?assertMatch( Error
              , epgsql:equery_with_timeout(C, "SELECT pg_sleep(2)", 1000)),
  ?assertMatch( {ok, _Cols, [{1}]}
              , epgsql:equery(C, "SELECT 1")).

-spec execute_timeout([any()]) -> ok.
execute_timeout(Config) ->
  C       = ?config(connection, Config),
  Error   = timeout_error(),
  {ok, S} = epgsql:parse(C, "select pg_sleep($1)"),
  ok      = epgsql:bind(C, S, [2]),
  ?assertMatch( Error
              , epgsql:execute_with_timeout(C, S, 1000)),
  ok      = epgsql:sync(C),
  ok      = epgsql:bind(C, S, [0]),
  ?assertEqual( {ok, [{<<>>}]}
              , epgsql:execute_with_timeout(C, S, 1000)),
  ok      = epgsql:sync(C).

-spec prepared_query_timeout([any()]) -> ok.
prepared_query_timeout(Config) ->
  C       = ?config(connection, Config),
  {ok, _} = epgsql:parse(C, "sleep", "select pg_sleep(2)", []),
  Error   = timeout_error(),
  ?assertMatch( Error
              , epgsql:prepared_query_with_timeout(C, "sleep", [], 1000)),
  {ok, _}= epgsql:parse(C, "no_sleep", "select pg_sleep(0)", []),
  ?assertMatch( {ok, _, [{<<>>}]}
              , epgsql:prepared_query_with_timeout(C, "no_sleep", [], 1000)).

-spec squery_timeout([any()]) -> ok.
squery_timeout(Config) ->
  C     = ?config(connection, Config),
  Error = timeout_error(),
  ?assertMatch( Error
              , epgsql:squery_with_timeout(C, "SELECT pg_sleep(2)", 1000)),
  ?assertMatch( {ok, _, [{<<>>}]}
              , epgsql:squery_with_timeout(C, "SELECT pg_sleep(0)", 1000)).

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

-spec timeout_error() -> {error, #error{}}.
timeout_error() ->
  Error = #error{ severity = error
                , code     = <<"57014">>
                , codename = query_canceled
                , message  = <<"canceling statement due to timeout">>
                , extra    = []
                },
  {error, Error}.
