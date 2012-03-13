-module(pgsql_tests).

-export([run_tests/0]).

-include_lib("eunit/include/eunit.hrl").
-include_lib("public_key/include/public_key.hrl").
-include("pgsql.hrl").

-define(host, "localhost").
-define(port, 5432).

-define(ssl_apps, [crypto, public_key, ssl]).

connect_test() ->
    connect_only([]).

connect_to_db_test() ->
    connect_only([{database, "epgsql_test_db1"}]).

connect_as_test() ->
    connect_only(["epgsql_test", [{database, "epgsql_test_db1"}]]).

connect_with_cleartext_test() ->
    connect_only(["epgsql_test_cleartext",
                  "epgsql_test_cleartext",
                  [{database, "epgsql_test_db1"}]]).

connect_with_md5_test() ->
    connect_only(["epgsql_test_md5",
                  "epgsql_test_md5",
                  [{database, "epgsql_test_db1"}]]).

connect_with_invalid_user_test() ->
    {error, invalid_authorization_specification} =
        pgsql:connect(?host,
                      "epgsql_test_invalid",
                      "epgsql_test_invalid",
                      [{port, ?port}, {database, "epgsql_test_db1"}]).

connect_with_invalid_password_test() ->
    {error, Why} =
        pgsql:connect(?host,
                      "epgsql_test_md5",
                      "epgsql_test_invalid",
                      [{port, ?port}, {database, "epgsql_test_db1"}]),
    case Why of
        invalid_authorization_specification -> ok; % =< 8.4
        invalid_password                    -> ok  % >= 9.0
    end.


connect_with_ssl_test() ->
    lists:foreach(fun application:start/1, ?ssl_apps),
    with_connection(
      fun(C) ->
              {ok, _Cols, [{true}]} = pgsql:equery(C, "select ssl_is_used()")
      end,
      "epgsql_test",
      [{ssl, true}]).

connect_with_client_cert_test() ->
    lists:foreach(fun application:start/1, ?ssl_apps),
    Dir = filename:join(filename:dirname(code:which(pgsql_tests)), "../test_data"),
    File = fun(Name) -> filename:join(Dir, Name) end,
    {ok, Pem} = file:read_file(File("epgsql.crt")),
    [{'Certificate', Der, not_encrypted}] = public_key:pem_decode(Pem),
    Cert = public_key:pkix_decode_cert(Der, plain),
    #'TBSCertificate'{serialNumber = Serial} = Cert#'Certificate'.tbsCertificate,
    Serial2 = list_to_binary(integer_to_list(Serial)),

    with_connection(
      fun(C) ->
              {ok, _, [{true}]} = pgsql:equery(C, "select ssl_is_used()"),
              {ok, _, [{Serial2}]} = pgsql:equery(C, "select ssl_client_serial()")
      end,
      "epgsql_test_cert",
      [{ssl, true}, {keyfile, File("epgsql.key")}, {certfile, File("epgsql.crt")}]).

select_test() ->
    with_connection(
      fun(C) ->
              {ok, Cols, Rows} = pgsql:squery(C, "select * from test_table1"),
              [#column{name = <<"id">>, type = int4, size = 4},
               #column{name = <<"value">>, type = text, size = -1}] = Cols,
              [{<<"1">>, <<"one">>}, {<<"2">>, <<"two">>}] = Rows
      end).

insert_test() ->
    with_rollback(
      fun(C) ->
              {ok, 1} = pgsql:squery(C, "insert into test_table1 (id, value) values (3, 'three')")
      end).

update_test() ->
    with_rollback(
      fun(C) ->
              {ok, 1} = pgsql:squery(C, "insert into test_table1 (id, value) values (3, 'three')"),
              {ok, 1} = pgsql:squery(C, "insert into test_table1 (id, value) values (4, 'four')"),
              {ok, 2} = pgsql:squery(C, "update test_table1 set value = 'foo' where id > 2"),
              {ok, _, [{<<"2">>}]} = pgsql:squery(C, "select count(*) from test_table1 where value = 'foo'")
      end).

delete_test() ->
    with_rollback(
      fun(C) ->
              {ok, 1} = pgsql:squery(C, "insert into test_table1 (id, value) values (3, 'three')"),
              {ok, 1} = pgsql:squery(C, "insert into test_table1 (id, value) values (4, 'four')"),
              {ok, 2} = pgsql:squery(C, "delete from test_table1 where id > 2"),
              {ok, _, [{<<"2">>}]} = pgsql:squery(C, "select count(*) from test_table1")
      end).

create_and_drop_table_test() ->
    with_rollback(
      fun(C) ->
              {ok, [], []} = pgsql:squery(C, "create table test_table3 (id int4)"),
              {ok, [#column{type = int4}], []} = pgsql:squery(C, "select * from test_table3"),
              {ok, [], []} = pgsql:squery(C, "drop table test_table3")
      end).

cursor_test() ->
    with_connection(
      fun(C) ->
              {ok, [], []} = pgsql:squery(C, "begin"),
              {ok, [], []} = pgsql:squery(C, "declare c cursor for select id from test_table1"),
              {ok, 2} = pgsql:squery(C, "move forward 2 from c"),
              {ok, 1} = pgsql:squery(C, "move backward 1 from c"),
              {ok, 1, _Cols, [{<<"2">>}]} = pgsql:squery(C, "fetch next from c"),
              {ok, [], []} = pgsql:squery(C, "close c")
      end).

multiple_result_test() ->
    with_connection(
      fun(C) ->
              [{ok, _, [{<<"1">>}]}, {ok, _, [{<<"2">>}]}] = pgsql:squery(C, "select 1; select 2"),
              [{ok, _, [{<<"1">>}]}, {error, #error{}}] = pgsql:squery(C, "select 1; select foo;")
      end).

extended_select_test() ->
    with_connection(
      fun(C) ->
              {ok, Cols, Rows} = pgsql:equery(C, "select * from test_table1", []),
              [#column{name = <<"id">>, type = int4, size = 4},
               #column{name = <<"value">>, type = text, size = -1}] = Cols,
              [{1, <<"one">>}, {2, <<"two">>}] = Rows
      end).

extended_sync_ok_test() ->
    with_connection(
      fun(C) ->
              {ok, _Cols, [{<<"one">>}]} = pgsql:equery(C, "select value from test_table1 where id = $1", [1]),
              {ok, _Cols, [{<<"two">>}]} = pgsql:equery(C, "select value from test_table1 where id = $1", [2])
      end).

extended_sync_error_test() ->
    with_connection(
      fun(C) ->
              {error, #error{}} = pgsql:equery(C, "select _alue from test_table1 where id = $1", [1]),
              {ok, _Cols, [{<<"one">>}]} = pgsql:equery(C, "select value from test_table1 where id = $1", [1])
      end).

returning_from_insert_test() ->
    with_rollback(
      fun(C) ->
              {ok, 1, _Cols, [{3}]} = pgsql:equery(C, "insert into test_table1 (id) values (3) returning id")
      end).

returning_from_update_test() ->
    with_rollback(
      fun(C) ->
              {ok, 2, _Cols, [{1}, {2}]} = pgsql:equery(C, "update test_table1 set value = 'hi' returning id")
      end).

returning_from_delete_test() ->
    with_rollback(
      fun(C) ->
              {ok, 2, _Cols, [{1}, {2}]} = pgsql:equery(C, "delete from test_table1 returning id")
      end).

parse_test() ->
    with_connection(
      fun(C) ->
              {ok, S} = pgsql:parse(C, "select * from test_table1"),
              [#column{name = <<"id">>}, #column{name = <<"value">>}] = S#statement.columns,
              ok = pgsql:close(C, S),
              ok = pgsql:sync(C)
      end).

parse_column_format_test() ->
    with_connection(
      fun(C) ->
              {ok, S} = pgsql:parse(C, "select 1::int4, false::bool, 2.0::float4"),
              [#column{type = int4},
               #column{type = bool},
               #column{type = float4}] = S#statement.columns,
              ok = pgsql:bind(C, S, []),
              {ok, [{1, false, 2.0}]} = pgsql:execute(C, S, 0),
              ok = pgsql:close(C, S),
              ok = pgsql:sync(C)
      end).

parse_error_test() ->
    with_connection(
      fun(C) ->
              {error, #error{}} = pgsql:parse(C, "select _ from test_table1"),
              {ok, S} = pgsql:parse(C, "select * from test_table1"),
              [#column{name = <<"id">>}, #column{name = <<"value">>}] = S#statement.columns,
              ok = pgsql:close(C, S),
              ok = pgsql:sync(C)
      end).

parse_and_close_test() ->
    with_connection(
      fun(C) ->
              Parse = fun() -> pgsql:parse(C, "test", "select * from test_table1", []) end,
              {ok, S} = Parse(),
              {error, #error{code = <<"42P05">>}} = Parse(),
              pgsql:close(C, S),
              {ok, S} = Parse(),
              ok = pgsql:sync(C)
      end).

bind_test() ->
    with_connection(
      fun(C) ->
              {ok, S} = pgsql:parse(C, "select value from test_table1 where id = $1"),
              ok = pgsql:bind(C, S, [1]),
              ok = pgsql:close(C, S),
              ok = pgsql:sync(C)
      end).

bind_parameter_format_test() ->
    with_connection(
      fun(C) ->
              {ok, S} = pgsql:parse(C, "select $1, $2, $3", [int2, text, bool]),
              [int2, text, bool] = S#statement.types,
              ok = pgsql:bind(C, S, [1, "hi", true]),
              {ok, [{1, <<"hi">>, true}]} = pgsql:execute(C, S, 0),
              ok = pgsql:close(C, S),
              ok = pgsql:sync(C)
      end).

bind_error_test() ->
    with_connection(
      fun(C) ->
              {ok, S} = pgsql:parse(C, "select $1::char"),
              {error, #error{}} = pgsql:bind(C, S, [0]),
              ok = pgsql:bind(C, S, [$A]),
              ok = pgsql:close(C, S),
              ok = pgsql:sync(C)
      end).

bind_and_close_test() ->
    with_connection(
      fun(C) ->
              {ok, S} = pgsql:parse(C, "select * from test_table1"),
              ok = pgsql:bind(C, S, "one", []),
              {error, #error{code = <<"42P03">>}} = pgsql:bind(C, S, "one", []),
              ok = pgsql:close(C, portal, "one"),
              ok = pgsql:bind(C, S, "one", []),
              ok = pgsql:sync(C)
      end).

execute_error_test() ->
    with_connection(
      fun(C) ->
          {ok, S} = pgsql:parse(C, "insert into test_table1 (id, value) values ($1, $2)"),
          ok = pgsql:bind(C, S, [1, <<"foo">>]),
          {error, #error{code = <<"23505">>}} = pgsql:execute(C, S, 0),
          {error, sync_required} = pgsql:bind(C, S, [3, <<"quux">>]),
          ok = pgsql:sync(C),
          ok = pgsql:bind(C, S, [3, <<"quux">>]),
          {ok, _} = pgsql:execute(C, S, 0),
          {ok, 1} = pgsql:squery(C, "delete from test_table1 where id = 3")
      end).

describe_test() ->
    with_connection(
      fun(C) ->
              {ok, S} = pgsql:parse(C, "select * from test_table1"),
              [#column{name = <<"id">>}, #column{name = <<"value">>}] = S#statement.columns,
              {ok, S} = pgsql:describe(C, S),
              ok = pgsql:close(C, S),
              ok = pgsql:sync(C)
      end).

describe_with_param_test() ->
    with_connection(
      fun(C) ->
              {ok, S} = pgsql:parse(C, "select id from test_table1 where id = $1"),
              [int4] = S#statement.types,
              [#column{name = <<"id">>}] = S#statement.columns,
              {ok, S} = pgsql:describe(C, S),
              ok = pgsql:close(C, S),
              ok = pgsql:sync(C)
      end).

describe_named_test() ->
    with_connection(
      fun(C) ->
              {ok, S} = pgsql:parse(C, "name", "select * from test_table1", []),
              [#column{name = <<"id">>}, #column{name = <<"value">>}] = S#statement.columns,
              {ok, S} = pgsql:describe(C, S),
              ok = pgsql:close(C, S),
              ok = pgsql:sync(C)
      end).

describe_error_test() ->
    with_connection(
      fun(C) ->
              {error, #error{}} = pgsql:describe(C, statement, ""),
              {ok, S} = pgsql:parse(C, "select * from test_table1"),
              {ok, S} = pgsql:describe(C, statement, ""),
              ok = pgsql:sync(C)

      end).

portal_test() ->
    with_connection(
      fun(C) ->
              {ok, S} = pgsql:parse(C, "select value from test_table1"),
              ok = pgsql:bind(C, S, []),
              {partial, [{<<"one">>}]} = pgsql:execute(C, S, 1),
              {partial, [{<<"two">>}]} = pgsql:execute(C, S, 1),
              {ok, []} = pgsql:execute(C, S,1),
              ok = pgsql:close(C, S),
              ok = pgsql:sync(C)
      end).

returning_test() ->
    with_rollback(
      fun(C) ->
              {ok, S} = pgsql:parse(C, "update test_table1 set value = $1 returning id"),
              ok = pgsql:bind(C, S, ["foo"]),
              {ok, 2, [{1}, {2}]} = pgsql:execute(C, S),
              ok = pgsql:sync(C)
      end).

multiple_statement_test() ->
    with_connection(
      fun(C) ->
              {ok, S1} = pgsql:parse(C, "one", "select value from test_table1 where id = 1", []),
              ok = pgsql:bind(C, S1, []),
              {partial, [{<<"one">>}]} = pgsql:execute(C, S1, 1),
              {ok, S2} = pgsql:parse(C, "two", "select value from test_table1 where id = 2", []),
              ok = pgsql:bind(C, S2, []),
              {partial, [{<<"two">>}]} = pgsql:execute(C, S2, 1),
              {ok, []} = pgsql:execute(C, S1, 1),
              {ok, []} = pgsql:execute(C, S2, 1),
              ok = pgsql:close(C, S1),
              ok = pgsql:close(C, S2),
              ok = pgsql:sync(C)
      end).

multiple_portal_test() ->
    with_connection(
      fun(C) ->
              {ok, S} = pgsql:parse(C, "select value from test_table1 where id = $1"),
              ok = pgsql:bind(C, S, "one", [1]),
              ok = pgsql:bind(C, S, "two", [2]),
              {ok, [{<<"one">>}]} = pgsql:execute(C, S, "one", 0),
              {ok, [{<<"two">>}]} = pgsql:execute(C, S, "two", 0),
              ok = pgsql:close(C, S),
              ok = pgsql:sync(C)
      end).

execute_function_test() ->
    with_rollback(
      fun(C) ->
              {ok, _Cols1, [{3}]} = pgsql:equery(C, "select insert_test1(3, 'three')"),
              {ok, _Cols2, [{<<>>}]} = pgsql:equery(C, "select do_nothing()")
      end).

parameter_get_test() ->
    with_connection(
      fun(C) ->
              {ok, <<"off">>} = pgsql:get_parameter(C, "is_superuser")
      end).

parameter_set_test() ->
    with_connection(
      fun(C) ->
              {ok, [], []} = pgsql:squery(C, "set DateStyle = 'ISO, MDY'"),
              {ok, <<"ISO, MDY">>} = pgsql:get_parameter(C, "DateStyle"),
              {ok, _Cols, [{<<"2000-01-02">>}]} = pgsql:squery(C, "select '2000-01-02'::date"),
              {ok, [], []} = pgsql:squery(C, "set DateStyle = 'German'"),
              {ok, <<"German, DMY">>} = pgsql:get_parameter(C, "DateStyle"),
              {ok, _Cols, [{<<"02.01.2000">>}]} = pgsql:squery(C, "select '2000-01-02'::date")
      end).

numeric_type_test() ->
    check_type(int2, "1", 1, [0, 256, -32768, +32767]),
    check_type(int4, "1", 1, [0, 512, -2147483648, +2147483647]),
    check_type(int8, "1", 1, [0, 1024, -9223372036854775808, +9223372036854775807]),
    check_type(float4, "1.0", 1.0, [0.0, 1.23456, -1.23456]),
    check_type(float8, "1.0", 1.0, [0.0, 1.23456789012345, -1.23456789012345]).

character_type_test() ->
    Alpha = unicode:characters_to_binary([16#03B1]),
    Ka    = unicode:characters_to_binary([16#304B]),
    One   = unicode:characters_to_binary([16#10D360]),
    check_type(bpchar, "'A'", $A, [1, $1, 16#7F, Alpha, Ka, One], "c_char"),
    check_type(text, "'hi'", <<"hi">>, [<<"">>, <<"hi">>]),
    check_type(varchar, "'hi'", <<"hi">>, [<<"">>, <<"hi">>]).

date_time_type_test() ->
    with_connection(
      fun(C) ->
              case pgsql:get_parameter(C, "integer_datetimes") of
                  {ok, <<"on">>}  -> MaxTsDate = 294276;
                  {ok, <<"off">>} -> MaxTsDate = 5874897
              end,

              check_type(date, "'2008-01-02'", {2008,1,2}, [{-4712,1,1}, {5874897,1,1}]),
              check_type(time, "'00:01:02'", {0,1,2.0}, [{0,0,0.0}, {24,0,0.0}]),
              check_type(timetz, "'00:01:02-01'", {{0,1,2.0},1*60*60},
                         [{{0,0,0.0},0}, {{24,0,0.0},-13*60*60}]),
              check_type(timestamp, "'2008-01-02 03:04:05'", {{2008,1,2},{3,4,5.0}},
                         [{{-4712,1,1},{0,0,0.0}}, {{MaxTsDate,12,31}, {23,59,59.0}}]),
              check_type(interval, "'1 hour 2 minutes 3.1 seconds'", {{1,2,3.1},0,0},
                         [{{0,0,0.0},0,-178000000 * 12}, {{0,0,0.0},0,178000000 * 12}])
      end).

misc_type_test() ->
    check_type(bool, "true", true, [true, false]),
    check_type(bytea, "E'\001\002'", <<1,2>>, [<<>>, <<0,128,255>>]).

array_type_test() ->
    with_connection(
      fun(C) ->
          {ok, _, [{[1, 2]}]} = pgsql:equery(C, "select ($1::int[])[1:2]", [[1, 2, 3]]),
          Select = fun(Type, A) ->
                       Query = "select $1::" ++ atom_to_list(Type) ++ "[]",
                       {ok, _Cols, [{A2}]} = pgsql:equery(C, Query, [A]),
                       case lists:all(fun({V, V2}) -> compare(Type, V, V2) end, lists:zip(A, A2)) of
                           true  -> ok;
                           false -> ?assertMatch(A, A2)
                       end
                   end,
          Select(int2,   []),
          Select(int2,   [1, 2, 3, 4]),
          Select(int2,   [[1], [2], [3], [4]]),
          Select(int2,   [[[[[[1, 2]]]]]]),
          Select(bool,   [true]),
          Select(char,   [$a, $b, $c]),
          Select(int4,   [[1, 2]]),
          Select(int8,   [[[[1, 2]], [[3, 4]]]]),
          Select(text,   [<<"one">>, <<"two>">>]),
          Select(float4, [0.0, 1.0, 0.123]),
          Select(float8, [0.0, 1.0, 0.123])
      end).

text_format_test() ->
    with_connection(
      fun(C) ->
              Select = fun(Type, V) ->
                               V2 = list_to_binary(V),
                               Query = "select $1::" ++ Type,
                               {ok, _Cols, [{V2}]} = pgsql:equery(C, Query, [V]),
                               {ok, _Cols, [{V2}]} = pgsql:equery(C, Query, [V2])
                       end,
              Select("inet", "127.0.0.1"),
              Select("numeric", "123456")
      end).

connect_timeout_test() ->
    {error, timeout} = pgsql:connect(?host, [{port, ?port}, {timeout, 0}]).

query_timeout_test() ->
    with_connection(
      fun(C) ->
              {error, timeout} = pgsql:squery(C, "select pg_sleep(1)"),
              {error, timeout} = pgsql:equery(C, "select pg_sleep(2)"),
              {ok, _Cols, [{1}]} = pgsql:equery(C, "select 1")
      end,
      [{timeout, 10}]).

execute_timeout_test() ->
    with_connection(
      fun(C) ->
              {ok, S} = pgsql:parse(C, "select pg_sleep($1)"),
              ok = pgsql:bind(C, S, [2]),
              {error, timeout} = pgsql:execute(C, S, 0),
              ok = pgsql:bind(C, S, [0]),
              {ok, [{<<>>}]} = pgsql:execute(C, S, 0),
              ok = pgsql:close(C, S),
              ok = pgsql:sync(C)
      end,
      [{timeout, 10}]).

connection_closed_test() ->
    P = self(),
    F = fun() ->
                process_flag(trap_exit, true),
                {ok, C} = pgsql:connect(?host, [{port, ?port}]),
                P ! {connected, C},
                receive
                    Any -> P ! Any
                end
        end,
    spawn_link(F),
    receive
        {connected, C} ->
            timer:sleep(100),
            pgsql:close(C),
            {'EXIT', C, _} = receive R -> R end
    end,
    flush().

active_connection_closed_test() ->
    P = self(),
    F = fun() ->
                process_flag(trap_exit, true),
                {ok, C} = pgsql:connect(?host, [{port, ?port}]),
                P ! {connected, C},
                R = pgsql:squery(C, "select pg_sleep(10)"),
                P ! R
        end,
    spawn_link(F),
    receive
        {connected, C} ->
            timer:sleep(100),
            pgsql:close(C),
            {error, closed} = receive R -> R end
    end,
    flush().

warning_notice_test() ->
    with_connection(
      fun(C) ->
          Q = "create function pg_temp.raise() returns void as $$
               begin
                 raise warning 'oops';
               end;
               $$ language plpgsql;
               select pg_temp.raise()",
          [{ok, _, _}, _] = pgsql:squery(C, Q),
          receive
              {pgsql, C, {notice, #error{message = <<"oops">>}}} -> ok
          after
              100 -> erlang:error(didnt_receive_notice)
          end
      end,
      [{async, self()}]).

listen_notify_test() ->
    with_connection(
      fun(C) ->
          {ok, [], []}     = pgsql:squery(C, "listen epgsql_test"),
          {ok, _, [{Pid}]} = pgsql:equery(C, "select pg_backend_pid()"),
          {ok, [], []}     = pgsql:squery(C, "notify epgsql_test"),
          receive
              {pgsql, C, {notification, <<"epgsql_test">>, Pid, <<>>}} -> ok
          after
              100 -> erlang:error(didnt_receive_notification)
          end
      end,
      [{async, self()}]).

listen_notify_payload_test() ->
    with_min_version(
      9.0,
      fun(C) ->
          {ok, [], []}     = pgsql:squery(C, "listen epgsql_test"),
          {ok, _, [{Pid}]} = pgsql:equery(C, "select pg_backend_pid()"),
          {ok, [], []}     = pgsql:squery(C, "notify epgsql_test, 'test!'"),
          receive
              {pgsql, C, {notification, <<"epgsql_test">>, Pid, <<"test!">>}} -> ok
          after
              100 -> erlang:error(didnt_receive_notification)
          end
      end,
      [{async, self()}]).

application_test() ->
    lists:foreach(fun application:start/1, ?ssl_apps),
    ok = application:start(epgsql).

%% -- run all tests --

run_tests() ->
    Files = filelib:wildcard("test_ebin/*tests.beam"),
    Mods = [list_to_atom(filename:basename(F, ".beam")) || F <- Files],
    eunit:test(Mods, []).

%% -- internal functions --

connect_only(Args) ->
    TestOpts = [{port, ?port}],
    case Args of
        [User, Opts]       -> Args2 = [User, TestOpts ++ Opts];
        [User, Pass, Opts] -> Args2 = [User, Pass, TestOpts ++ Opts];
        Opts               -> Args2 = [TestOpts ++ Opts]
    end,
    {ok, C} = apply(pgsql, connect, [?host | Args2]),
    pgsql:close(C),
    flush().

with_connection(F) ->
    with_connection(F, "epgsql_test", []).

with_connection(F, Args) ->
    with_connection(F, "epgsql_test", Args).

with_connection(F, Username, Args) ->
    Args2 = [{port, ?port}, {database, "epgsql_test_db1"} | Args],
    {ok, C} = pgsql:connect(?host, Username, Args2),
    try
        F(C)
    after
        pgsql:close(C)
    end,
    flush().

with_rollback(F) ->
    with_connection(
      fun(C) ->
              try
                  pgsql:squery(C, "begin"),
                  F(C)
                  after
                      pgsql:squery(C, "rollback")
                  end
      end).

with_min_version(Min, F, Args) ->
    with_connection(
      fun(C) ->
          {ok, Bin} = pgsql:get_parameter(C, <<"server_version">>),
          {ok, [{float, 1, Ver} | _], _} = erl_scan:string(binary_to_list(Bin)),
          case Ver >= Min of
              true  -> F(C);
              false -> ?debugFmt("skipping test requiring PostgreSQL >= ~.2f~n", [Min])
          end
      end,
      Args).

check_type(Type, In, Out, Values) ->
    Column = "c_" ++ atom_to_list(Type),
    check_type(Type, In, Out, Values, Column).

check_type(Type, In, Out, Values, Column) ->
    with_connection(
      fun(C) ->
              Select = io_lib:format("select ~s::~w", [In, Type]),
              {ok, [#column{type = Type}], [{Out}]} = pgsql:equery(C, Select),
              Sql = io_lib:format("insert into test_table2 (~s) values ($1) returning ~s", [Column, Column]),
              {ok, #statement{columns = [#column{type = Type}]} = S} = pgsql:parse(C, Sql),
              Insert = fun(V) ->
                               pgsql:bind(C, S, [V]),
                               {ok, 1, [{V2}]} = pgsql:execute(C, S),
                               case compare(Type, V, V2) of
                                   true  -> ok;
                                   false -> ?debugFmt("~p =/= ~p~n", [V, V2]), ?assert(false)
                               end,
                               ok = pgsql:sync(C)
                       end,
              lists:foreach(Insert, [null | Values])
      end).

compare(_Type, null, null) -> true;
compare(float4, V1, V2)    -> abs(V2 - V1) < 0.000001;
compare(float8, V1, V2)    -> abs(V2 - V1) < 0.000000000000001;
compare(_Type, V1, V2)     -> V1 =:= V2.

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
