-module(pgsql_tests).

-export([run_tests/0]).

-include_lib("eunit/include/eunit.hrl").
-include("pgsql.hrl").

-define(host, "localhost").

connect_test() ->
    connect_only([[]]).

connect_to_db_test() ->
    connect_only([[{database, "epgsql_test_db1"}]]).

connect_as_test() ->
    connect_only(["epgsql_test1", [{database, "epgsql_test_db1"}]]).

connect_with_cleartext_test() ->
    connect_only(["epgsql_test_cleartext",
                  "epgsql_test_cleartext",
                  [{database, "epgsql_test_db1"}]]).

connect_with_md5_test() ->
    connect_only(["epgsql_test_md5",
                  "epgsql_test_md5",
                  [{database, "epgsql_test_db1"}]]).

connect_with_invalid_password_test() ->
    {error, invalid_authorization_specification} =
        pgsql:connect(?host,
                      "epgsql_test_md5",
                      "epgsql_test_sha1",
                      [{database, "epgsql_test_db1"}]).

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

delete_test() ->
    with_rollback(
      fun(C) ->
              {ok, 1} = pgsql:squery(C, "insert into test_table1 (id, value) values (3, 'three')"),
              {ok, 1} = pgsql:squery(C, "insert into test_table1 (id, value) values (4, 'four')"),
              {ok, 2} = pgsql:squery(C, "delete from test_table1 where id > 2"),
              {ok, _, [{<<"2">>}]} = pgsql:squery(C, "select count(*) from test_table1")
      end).

update_test() ->
    with_rollback(
      fun(C) ->
              {ok, 1} = pgsql:squery(C, "insert into test_table1 (id, value) values (3, 'three')"),
              {ok, 1} = pgsql:squery(C, "insert into test_table1 (id, value) values (4, 'four')"),
              {ok, 2} = pgsql:squery(C, "update test_table1 set value = 'foo' where id > 2"),
              {ok, _, [{<<"2">>}]} = pgsql:squery(C, "select count(*) from test_table1 where value = 'foo'")
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
              {ok, <<"off">>} = pgsql:get_parameter(C, "integer_datetimes")
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

decode_binary_format_test() ->
    with_connection(
      fun(C) ->
              {ok, [#column{type = unknown}], [{null}]} = pgsql:equery(C, "select null"),
              {ok, [#column{type = bool}], [{true}]} = pgsql:equery(C, "select true"),
              {ok, [#column{type = bool}], [{false}]} = pgsql:equery(C, "select false"),
              {ok, [#column{type = bpchar}], [{$A}]} = pgsql:equery(C, "select 'A'::char"),
              {ok, [#column{type = int2}], [{1}]} = pgsql:equery(C, "select 1::int2"),
              {ok, [#column{type = int2}], [{-1}]} = pgsql:equery(C, "select -1::int2"),
              {ok, [#column{type = int4}], [{1}]} = pgsql:equery(C, "select 1::int4"),
              {ok, [#column{type = int4}], [{-1}]} = pgsql:equery(C, "select -1::int4"),
              {ok, [#column{type = int8}], [{1}]} = pgsql:equery(C, "select 1::int8"),
              {ok, [#column{type = int8}], [{-1}]} = pgsql:equery(C, "select -1::int8"),
              {ok, [#column{type = float4}], [{1.0}]} = pgsql:equery(C, "select 1.0::float4"),
              {ok, [#column{type = float4}], [{-1.0}]} = pgsql:equery(C, "select -1.0::float4"),
              {ok, [#column{type = float8}], [{1.0}]} = pgsql:equery(C, "select 1.0::float8"),
              {ok, [#column{type = float8}], [{-1.0}]} = pgsql:equery(C, "select -1.0::float8"),
              {ok, [#column{type = bytea}], [{<<1, 2>>}]} = pgsql:equery(C, "select E'\001\002'::bytea"),
              {ok, [#column{type = text}], [{<<"hi">>}]} = pgsql:equery(C, "select 'hi'::text"),
              {ok, [#column{type = varchar}], [{<<"hi">>}]} = pgsql:equery(C, "select 'hi'::varchar"),
              {ok, [#column{type = record}], [{{1, null, <<"hi">>}}]} = pgsql:equery(C, "select (1, null, 'hi')")
      end).

encode_binary_format_test() ->
    with_connection(
      fun(C) ->
              {ok, 1} = pgsql:equery(C, "insert into test_table2 (c_bool) values ($1)", [null]),
              {ok, 1} = pgsql:equery(C, "insert into test_table2 (c_bool) values ($1)", [true]),
              {ok, 1} = pgsql:equery(C, "insert into test_table2 (c_bool) values ($1)", [false]),
              {ok, 1} = pgsql:equery(C, "insert into test_table2 (c_char) values ($1)", [$A]),
              {ok, 1} = pgsql:equery(C, "insert into test_table2 (c_int2) values ($1)", [1]),
              {ok, 1} = pgsql:equery(C, "insert into test_table2 (c_int2) values ($1)", [-1]),
              {ok, 1} = pgsql:equery(C, "insert into test_table2 (c_int4) values ($1)", [1]),
              {ok, 1} = pgsql:equery(C, "insert into test_table2 (c_int4) values ($1)", [-1]),
              {ok, 1} = pgsql:equery(C, "insert into test_table2 (c_int8) values ($1)", [1]),
              {ok, 1} = pgsql:equery(C, "insert into test_table2 (c_int8) values ($1)", [-1]),
              {ok, 1} = pgsql:equery(C, "insert into test_table2 (c_float4) values ($1)", [1.0]),
              {ok, 1} = pgsql:equery(C, "insert into test_table2 (c_float4) values ($1)", [-1.0]),
              {ok, 1} = pgsql:equery(C, "insert into test_table2 (c_float8) values ($1)", [1.0]),
              {ok, 1} = pgsql:equery(C, "insert into test_table2 (c_float8) values ($1)", [-1.0]),
              {ok, 1} = pgsql:equery(C, "insert into test_table2 (c_bytea) values ($1)", [<<1, 2>>]),
              {ok, 1} = pgsql:equery(C, "insert into test_table2 (c_bytea) values ($1)", [[1, 2]]),
              {ok, 1} = pgsql:equery(C, "insert into test_table2 (c_text) values ($1)", [<<"hi">>]),
              {ok, 1} = pgsql:equery(C, "insert into test_table2 (c_text) values ($1)", ["hi"]),
              {ok, 1} = pgsql:equery(C, "insert into test_table2 (c_varchar) values ($1)", [<<"hi">>]),
              {ok, 1} = pgsql:equery(C, "insert into test_table2 (c_varchar) values ($1)", ["hi"])
      end).

%% -- run all tests --

run_tests() ->
    crypto:start(),
    Files = filelib:wildcard("test_ebin/*tests.beam"),
    Mods = [list_to_atom(filename:basename(F, ".beam")) || F <- Files],
    eunit:test(Mods, []).

%% -- internal functions --

connect_only(Args) ->
    {ok, C} = apply(pgsql, connect, [?host | Args]),
    pgsql:close(C),
    flush().

with_connection(F) ->
    {ok, C} = pgsql:connect(?host, "epgsql_test1", [{database, "epgsql_test_db1"}]),
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

