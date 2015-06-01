-module(epgsql_tests).

-export([run_tests/0]).
-compile([export_all]).

-include_lib("eunit/include/eunit.hrl").
-include_lib("public_key/include/public_key.hrl").
-include("epgsql.hrl").

-define(host, "localhost").
-define(port, 10432).

-define(ssl_apps, [crypto, asn1, public_key, ssl]).

-define(UUID1,
        <<163,189,240,40,149,151,17,227,141,6,112,24,139,130,16,73>>).

-define(UUID2,
        <<183,55,22,52,149,151,17,227,187,167,112,24,139,130,16,73>>).

-define(UUID3,
        <<198,188,155,66,149,151,17,227,138,98,112,24,139,130,16,73>>).

-define(TIMEOUT_ERROR,
        {error,{error,error,<<"57014">>,
                <<"canceling statement due to statement timeout">>,[]}}).

%% From uuid.erl in http://gitorious.org/avtobiff/erlang-uuid
uuid_to_string(<<U0:32, U1:16, U2:16, U3:16, U4:48>>) ->
    lists:flatten(io_lib:format(
                    "~8.16.0b-~4.16.0b-~4.16.0b-~4.16.0b-~12.16.0b",
                    [U0, U1, U2, U3, U4])).

connect_test(Module) ->
    connect_only(Module, []).

connect_to_db_test(Module) ->
    connect_only(Module, [{database, "epgsql_test_db1"}]).

connect_as_test(Module) ->
    connect_only(Module, ["epgsql_test", [{database, "epgsql_test_db1"}]]).

connect_with_cleartext_test(Module) ->
    connect_only(Module, ["epgsql_test_cleartext",
                  "epgsql_test_cleartext",
                  [{database, "epgsql_test_db1"}]]).

connect_with_md5_test(Module) ->
    connect_only(Module, ["epgsql_test_md5",
                  "epgsql_test_md5",
                  [{database, "epgsql_test_db1"}]]).

connect_with_invalid_user_test(Module) ->
    {error, Why} =
        Module:connect(?host,
                      "epgsql_test_invalid",
                      "epgsql_test_invalid",
                      [{port, ?port}, {database, "epgsql_test_db1"}]),
    case Why of
        invalid_authorization_specification -> ok; % =< 8.4
        invalid_password                    -> ok  % >= 9.0
    end.

connect_with_invalid_password_test(Module) ->
    {error, Why} =
        Module:connect(?host,
                      "epgsql_test_md5",
                      "epgsql_test_invalid",
                      [{port, ?port}, {database, "epgsql_test_db1"}]),
    case Why of
        invalid_authorization_specification -> ok; % =< 8.4
        invalid_password                    -> ok  % >= 9.0
    end.


connect_with_ssl_test(Module) ->
    lists:foreach(fun application:start/1, ?ssl_apps),
    with_connection(
      Module,
      fun(C) ->
              {ok, _Cols, [{true}]} = Module:equery(C, "select ssl_is_used()")
      end,
      "epgsql_test",
      [{ssl, true}]).

connect_with_client_cert_test(Module) ->
    lists:foreach(fun application:start/1, ?ssl_apps),
    Dir = filename:join(filename:dirname(code:which(epgsql_tests)), "../test_data"),
    File = fun(Name) -> filename:join(Dir, Name) end,
    {ok, Pem} = file:read_file(File("epgsql.crt")),
    [{'Certificate', Der, not_encrypted}] = public_key:pem_decode(Pem),
    Cert = public_key:pkix_decode_cert(Der, plain),
    #'TBSCertificate'{serialNumber = Serial} = Cert#'Certificate'.tbsCertificate,
    Serial2 = list_to_binary(integer_to_list(Serial)),

    with_connection(
      Module,
      fun(C) ->
              {ok, _, [{true}]} = Module:equery(C, "select ssl_is_used()"),
              {ok, _, [{Serial2}]} = Module:equery(C, "select ssl_client_serial()")
      end,
      "epgsql_test_cert",
      [{ssl, true}, {keyfile, File("epgsql.key")}, {certfile, File("epgsql.crt")}]).

select_test(Module) ->
    with_connection(
      Module,
      fun(C) ->
              {ok, Cols, Rows} = Module:squery(C, "select * from test_table1"),
              [#column{name = <<"id">>, type = int4, size = 4},
               #column{name = <<"value">>, type = text, size = -1}] = Cols,
              [{<<"1">>, <<"one">>}, {<<"2">>, <<"two">>}] = Rows
      end).

insert_test(Module) ->
    with_rollback(
      Module,
      fun(C) ->
              {ok, 1} = Module:squery(C, "insert into test_table1 (id, value) values (3, 'three')")
      end).

update_test(Module) ->
    with_rollback(
      Module,
      fun(C) ->
              {ok, 1} = Module:squery(C, "insert into test_table1 (id, value) values (3, 'three')"),
              {ok, 1} = Module:squery(C, "insert into test_table1 (id, value) values (4, 'four')"),
              {ok, 2} = Module:squery(C, "update test_table1 set value = 'foo' where id > 2"),
              {ok, _, [{<<"2">>}]} = Module:squery(C, "select count(*) from test_table1 where value = 'foo'")
      end).

delete_test(Module) ->
    with_rollback(
      Module,
      fun(C) ->
              {ok, 1} = Module:squery(C, "insert into test_table1 (id, value) values (3, 'three')"),
              {ok, 1} = Module:squery(C, "insert into test_table1 (id, value) values (4, 'four')"),
              {ok, 2} = Module:squery(C, "delete from test_table1 where id > 2"),
              {ok, _, [{<<"2">>}]} = Module:squery(C, "select count(*) from test_table1")
      end).

create_and_drop_table_test(Module) ->
    with_rollback(
      Module,
      fun(C) ->
              {ok, [], []} = Module:squery(C, "create table test_table3 (id int4)"),
              {ok, [#column{type = int4}], []} = Module:squery(C, "select * from test_table3"),
              {ok, [], []} = Module:squery(C, "drop table test_table3")
      end).

cursor_test(Module) ->
    with_connection(
      Module,
      fun(C) ->
              {ok, [], []} = Module:squery(C, "begin"),
              {ok, [], []} = Module:squery(C, "declare c cursor for select id from test_table1"),
              {ok, 2} = Module:squery(C, "move forward 2 from c"),
              {ok, 1} = Module:squery(C, "move backward 1 from c"),
              {ok, 1, _Cols, [{<<"2">>}]} = Module:squery(C, "fetch next from c"),
              {ok, [], []} = Module:squery(C, "close c")
      end).

multiple_result_test(Module) ->
    with_connection(
      Module,
      fun(C) ->
              [{ok, _, [{<<"1">>}]}, {ok, _, [{<<"2">>}]}] = Module:squery(C, "select 1; select 2"),
              [{ok, _, [{<<"1">>}]}, {error, #error{}}] = Module:squery(C, "select 1; select foo;")
      end).

execute_batch_test(Module) ->
    with_connection(
      Module,
      fun(C) ->
              {ok, S1} = Module:parse(C, "one", "select $1", [int4]),
              {ok, S2} = Module:parse(C, "two", "select $1 + $2", [int4, int4]),
              [{ok, [{1}]}, {ok, [{3}]}] =
                  Module:execute_batch(C, [{S1, [1]}, {S2, [1, 2]}])
      end).

batch_error_test(Module) ->
    with_rollback(
      Module,
      fun(C) ->
              {ok, S} = Module:parse(C, "insert into test_table1(id, value) values($1, $2)"),
              [{ok, 1}, {error, _}] =
                  Module:execute_batch(
                    C,
                    [{S, [3, "batch_error 3"]},
                     {S, [2, "batch_error 2"]}, % duplicate key error
                     {S, [5, "batch_error 5"]}  % won't be executed
                    ])
      end).

single_batch_test(Module) ->
    with_connection(
      Module,
      fun(C) ->
              {ok, S1} = Module:parse(C, "one", "select $1", [int4]),
              [{ok, [{1}]}] = Module:execute_batch(C, [{S1, [1]}])
      end).

extended_select_test(Module) ->
    with_connection(
      Module,
      fun(C) ->
              {ok, Cols, Rows} = Module:equery(C, "select * from test_table1", []),
              [#column{name = <<"id">>, type = int4, size = 4},
               #column{name = <<"value">>, type = text, size = -1}] = Cols,
              [{1, <<"one">>}, {2, <<"two">>}] = Rows
      end).

extended_sync_ok_test(Module) ->
    with_connection(
      Module,
      fun(C) ->
              {ok, _Cols, [{<<"one">>}]} = Module:equery(C, "select value from test_table1 where id = $1", [1]),
              {ok, _Cols, [{<<"two">>}]} = Module:equery(C, "select value from test_table1 where id = $1", [2])
      end).

extended_sync_error_test(Module) ->
    with_connection(
      Module,
      fun(C) ->
              {error, #error{}} = Module:equery(C, "select _alue from test_table1 where id = $1", [1]),
              {ok, _Cols, [{<<"one">>}]} = Module:equery(C, "select value from test_table1 where id = $1", [1])
      end).

returning_from_insert_test(Module) ->
    with_rollback(
      Module,
      fun(C) ->
              {ok, 1, _Cols, [{3}]} = Module:equery(C, "insert into test_table1 (id) values (3) returning id")
      end).

returning_from_update_test(Module) ->
    with_rollback(
      Module,
      fun(C) ->
              {ok, 2, _Cols, [{1}, {2}]} = Module:equery(C, "update test_table1 set value = 'hi' returning id")
      end).

returning_from_delete_test(Module) ->
    with_rollback(
      Module,
      fun(C) ->
              {ok, 2, _Cols, [{1}, {2}]} = Module:equery(C, "delete from test_table1 returning id")
      end).

parse_test(Module) ->
    with_connection(
      Module,
      fun(C) ->
              {ok, S} = Module:parse(C, "select * from test_table1"),
              [#column{name = <<"id">>}, #column{name = <<"value">>}] = S#statement.columns,
              ok = Module:close(C, S),
              ok = Module:sync(C)
      end).

parse_column_format_test(Module) ->
    with_connection(
      Module,
      fun(C) ->
              {ok, S} = Module:parse(C, "select 1::int4, false::bool, 2.0::float4"),
              [#column{type = int4},
               #column{type = bool},
               #column{type = float4}] = S#statement.columns,
              ok = Module:bind(C, S, []),
              {ok, [{1, false, 2.0}]} = Module:execute(C, S, 0),
              ok = Module:close(C, S),
              ok = Module:sync(C)
      end).

parse_error_test(Module) ->
    with_connection(
      Module,
      fun(C) ->
              {error, #error{}} = Module:parse(C, "select _ from test_table1"),
              {ok, S} = Module:parse(C, "select * from test_table1"),
              [#column{name = <<"id">>}, #column{name = <<"value">>}] = S#statement.columns,
              ok = Module:close(C, S),
              ok = Module:sync(C)
      end).

parse_and_close_test(Module) ->
    with_connection(
      Module,
      fun(C) ->
              Parse = fun() -> Module:parse(C, "test", "select * from test_table1", []) end,
              {ok, S} = Parse(),
              {error, #error{code = <<"42P05">>, codename = duplicate_prepared_statement}} = Parse(),
              Module:close(C, S),
              {ok, S} = Parse(),
              ok = Module:sync(C)
      end).

bind_test(Module) ->
    with_connection(
      Module,
      fun(C) ->
              {ok, S} = Module:parse(C, "select value from test_table1 where id = $1"),
              ok = Module:bind(C, S, [1]),
              ok = Module:close(C, S),
              ok = Module:sync(C)
      end).

bind_parameter_format_test(Module) ->
    with_connection(
      Module,
      fun(C) ->
              {ok, S} = Module:parse(C, "select $1, $2, $3", [int2, text, bool]),
              [int2, text, bool] = S#statement.types,
              ok = Module:bind(C, S, [1, "hi", true]),
              {ok, [{1, <<"hi">>, true}]} = Module:execute(C, S, 0),
              ok = Module:close(C, S),
              ok = Module:sync(C)
      end).

bind_error_test(Module) ->
    with_connection(
      Module,
      fun(C) ->
              {ok, S} = Module:parse(C, "select $1::char"),
              {error, #error{}} = Module:bind(C, S, [0]),
              ok = Module:bind(C, S, [$A]),
              ok = Module:close(C, S),
              ok = Module:sync(C)
      end).

bind_and_close_test(Module) ->
    with_connection(
      Module,
      fun(C) ->
              {ok, S} = Module:parse(C, "select * from test_table1"),
              ok = Module:bind(C, S, "one", []),
              {error, #error{code = <<"42P03">>, codename = duplicate_cursor}} = Module:bind(C, S, "one", []),
              ok = Module:close(C, portal, "one"),
              ok = Module:bind(C, S, "one", []),
              ok = Module:sync(C)
      end).

execute_error_test(Module) ->
    with_connection(
      Module,
      fun(C) ->
          {ok, S} = Module:parse(C, "insert into test_table1 (id, value) values ($1, $2)"),
          ok = Module:bind(C, S, [1, <<"foo">>]),
          {error, #error{code = <<"23505">>, codename = unique_violation}} = Module:execute(C, S, 0),
          {error, sync_required} = Module:bind(C, S, [3, <<"quux">>]),
          ok = Module:sync(C),
          ok = Module:bind(C, S, [3, <<"quux">>]),
          {ok, _} = Module:execute(C, S, 0),
          {ok, 1} = Module:squery(C, "delete from test_table1 where id = 3")
      end).

describe_test(Module) ->
    with_connection(
      Module,
      fun(C) ->
              {ok, S} = Module:parse(C, "select * from test_table1"),
              [#column{name = <<"id">>}, #column{name = <<"value">>}] = S#statement.columns,
              {ok, S} = Module:describe(C, S),
              ok = Module:close(C, S),
              ok = Module:sync(C)
      end).

describe_with_param_test(Module) ->
    with_connection(
      Module,
      fun(C) ->
              {ok, S} = Module:parse(C, "select id from test_table1 where id = $1"),
              [int4] = S#statement.types,
              [#column{name = <<"id">>}] = S#statement.columns,
              {ok, S} = Module:describe(C, S),
              ok = Module:close(C, S),
              ok = Module:sync(C)
      end).

describe_named_test(Module) ->
    with_connection(
      Module,
      fun(C) ->
              {ok, S} = Module:parse(C, "name", "select * from test_table1", []),
              [#column{name = <<"id">>}, #column{name = <<"value">>}] = S#statement.columns,
              {ok, S} = Module:describe(C, S),
              ok = Module:close(C, S),
              ok = Module:sync(C)
      end).

describe_error_test(Module) ->
    with_connection(
      Module,
      fun(C) ->
              {error, #error{}} = Module:describe(C, statement, ""),
              {ok, S} = Module:parse(C, "select * from test_table1"),
              {ok, S} = Module:describe(C, statement, ""),
              ok = Module:sync(C)

      end).

portal_test(Module) ->
    with_connection(
      Module,
      fun(C) ->
              {ok, S} = Module:parse(C, "select value from test_table1"),
              ok = Module:bind(C, S, []),
              {partial, [{<<"one">>}]} = Module:execute(C, S, 1),
              {partial, [{<<"two">>}]} = Module:execute(C, S, 1),
              {ok, []} = Module:execute(C, S,1),
              ok = Module:close(C, S),
              ok = Module:sync(C)
      end).

returning_test(Module) ->
    with_rollback(
      Module,
      fun(C) ->
              {ok, S} = Module:parse(C, "update test_table1 set value = $1 returning id"),
              ok = Module:bind(C, S, ["foo"]),
              {ok, 2, [{1}, {2}]} = Module:execute(C, S),
              ok = Module:sync(C)
      end).

multiple_statement_test(Module) ->
    with_connection(
      Module,
      fun(C) ->
              {ok, S1} = Module:parse(C, "one", "select value from test_table1 where id = 1", []),
              ok = Module:bind(C, S1, []),
              {partial, [{<<"one">>}]} = Module:execute(C, S1, 1),
              {ok, S2} = Module:parse(C, "two", "select value from test_table1 where id = 2", []),
              ok = Module:bind(C, S2, []),
              {partial, [{<<"two">>}]} = Module:execute(C, S2, 1),
              {ok, []} = Module:execute(C, S1, 1),
              {ok, []} = Module:execute(C, S2, 1),
              ok = Module:close(C, S1),
              ok = Module:close(C, S2),
              ok = Module:sync(C)
      end).

multiple_portal_test(Module) ->
    with_connection(
      Module,
      fun(C) ->
              {ok, S} = Module:parse(C, "select value from test_table1 where id = $1"),
              ok = Module:bind(C, S, "one", [1]),
              ok = Module:bind(C, S, "two", [2]),
              {ok, [{<<"one">>}]} = Module:execute(C, S, "one", 0),
              {ok, [{<<"two">>}]} = Module:execute(C, S, "two", 0),
              ok = Module:close(C, S),
              ok = Module:sync(C)
      end).

execute_function_test(Module) ->
    with_rollback(
      Module,
      fun(C) ->
              {ok, _Cols1, [{3}]} = Module:equery(C, "select insert_test1(3, 'three')"),
              {ok, _Cols2, [{<<>>}]} = Module:equery(C, "select do_nothing()")
      end).

parameter_get_test(Module) ->
    with_connection(
      Module,
      fun(C) ->
              {ok, <<"off">>} = Module:get_parameter(C, "is_superuser")
      end).

parameter_set_test(Module) ->
    with_connection(
      Module,
      fun(C) ->
              {ok, [], []} = Module:squery(C, "set DateStyle = 'ISO, MDY'"),
              {ok, <<"ISO, MDY">>} = Module:get_parameter(C, "DateStyle"),
              {ok, _Cols, [{<<"2000-01-02">>}]} = Module:squery(C, "select '2000-01-02'::date"),
              {ok, [], []} = Module:squery(C, "set DateStyle = 'German'"),
              {ok, <<"German, DMY">>} = Module:get_parameter(C, "DateStyle"),
              {ok, _Cols, [{<<"02.01.2000">>}]} = Module:squery(C, "select '2000-01-02'::date")
      end).

numeric_type_test(Module) ->
    check_type(Module, int2, "1", 1, [0, 256, -32768, +32767]),
    check_type(Module, int4, "1", 1, [0, 512, -2147483648, +2147483647]),
    check_type(Module, int8, "1", 1, [0, 1024, -9223372036854775808, +9223372036854775807]),
    check_type(Module, float4, "1.0", 1.0, [0.0, 1.23456, -1.23456]),
    check_type(Module, float8, "1.0", 1.0, [0.0, 1.23456789012345, -1.23456789012345]).

character_type_test(Module) ->
    Alpha = unicode:characters_to_binary([16#03B1]),
    Ka    = unicode:characters_to_binary([16#304B]),
    One   = unicode:characters_to_binary([16#10D360]),
    check_type(Module, bpchar, "'A'", $A, [1, $1, 16#7F, Alpha, Ka, One], "c_char"),
    check_type(Module, text, "'hi'", <<"hi">>, [<<"">>, <<"hi">>]),
    check_type(Module, varchar, "'hi'", <<"hi">>, [<<"">>, <<"hi">>]).

uuid_type_test(Module) ->
    check_type(Module, uuid,
               io_lib:format("'~s'", [uuid_to_string(?UUID1)]),
               list_to_binary(uuid_to_string(?UUID1)), []).

point_type_test(Module) ->
    check_type(Module, point, "'(23.15, 100)'", {23.15, 100.0}, []).

geometry_type_test(Module) ->
    check_type(Module, geometry, "'COMPOUNDCURVE(CIRCULARSTRING(0 0,1 1,1 0),(1 0,0 1))'",
      {compound_curve,'2d',
          [{circular_string,'2d',
              [{point,'2d',0.0,0.0,undefined,undefined},
               {point,'2d',1.0,1.0,undefined,undefined},
               {point,'2d',1.0,0.0,undefined,undefined}]},
         {line_string,'2d',
              [{point,'2d',1.0,0.0,undefined,undefined},
                 {point,'2d',0.0,1.0,undefined,undefined}]}]},
      []).

uuid_select_test(Module) ->
    with_rollback(
      Module,
      fun(C) ->
              U1 = uuid_to_string(?UUID1),
              U2 = uuid_to_string(?UUID2),
              U3 = uuid_to_string(?UUID3),
              {ok, 1} =
                  Module:equery(C, "insert into test_table2 (c_varchar, c_uuid) values ('UUID1', $1)",
                                [U1]),
              {ok, 1} =
                  Module:equery(C, "insert into test_table2 (c_varchar, c_uuid) values ('UUID2', $1)",
                                [U2]),
              {ok, 1} =
                  Module:equery(C, "insert into test_table2 (c_varchar, c_uuid) values ('UUID3', $1)",
                                [U3]),
              Res = Module:equery(C, "select c_varchar, c_uuid from test_table2 where c_uuid = any($1)",
                                 [[U1, U2]]),
              U1Bin = list_to_binary(U1),
              U2Bin = list_to_binary(U2),
              {ok,[{column,<<"c_varchar">>,varchar,_,_,_},
                   {column,<<"c_uuid">>,uuid,_,_,_}],
               [{<<"UUID1">>, U1Bin},
                {<<"UUID2">>, U2Bin}]} = Res
      end).


date_time_type_test(Module) ->
    with_connection(
      Module,
      fun(C) ->
              case Module:get_parameter(C, "integer_datetimes") of
                  {ok, <<"on">>}  -> MaxTsDate = 294276;
                  {ok, <<"off">>} -> MaxTsDate = 5874897
              end,

              check_type(Module, date, "'2008-01-02'", {2008,1,2}, [{-4712,1,1}, {5874897,1,1}]),
              check_type(Module, time, "'00:01:02'", {0,1,2.0}, [{0,0,0.0}, {24,0,0.0}]),
              check_type(Module, timetz, "'00:01:02-01'", {{0,1,2.0},1*60*60},
                         [{{0,0,0.0},0}, {{24,0,0.0},-13*60*60}]),
              check_type(Module, timestamp, "'2008-01-02 03:04:05'", {{2008,1,2},{3,4,5.0}},
                         [{{-4712,1,1},{0,0,0.0}}, {{MaxTsDate,12,31}, {23,59,59.0}}, {1322,334285,440966}]),
              check_type(Module, timestamptz, "'2011-01-02 03:04:05+3'", {{2011, 1, 2}, {0, 4, 5.0}}, [{1324,875970,286983}]),
              check_type(Module, interval, "'1 hour 2 minutes 3.1 seconds'", {{1,2,3.1},0,0},
                         [{{0,0,0.0},0,-178000000 * 12}, {{0,0,0.0},0,178000000 * 12}])
      end).

misc_type_test(Module) ->
    check_type(Module, bool, "true", true, [true, false]),
    check_type(Module, bytea, "E'\001\002'", <<1,2>>, [<<>>, <<0,128,255>>]).

hstore_type_test(Module) ->
    Values = [
        {[]},
        {[{null, null}]},
        {[{null, undefined}]},
        {[{1, null}]},
        {[{1.0, null}]},
        {[{1, undefined}]},
        {[{1.0, undefined}]},
        {[{<<"a">>, <<"c">>}, {<<"c">>, <<"d">>}]},
        {[{<<"a">>, <<"c">>}, {<<"c">>, null}]},
        {[{<<"a">>, <<"c">>}, {<<"c">>, undefined}]}
    ],
    with_connection(
      Module,
      fun(_C) ->
              check_type(Module, hstore, "''", {[]}, []),
              check_type(Module, hstore,
                         "'a => 1, b => 2.0, c => null'",
                         {[{<<"c">>, null}, {<<"b">>, <<"2.0">>}, {<<"a">>, <<"1">>}]}, Values)
      end).

net_type_test(Module) ->
    check_type(Module, cidr, "'127.0.0.1/32'", {{127,0,0,1}, 32}, [{{127,0,0,1}, 32}, {{0,0,0,0,0,0,0,1}, 128}]),
    check_type(Module, inet, "'127.0.0.1'", {127,0,0,1}, [{127,0,0,1}, {0,0,0,0,0,0,0,1}]).

array_type_test(Module) ->
    with_connection(
      Module,
      fun(C) ->
          {ok, _, [{[1, 2]}]} = Module:equery(C, "select ($1::int[])[1:2]", [[1, 2, 3]]),
          Select = fun(Type, A) ->
                       Query = "select $1::" ++ atom_to_list(Type) ++ "[]",
                       {ok, _Cols, [{A2}]} = Module:equery(C, Query, [A]),
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
          Select(varchar,   [<<"one">>, <<"two>">>]),
          Select(float4, [0.0, 1.0, 0.123]),
          Select(float8, [0.0, 1.0, 0.123]),
          Select(date, [{2008,1,2}, {2008,1,3}]),
          Select(time, [{0,1,2.0}, {0,1,3.0}]),
          Select(timetz, [{{0,1,2.0},1*60*60}, {{0,1,3.0},1*60*60}]),
          Select(timestamp, [{{2008,1,2},{3,4,5.0}}, {{2008,1,2},{3,4,6.0}}]),
          Select(timestamptz, [{{2008,1,2},{3,4,5.0}}, {{2008,1,2},{3,4,6.0}}]),
          Select(interval, [{{1,2,3.1},0,0}, {{1,2,3.2},0,0}]),
          Select(hstore, [{[{null, null}, {a, 1}, {1, 2}, {b, undefined}]}]),
          Select(hstore, [[{[{null, null}, {a, 1}, {1, 2}, {b, undefined}]}, {[]}], [{[{a, 1}]}, {[{null, 2}]}]]),
          Select(cidr, [{{127,0,0,1}, 32}, {{0,0,0,0,0,0,0,1}, 128}]),
          Select(inet, [{127,0,0,1}, {0,0,0,0,0,0,0,1}])
      end).

text_format_test(Module) ->
    with_connection(
      Module,
      fun(C) ->
              Select = fun(Type, V) ->
                               V2 = list_to_binary(V),
                               Query = "select $1::" ++ Type,
                               {ok, _Cols, [{V2}]} = Module:equery(C, Query, [V]),
                               {ok, _Cols, [{V2}]} = Module:equery(C, Query, [V2])
                       end,
              Select("numeric", "123456")
      end).

query_timeout_test(Module) ->
    with_connection(
      Module,
      fun(C) ->
              {ok, _, _} = Module:squery(C, "SET statement_timeout = 500"),
              ?TIMEOUT_ERROR = Module:squery(C, "SELECT pg_sleep(1)"),
              ?TIMEOUT_ERROR = Module:equery(C, "SELECT pg_sleep(2)"),
              {ok, _Cols, [{1}]} = Module:equery(C, "SELECT 1")
      end,
      []).

execute_timeout_test(Module) ->
    with_connection(
      Module,
      fun(C) ->
              {ok, _, _} = Module:squery(C, "SET statement_timeout = 500"),
              {ok, S} = Module:parse(C, "select pg_sleep($1)"),
              ok = Module:bind(C, S, [2]),
              ?TIMEOUT_ERROR = Module:execute(C, S, 0),
              ok = Module:sync(C),
              ok = Module:bind(C, S, [0]),
              {ok, [{<<>>}]} = Module:execute(C, S, 0),
              ok = Module:close(C, S),
              ok = Module:sync(C)
      end,
      []).

connection_closed_test(Module) ->
    P = self(),
    F = fun() ->
                process_flag(trap_exit, true),
                {ok, C} = Module:connect(?host, "epgsql_test",
                                         [{port, ?port}, {database, "epgsql_test_db1"}]),
                P ! {connected, C},
                receive
                    Any -> P ! Any
                end
        end,
    spawn_link(F),
    receive
        {connected, C} ->
            timer:sleep(100),
            Module:close(C),
            {'EXIT', C, _} = receive R -> R end
    end,
    flush().

active_connection_closed_test(Module) ->
    P = self(),
    F = fun() ->
                process_flag(trap_exit, true),
                {ok, C} = Module:connect(?host, [{database,
                                                  "epgsql_test_db1"}, {port, ?port}]),
                P ! {connected, C},
                R = Module:squery(C, "select pg_sleep(10)"),
                P ! R
        end,
    spawn_link(F),
    receive
        {connected, C} ->
            timer:sleep(100),
            Module:close(C),
            {error, closed} = receive R -> R end
    end,
    flush().

warning_notice_test(Module) ->
    with_connection(
      Module,
      fun(C) ->
          Q = "create function pg_temp.raise() returns void as $$
               begin
                 raise warning 'oops';
               end;
               $$ language plpgsql;
               select pg_temp.raise()",
          [{ok, _, _}, _] = Module:squery(C, Q),
          receive
              {epgsql, C, {notice, #error{message = <<"oops">>}}} -> ok
          after
              100 -> erlang:error(didnt_receive_notice)
          end
      end,
      [{async, self()}]).

listen_notify_test(Module) ->
    with_connection(
      Module,
      fun(C) ->
          {ok, [], []}     = Module:squery(C, "listen epgsql_test"),
          {ok, _, [{Pid}]} = Module:equery(C, "select pg_backend_pid()"),
          {ok, [], []}     = Module:squery(C, "notify epgsql_test"),
          receive
              {epgsql, C, {notification, <<"epgsql_test">>, Pid, <<>>}} -> ok
          after
              100 -> erlang:error(didnt_receive_notification)
          end
      end,
      [{async, self()}]).

listen_notify_payload_test(Module) ->
    with_min_version(
      Module,
      9.0,
      fun(C) ->
          {ok, [], []}     = Module:squery(C, "listen epgsql_test"),
          {ok, _, [{Pid}]} = Module:equery(C, "select pg_backend_pid()"),
          {ok, [], []}     = Module:squery(C, "notify epgsql_test, 'test!'"),
          receive
              {epgsql, C, {notification, <<"epgsql_test">>, Pid, <<"test!">>}} -> ok
          after
              100 -> erlang:error(didnt_receive_notification)
          end
      end,
      [{async, self()}]).

application_test(_Module) ->
    lists:foreach(fun application:start/1, ?ssl_apps),
    ok = application:start(epgsql),
    ok = application:stop(epgsql).

range_type_test(Module) ->
    with_min_version(
      Module,
      9.2,
      fun(_C) ->
          check_type(Module, int4range, "int4range(10, 20)", <<"[10,20)">>,
                     [{1, 58}, {-1, 12}, {-985521, 5412687}, {minus_infinity, 0},
                      {984655, plus_infinity}, {minus_infinity, plus_infinity}])
      end,
      []).

%% -- run all tests --

run_tests() ->
    Files = filelib:wildcard(filename:dirname(code:which(epgsql_tests))
                             ++ "/*tests.beam"),
    Mods = [list_to_atom(filename:basename(F, ".beam")) || F <- Files],
    eunit:test(Mods, []).

all_test_() ->
    Version =
        erlang:list_to_binary(
          re:replace(os:cmd("git rev-parse HEAD"), "\\s+", "")),

    with_connection(
      epgsql,
      fun(C) ->
              {ok, _Cols, [{DBVersion}]} = epgsql:squery(C, "SELECT version FROM schema_version"),
              case DBVersion == Version of
                  false ->
                      error_logger:info_msg("Git version of test schema does not match: ~p ~p~nPlease run make create_testdbs to update your test databases", [Version, DBVersion]),
                      erlang:exit(1);
                  _ ->
                      undefined
              end
      end),

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
    [WithModule(epgsql),
     WithModule(epgsql_cast),
     WithModule(epgsql_incremental)].

%% -- internal functions --

connect_only(Module, Args) ->
    TestOpts = [{port, ?port}],
    case Args of
        [User, Opts]       -> Args2 = [User, TestOpts ++ Opts];
        [User, Pass, Opts] -> Args2 = [User, Pass, TestOpts ++ Opts];
        Opts               -> Args2 = [TestOpts ++ Opts]
    end,
    {ok, C} = apply(Module, connect, [?host | Args2]),
    Module:close(C),
    flush().

with_connection(Module, F) ->
    with_connection(Module, F, "epgsql_test", []).

with_connection(Module, F, Args) ->
    with_connection(Module, F, "epgsql_test", Args).

with_connection(Module, F, Username, Args) ->
    Args2 = [{port, ?port}, {database, "epgsql_test_db1"} | Args],
    {ok, C} = Module:connect(?host, Username, Args2),
    try
        F(C)
    after
        Module:close(C)
    end,
    flush().

with_rollback(Module, F) ->
    with_connection(
      Module,
      fun(C) ->
              try
                  Module:squery(C, "begin"),
                  F(C)
                  after
                      Module:squery(C, "rollback")
                  end
      end).

with_min_version(Module, Min, F, Args) ->
    with_connection(
      Module,
      fun(C) ->
          {ok, Bin} = Module:get_parameter(C, <<"server_version">>),
          {ok, [{float, 1, Ver} | _], _} = erl_scan:string(binary_to_list(Bin)),
          case Ver >= Min of
              true  -> F(C);
              false -> ?debugFmt("skipping test requiring PostgreSQL >= ~.2f~n", [Min])
          end
      end,
      Args).

check_type(Module, Type, In, Out, Values) ->
    Column = "c_" ++ atom_to_list(Type),
    check_type(Module, Type, In, Out, Values, Column).

check_type(Module, Type, In, Out, Values, Column) ->
    with_connection(
      Module,
      fun(C) ->
              Select = io_lib:format("select ~s::~w", [In, Type]),
              Res = Module:equery(C, Select),
              {ok, [#column{type = Type}], [{Out}]} = Res,
              Sql = io_lib:format("insert into test_table2 (~s) values ($1) returning ~s", [Column, Column]),
              {ok, #statement{columns = [#column{type = Type}]} = S} = Module:parse(C, Sql),
              Insert = fun(V) ->
                               ok = Module:bind(C, S, [V]),
                               {ok, 1, [{V2}]} = Module:execute(C, S),
                               case compare(Type, V, V2) of
                                   true  -> ok;
                                   false -> ?debugFmt("~p =/= ~p~n", [V, V2]), ?assert(false)
                               end,
                               ok = Module:sync(C)
                       end,
              lists:foreach(Insert, [null, undefined | Values])
      end).

compare(_Type, null, null)      -> true;
compare(_Type, undefined, null) -> true;
compare(float4, V1, V2)         -> abs(V2 - V1) < 0.000001;
compare(float8, V1, V2)         -> abs(V2 - V1) < 0.000000000000001;
compare(hstore, {V1}, V2)       -> compare(hstore, V1, V2);
compare(hstore, V1, {V2})       -> compare(hstore, V1, V2);
compare(hstore, V1, V2)         ->
    orddict:from_list(format_hstore(V1)) =:= orddict:from_list(format_hstore(V2));
compare(Type, V1 = {_, _, MS}, {D2, {H2, M2, S2}}) when Type == timestamp;
                                                        Type == timestamptz ->
    {D1, {H1, M1, S1}} = calendar:now_to_universal_time(V1),
    ({D1, H1, M1} =:= {D2, H2, M2}) and (abs(S1 + MS/1000000 - S2) < 0.000000000000001);
compare(int4range, {Lower, Upper}, Result) ->
  translate_infinities(Lower, Upper) =:= Result;
compare(_Type, V1, V2)     -> V1 =:= V2.

translate_infinities(Lower, Upper) ->
  iolist_to_binary([lower(Lower), [","], upper(Upper)]).

lower(minus_infinity) ->
  "(";
lower(Val) ->
  io_lib:format("[~p", [Val]).

upper(plus_infinity) ->
  ")";
upper(Val) ->
  io_lib:format("~p)", [Val]).

format_hstore({Hstore}) -> Hstore;
format_hstore(Hstore) ->
    [{format_hstore_key(Key), format_hstore_value(Value)} || {Key, Value} <- Hstore].

format_hstore_key(Key) -> format_hstore_string(Key).

format_hstore_value(null) -> null;
format_hstore_value(undefined) -> null;
format_hstore_value(Value) -> format_hstore_string(Value).

format_hstore_string(Num) when is_number(Num) -> iolist_to_binary(io_lib:format("~w", [Num]));
format_hstore_string(Str) -> iolist_to_binary(io_lib:format("~s", [Str])).

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
