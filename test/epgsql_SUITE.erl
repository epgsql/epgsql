-module(epgsql_SUITE).

-include_lib("stdlib/include/assert.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("public_key/include/public_key.hrl").
-include("epgsql_tests.hrl").
-include("epgsql.hrl").

-export([
    init_per_suite/1,
    init_per_group/2,
    all/0,
    groups/0,
    end_per_group/2,
    end_per_suite/1
]).

%% logger handler
-export([log/2]).

-compile([export_all, nowarn_export_all]).

modules() ->
    [
     epgsql,
     epgsql_cast,
     epgsql_incremental
    ].

init_per_suite(Config) ->
    Config.

all() ->
    [{group, M} || M <- modules()].

groups() ->
    SubGroups = [
        {connect, [parallel], [
            connect,
            connect_with_application_name,
            connect_to_db,
            connect_as,
            connect_with_cleartext,
            connect_with_md5,
            connect_with_scram,
            connect_with_invalid_user,
            connect_with_invalid_password,
            connect_to_invalid_database,
            connect_with_other_error,
            connect_with_ssl,
            cancel_query_for_connection_with_ssl,
            cancel_query_for_connection_with_gen_tcp,
            connect_with_client_cert,
            connect_with_invalid_client_cert,
            connect_to_closed_port,
            connect_map,
            connect_proplist
        ]},
        {types, [parallel], [
            numeric_type,
            character_type,
            uuid_type,
            point_type,
            geometry_type,
            uuid_select,
            date_time_type,
            json_type,
            misc_type,
            hstore_type,
            net_type,
            array_type,
            record_type,
            range_type,
            range8_type,
            date_time_range_type,
            custom_types,
            custom_null
        ]},
        {pipelining, [parallel], [
            pipelined_prepared_query,
            pipelined_parse_batch_execute
        ]},
        {incremental_sock_active, [parallel], [
            incremental_sock_active_n,
            incremental_sock_active_n_ssl
        ]},
        {generic, [parallel], [
            with_transaction,
            mixed_api,
            redacted_state
        ]}
    ],

    Tests = [
        {group, connect},
        {group, types},

        prepared_query,
        select,
        insert,
        update,
        delete,
        create_and_drop_table,
        cursor,
        multiple_result,
        execute_batch,
        execute_batch_3_named_stmt,
        execute_batch_3_unnamed_stmt,
        execute_batch_3_sql,
        batch_error,
        single_batch,
        extended_select,
        extended_sync_ok,
        extended_sync_error,
        returning_from_insert,
        returning_from_update,
        returning_from_delete,
        parse,
        parse_column_format,
        parse_error,
        parse_and_close,
        bind,
        bind_parameter_format,
        bind_error,
        bind_and_close,
        execute_error,
        describe,
        describe_with_param,
        describe_named,
        describe_error,
        describe_portal,
        portal,
        returning,
        multiple_statement,
        multiple_portal,
        execute_function,
        parameter_get,
        parameter_set,

        text_format,
        query_timeout,
        execute_timeout,
        connection_closed,
        connection_closed_by_server,
        active_connection_closed,
        warning_notice,
        listen_notify,
        listen_notify_payload,
        set_notice_receiver,
        get_cmd_status,
        get_backend_pid
    ],
    SubGroups ++
        [{epgsql, [], [{group, generic} | Tests]},
         {epgsql_cast, [], [{group, pipelining} | Tests]},
         {epgsql_incremental, [], [{group, incremental_sock_active} | Tests]}].

end_per_suite(_Config) ->
    ok.

init_per_group(GroupName, Config) ->
    case lists:member(GroupName, modules()) of
        true -> [{module, GroupName}|Config];
        false -> Config
    end.
end_per_group(_GroupName, _Config) ->
    ok.

-define(UUID1,
        <<163,189,240,40,149,151,17,227,141,6,112,24,139,130,16,73>>).

-define(UUID2,
        <<183,55,22,52,149,151,17,227,187,167,112,24,139,130,16,73>>).

-define(UUID3,
        <<198,188,155,66,149,151,17,227,138,98,112,24,139,130,16,73>>).

-define(TIMEOUT_ERROR, {error, #error{
        severity = error,
        code = <<"57014">>,
        codename = query_canceled,
        message = <<"canceling statement due to statement timeout">>,
        extra = [{file, <<"postgres.c">>},
                 {line, _},
                 {routine, _} | _]
        }}).

-define(QUERY_CANCELED, {error, #error{
        severity = error,
        code = <<"57014">>,
        codename = query_canceled,
        message = <<"canceling statement due to user request">>,
        extra = [{file, <<"postgres.c">>},
                 {line, _},
                 {routine, _} | _]
        }}).

%% From uuid.erl in http://gitorious.org/avtobiff/erlang-uuid
uuid_to_bin_string(<<U0:32, U1:16, U2:16, U3:16, U4:48>>) ->
    iolist_to_binary(io_lib:format(
                       "~8.16.0b-~4.16.0b-~4.16.0b-~4.16.0b-~12.16.0b",
                       [U0, U1, U2, U3, U4])).

connect(Config) ->
    epgsql_ct:connect_only(Config, []).

connect_with_application_name(Config) ->
    Module = ?config(module, Config),
    Fun = fun(C) ->
              Query = "select application_name from pg_stat_activity",
              {ok, _Columns, Rows} = Module:equery(C, Query),
              ?assert(lists:member({<<"app_test">>}, Rows))
          end,
    epgsql_ct:with_connection(Config,
                              Fun,
                              "epgsql_test",
                              [{application_name, "app_test"}]).

connect_to_db(Connect) ->
    epgsql_ct:connect_only(Connect, [{database, "epgsql_test_db1"}]).

connect_as(Config) ->
    epgsql_ct:connect_only(Config, ["epgsql_test", [{database, "epgsql_test_db1"}]]).

connect_with_cleartext(Config) ->
    epgsql_ct:connect_only(Config, [
        "epgsql_test_cleartext",
        "epgsql_test_cleartext",
        [{database, "epgsql_test_db1"}]
    ]).

connect_with_md5(Config) ->
    epgsql_ct:connect_only(Config, [
        "epgsql_test_md5",
        "epgsql_test_md5",
        [{database, "epgsql_test_db1"}]
    ]).

connect_with_scram(Config) ->
    PgConf = ?config(pg_config, Config),
    Ver = ?config(version, PgConf),
    (Ver >= [10, 0])
        andalso
        epgsql_ct:connect_only(
          Config,
          [
           "epgsql_test_scram",
           "epgsql_test_scram",
           [{database, "epgsql_test_db1"}]
          ]).

connect_with_invalid_user(Config) ->
    {Host, Port} = epgsql_ct:connection_data(Config),
    Module = ?config(module, Config),
    {error, Why} = Module:connect(
        Host,
        "epgsql_test_invalid",
        "epgsql_test_invalid",
        [{port, Port}, {database, "epgsql_test_db1"}]),
    case Why of
        invalid_authorization_specification -> ok; % =< 8.4
        invalid_password                    -> ok  % >= 9.0
    end.

connect_with_invalid_password(Config) ->
    {Host, Port} = epgsql_ct:connection_data(Config),
    Module = ?config(module, Config),
    {error, Why} = Module:connect(
        Host,
        "epgsql_test_md5",
        "epgsql_test_invalid",
        [{port, Port}, {database, "epgsql_test_db1"}]),
    case Why of
        invalid_authorization_specification -> ok; % =< 8.4
        invalid_password                    -> ok  % >= 9.0
    end.

connect_to_invalid_database(Config) ->
    {Host, Port} = epgsql_ct:connection_data(Config),
    Module = ?config(module, Config),
    ?assertMatch(
       {error, invalid_authorization_specification},
       Module:connect(
        Host,
        "epgsql_test_md5",
        "epgsql_test_md5",
        [{port, Port}, {database, "epgsql_test_invalid_db"}])).

connect_with_other_error(Config) ->
    {Host, Port} = epgsql_ct:connection_data(Config),
    Module = ?config(module, Config),
    ?assertMatch(
       {error,
        #error{severity = fatal,
               codename = protocol_violation}},
       Module:connect(
        Host,
        <<0, 0>>,
        "epgsql_test_invalid",
        [{port, Port}, {database, <<0, 0>>}])).

connect_with_ssl(Config) ->
    Module = ?config(module, Config),
    epgsql_ct:with_connection(Config,
        fun(C) ->
             {ok, _Cols, [{true}]} = Module:equery(C, "select ssl_is_used()")
        end,
        "epgsql_test",
        [{ssl, true}]).

cancel_query_for_connection_with_ssl(Config) ->
    Module = ?config(module, Config),
    {Host, Port} = epgsql_ct:connection_data(Config),
    Module = ?config(module, Config),
    Args2 = [ {port, Port}, {database, "epgsql_test_db1"}
            | [ {ssl, true}
              , {timeout, 1000} ]
            ],
    {ok, C} = Module:connect(Host, "epgsql_test", Args2),
    ?assertMatch({ok, _Cols, [{true}]},
                Module:equery(C, "select ssl_is_used()")),
    Self = self(),
    spawn_link(fun() ->
                   ?assertMatch(?QUERY_CANCELED, Module:equery(C, "SELECT pg_sleep(5)")),
                   Self ! done
               end),
    %% this timer is needed for the test not to be flaky
    timer:sleep(1000),
    epgsql:cancel(C),
    receive done ->
        ?assert(true)
    after 5000 ->
        epgsql:close(C),
        ?assert(false)
    end,
    epgsql_ct:flush().

cancel_query_for_connection_with_gen_tcp(Config) ->
    Module = ?config(module, Config),
    {Host, Port} = epgsql_ct:connection_data(Config),
    Module = ?config(module, Config),
    Args2 = [ {port, Port}, {database, "epgsql_test_db1"}
            | [ {timeout, 1000} ]
            ],
    {ok, C} = Module:connect(Host, "epgsql_test", Args2),
    process_flag(trap_exit, true),
    Self = self(),
    spawn_link(fun() ->
                   ?assertMatch(?QUERY_CANCELED, Module:equery(C, "SELECT pg_sleep(5)")),
                   Self ! done
               end),
    %% this timer is needed for the test not to be flaky
    timer:sleep(1000),
    epgsql:cancel(C),
    receive done ->
        ?assert(true)
    after 5000 ->
        epgsql:close(C),
        ?assert(false)
    end,
    epgsql_ct:flush().

connect_with_client_cert(Config) ->
    Module = ?config(module, Config),
    Dir = filename:join(code:lib_dir(epgsql), ?TEST_DATA_DIR),
    File = fun(Name) -> filename:join(Dir, Name) end,
    {ok, Pem} = file:read_file(File("client.crt")),
    [{'Certificate', Der, not_encrypted}] = public_key:pem_decode(Pem),
    Cert = public_key:pkix_decode_cert(Der, plain),
    #'TBSCertificate'{serialNumber = Serial} = Cert#'Certificate'.tbsCertificate,
    Serial2 = list_to_binary(integer_to_list(Serial)),

    epgsql_ct:with_connection(Config,
         fun(C) ->
             ?assertMatch({ok, _, [{true}]}, Module:equery(C, "select ssl_is_used()")),
             ?assertMatch({ok, _, [{Serial2}]}, Module:equery(C, "select ssl_client_serial()"))
         end,
         "epgsql_test_cert",
        [{ssl, true}, {ssl_opts, [{keyfile, File("client.key")},
                                  {certfile, File("client.crt")}]}]).

connect_with_invalid_client_cert(Config) ->
    {Host, Port} = epgsql_ct:connection_data(Config),
    Module = ?config(module, Config),
    Dir = filename:join(code:lib_dir(epgsql), ?TEST_DATA_DIR),
    File = fun(Name) -> filename:join(Dir, Name) end,
    Trap = process_flag(trap_exit, true),
    %% pre-otp23:
    %% {error,
    %%   {ssl_negotiation_failed,
    %%     {tls_alert,
    %%       {unknown_ca, "received SERVER ALERT: Fatal - Unknown CA"}}}}
    %% otp23+:
    %% {error,
    %%   {sock_error,
    %%     {tls_alert,
    %%       {unknown_ca, "TLS client: <..> received SERVER ALERT: Fatal - Unknown CA\n"}}}}
    ?assertMatch(
       {error, {Err, {tls_alert, _}}} when Err == ssl_negotiation_failed;
                                           Err == sock_error,
       Module:connect(
         #{username => "epgsql_test_cert",
           database => "epgsql_test_db1",
           host => Host,
           port => Port,
           ssl => true,
           ssl_opts =>
               [{keyfile, File("bad-client.key")},
                {certfile, File("bad-client.crt")},
                %% TLS-1.3 seems to connect fine, but then sends alert asynchronously
                {versions, ['tlsv1.2']}
               ]}
        )),
    ?assertMatch({'EXIT', _, {Err, {tls_alert, _}}} when Err == ssl_negotiation_failed;
                                                         Err == sock_error,
                 receive Stop -> Stop end),
    process_flag(trap_exit, Trap).

connect_map(Config) ->
    {Host, Port} = epgsql_ct:connection_data(Config),
    Module = ?config(module, Config),

    Opts = #{
        host => Host,
        port => Port,
        database => "epgsql_test_db1",
        username => "epgsql_test_md5",
        password => "epgsql_test_md5"
    },
    {ok, C} = Module:connect(Opts),
    Module:close(C),
    epgsql_ct:flush(),
    ok.

connect_proplist(Config) ->
    {Host, Port} = epgsql_ct:connection_data(Config),
    Module = ?config(module, Config),

    Opts = [
        {host, Host},
        {port, Port},
        {database, "epgsql_test_db1"},
        {username, "epgsql_test_md5"},
        {password, "epgsql_test_md5"}
    ],
    {ok, C} = Module:connect(Opts),
    Module:close(C),
    epgsql_ct:flush(),
    ok.

connect_to_closed_port(Config) ->
    {Host, Port} = epgsql_ct:connection_data(Config),
    Module = ?config(module, Config),
    Trap = process_flag(trap_exit, true),
    ?assertEqual({error, econnrefused},
                 Module:connect(
                   Host,
                   "epgsql_test",
                   "epgsql_test",
                   [{port, Port + 1}, {database, "epgsql_test_db1"}])),
    ?assertMatch({'EXIT', _, econnrefused}, receive Stop -> Stop end),
    process_flag(trap_exit, Trap).

prepared_query(Config) ->
    Module = ?config(module, Config),
    epgsql_ct:with_connection(Config, fun(C) ->
        {ok, Stmt} = Module:parse(C, "inc", "select $1+1", []),
        {ok, Cols, [{5}]} = Module:prepared_query(C, "inc", [4]),
        {ok, Cols, [{2}]} = Module:prepared_query(C, "inc", [1]),
        {ok, Cols, [{23}]} = Module:prepared_query(C, "inc", [22]),
        {ok, Cols, [{34}]} = Module:prepared_query(C, Stmt, [33]),
        {error, #error{codename = invalid_sql_statement_name}} =
            Module:prepared_query(C, "non_existent_query", [4])
    end).

select(Config) ->
    Module = ?config(module, Config),
    epgsql_ct:with_connection(Config, fun(C) ->
        {ok, Cols, Rows} = Module:squery(C, "select * from test_table1"),
        [
            #column{name = <<"id">>, type = int4, size = 4},
            #column{name = <<"value">>, type = text, size = -1}
        ] = Cols,
        [{<<"1">>, <<"one">>}, {<<"2">>, <<"two">>}] = Rows
      end).

insert(Config) ->
    Module = ?config(module, Config),
    epgsql_ct:with_rollback(Config, fun(C) ->
        {ok, 1} = Module:squery(C, "insert into test_table1 (id, value) values (3, 'three')")
    end).

update(Config) ->
    Module = ?config(module, Config),
    epgsql_ct:with_rollback(Config, fun(C) ->
        {ok, 1} = Module:squery(C, "insert into test_table1 (id, value) values (3, 'three')"),
        {ok, 1} = Module:squery(C, "insert into test_table1 (id, value) values (4, 'four')"),
        {ok, 2} = Module:squery(C, "update test_table1 set value = 'foo' where id > 2"),
        {ok, _, [{<<"2">>}]} = Module:squery(C, "select count(*) from test_table1 where value = 'foo'")
    end).

delete(Config) ->
    Module = ?config(module, Config),
    epgsql_ct:with_rollback(Config, fun(C) ->
        {ok, 1} = Module:squery(C, "insert into test_table1 (id, value) values (3, 'three')"),
        {ok, 1} = Module:squery(C, "insert into test_table1 (id, value) values (4, 'four')"),
        {ok, 2} = Module:squery(C, "delete from test_table1 where id > 2"),
        {ok, _, [{<<"2">>}]} = Module:squery(C, "select count(*) from test_table1")
    end).

create_and_drop_table(Config) ->
    Module = ?config(module, Config),
    epgsql_ct:with_rollback(Config, fun(C) ->
        {ok, [], []} = Module:squery(C, "create table test_table3 (id int4)"),
        {ok, [#column{type = int4}], []} = Module:squery(C, "select * from test_table3"),
        {ok, [], []} = Module:squery(C, "drop table test_table3")
    end).

cursor(Config) ->
    Module = ?config(module, Config),
    epgsql_ct:with_connection(Config, fun(C) ->
        {ok, [], []} = Module:squery(C, "begin"),
        {ok, [], []} = Module:squery(C, "declare c cursor for select id from test_table1"),
        {ok, 2} = Module:squery(C, "move forward 2 from c"),
        {ok, 1} = Module:squery(C, "move backward 1 from c"),
        {ok, 1, _Cols, [{<<"2">>}]} = Module:squery(C, "fetch next from c"),
        {ok, [], []} = Module:squery(C, "close c")
    end).

multiple_result(Config) ->
    Module = ?config(module, Config),
    epgsql_ct:with_connection(Config, fun(C) ->
        Module:squery(C, "delete test_table1 where id = 3;"),
        [{ok, _, [{<<"1">>}]}, {ok, _, [{<<"2">>}]}] = Module:squery(C, "select 1; select 2"),
        [{ok, _, [{<<"1">>}]}, {error, #error{}}] = Module:squery(C, "select 1; select foo;"),
        [{ok, _, [{<<"one">>}]}, {ok, 1}, {ok, 1}] =
             Module:squery(C,
                 "select value from test_table1 where id = 1; "
                 "insert into test_table1 (id, value) values (3, 'three');"
                 "delete from test_table1 where id = 3;")
    end).

execute_batch(Config) ->
    Module = ?config(module, Config),
    epgsql_ct:with_connection(Config, fun(C) ->
        {ok, S1} = Module:parse(C, "one", "select $1", [int4]),
        {ok, S2} = Module:parse(C, "two", "select $1 + $2", [int4, int4]),
        [{ok, [{1}]}, {ok, [{3}]}] =
            Module:execute_batch(C, [{S1, [1]}, {S2, [1, 2]}])
    end).

execute_batch_3_named_stmt(Config) ->
    Module = ?config(module, Config),
    epgsql_ct:with_connection(Config, fun(C) ->
        {ok, Stmt} = Module:parse(C, "my_stmt", "select $1 + $2", [int4, int4]),
        ?assertMatch(
           {[#column{type = int4, _ = _}], [{ok, [{3}]}, {ok, [{7}]}]},
           Module:execute_batch(C, Stmt, [[1, 2], [3, 4]]))
    end).

execute_batch_3_unnamed_stmt(Config) ->
    Module = ?config(module, Config),
    epgsql_ct:with_connection(Config, fun(C) ->
        {ok, Stmt} = Module:parse(C, "select $1::integer + $2::integer"),
        ?assertMatch(
           {[#column{type = int4, _ = _}], [{ok, [{3}]}, {ok, [{7}]}]},
           Module:execute_batch(C, Stmt, [[2, 1], [4, 3]]))
    end).

execute_batch_3_sql(Config) ->
    Module = ?config(module, Config),
    epgsql_ct:with_connection(Config, fun(C) ->
        ?assertMatch(
           {[#column{type = int4, _ = _}], [{ok, [{3}]}, {ok, [{7}]}]},
           Module:execute_batch(C, "select $1::integer + $2::integer", [[1, 2], [3, 4]]))
    end).

batch_error(Config) ->
    Module = ?config(module, Config),
    epgsql_ct:with_rollback(Config, fun(C) ->
        {ok, S} = Module:parse(C, "insert into test_table1(id, value) values($1, $2)"),
        [{ok, 1}, {error, Error}] =
            Module:execute_batch(
              C,
              [{S, [3, "batch_error 3"]},
               {S, [2, "batch_error 2"]}, % duplicate key error
               {S, [5, "batch_error 5"]},  % won't be executed
               {S, [6, "batch_error 6"]}  % won't be executed
              ]),
        ?assertMatch(#error{}, Error)
    end).

single_batch(Config) ->
    Module = ?config(module, Config),
    epgsql_ct:with_connection(Config, fun(C) ->
        {ok, S1} = Module:parse(C, "one", "select $1", [int4]),
        [{ok, [{1}]}] = Module:execute_batch(C, [{S1, [1]}])
    end).

extended_select(Config) ->
    Module = ?config(module, Config),
    epgsql_ct:with_connection(Config, fun(C) ->
        {ok, Cols, Rows} = Module:equery(C, "select * from test_table1", []),
        [#column{name = <<"id">>, type = int4, size = 4},
         #column{name = <<"value">>, type = text, size = -1}] = Cols,
        [{1, <<"one">>}, {2, <<"two">>}] = Rows
    end).

extended_sync_ok(Config) ->
    Module = ?config(module, Config),
    epgsql_ct:with_connection(Config, fun(C) ->
        {ok, _Cols, [{<<"one">>}]} = Module:equery(C, "select value from test_table1 where id = $1", [1]),
        {ok, _Cols, [{<<"two">>}]} = Module:equery(C, "select value from test_table1 where id = $1", [2])
    end).

extended_sync_error(Config) ->
    Module = ?config(module, Config),
    epgsql_ct:with_connection(Config, fun(C) ->
        {error, #error{}} = Module:equery(C, "select _alue from test_table1 where id = $1", [1]),
        {ok, _Cols, [{<<"one">>}]} = Module:equery(C, "select value from test_table1 where id = $1", [1])
    end).

returning_from_insert(Config) ->
    Module = ?config(module, Config),
    epgsql_ct:with_rollback(Config, fun(C) ->
        {ok, 1, _Cols, [{3}]} = Module:equery(C, "insert into test_table1 (id) values (3) returning id")
    end).

returning_from_update(Config) ->
    Module = ?config(module, Config),
    epgsql_ct:with_rollback(Config, fun(C) ->
        {ok, 2, _Cols, [{1}, {2}]} = Module:equery(C, "update test_table1 set value = 'hi' returning id"),
        ?assertMatch({ok, 0, [#column{}], []},
                     Module:equery(C, "update test_table1 set value = 'hi' where false returning id")),
        ?assertMatch([{ok, 2, [#column{}], [{<<"1">>}, {<<"2">>}]},
               {ok, 0, [#column{}], []}],
                     Module:squery(C,
                            "update test_table1 set value = 'hi2' returning id; "
                            "update test_table1 set value = 'hi' where false returning id"))
    end).

returning_from_delete(Config) ->
    Module = ?config(module, Config),
    epgsql_ct:with_rollback(Config, fun(C) ->
        {ok, 2, _Cols, [{1}, {2}]} = Module:equery(C, "delete from test_table1 returning id"),
        ?assertMatch({ok, 0, [#column{}], []},
                     Module:equery(C, "delete from test_table1 returning id"))
    end).

parse(Config) ->
    Module = ?config(module, Config),
    epgsql_ct:with_connection(Config, fun(C) ->
        {ok, S} = Module:parse(C, "select * from test_table1"),
        [#column{name = <<"id">>}, #column{name = <<"value">>}] = S#statement.columns,
        ok = Module:close(C, S),
        ok = Module:sync(C)
    end).

parse_column_format(Config) ->
    Module = ?config(module, Config),
    epgsql_ct:with_connection(Config, fun(C) ->
        {ok, S} = Module:parse(C, "select 1::int4, false::bool, 2.0::float4"),
        [#column{type = int4, table_oid = 0, table_attr_number = 0},
         #column{type = bool, table_oid = 0, table_attr_number = 0},
         #column{type = float4, table_oid = 0, table_attr_number = 0}] = S#statement.columns,
        ok = Module:bind(C, S, []),
        {ok, [{1, false, 2.0}]} = Module:execute(C, S, 0),
        ok = Module:close(C, S),
        ok = Module:sync(C)
    end).

parse_error(Config) ->
    Module = ?config(module, Config),
    epgsql_ct:with_connection(Config, fun(C) ->
        {error, #error{
            extra = [{file, _}, {line, _}, {position, <<"8">>}, {routine, _} | _]
        }} = Module:parse(C, "select _ from test_table1"),
        {ok, S} = Module:parse(C, "select * from test_table1"),
        [#column{name = <<"id">>}, #column{name = <<"value">>}] = S#statement.columns,
        ok = Module:close(C, S),
        ok = Module:sync(C)
    end).

parse_and_close(Config) ->
    Module = ?config(module, Config),
    epgsql_ct:with_connection(Config, fun(C) ->
        Parse = fun() -> Module:parse(C, "test", "select * from test_table1", []) end,
        {ok, S} = Parse(),
        {error, #error{code = <<"42P05">>, codename = duplicate_prepared_statement}} = Parse(),
        Module:close(C, S),
        {ok, S} = Parse(),
        ok = Module:sync(C)
    end).

bind(Config) ->
    Module = ?config(module, Config),
    epgsql_ct:with_connection(Config, fun(C) ->
        {ok, S} = Module:parse(C, "select value from test_table1 where id = $1"),
        ok = Module:bind(C, S, [1]),
        ok = Module:close(C, S),
        ok = Module:sync(C)
    end).

bind_parameter_format(Config) ->
    Module = ?config(module, Config),
    epgsql_ct:with_connection(Config, fun(C) ->
        {ok, S} = Module:parse(C, "select $1, $2, $3", [int2, text, bool]),
        [int2, text, bool] = S#statement.types,
        ok = Module:bind(C, S, [1, "hi", true]),
        {ok, [{1, <<"hi">>, true}]} = Module:execute(C, S, 0),
        ok = Module:close(C, S),
        ok = Module:sync(C)
    end).

bind_error(Config) ->
    Module = ?config(module, Config),
    epgsql_ct:with_connection(Config, fun(C) ->
        {ok, S} = Module:parse(C, "select $1::char"),
        {error, #error{}} = Module:bind(C, S, [0]),
        ok = Module:bind(C, S, [$A]),
        ok = Module:close(C, S),
        ok = Module:sync(C)
    end).

bind_and_close(Config) ->
    Module = ?config(module, Config),
    epgsql_ct:with_connection(Config, fun(C) ->
        {ok, S} = Module:parse(C, "select * from test_table1"),
        ok = Module:bind(C, S, "one", []),
        {error, #error{code = <<"42P03">>, codename = duplicate_cursor}} = Module:bind(C, S, "one", []),
        ok = Module:close(C, portal, "one"),
        ok = Module:bind(C, S, "one", []),
        ok = Module:sync(C)
    end).

execute_error(Config) ->
    Module = ?config(module, Config),
    epgsql_ct:with_connection(Config, fun(C) ->
          {ok, S} = Module:parse(C, "insert into test_table1 (id, value) values ($1, $2)"),
          ok = Module:bind(C, S, [1, <<"foo">>]),
          {error, #error{
              code = <<"23505">>, codename = unique_violation,
              extra = [
                  {constraint_name, <<"test_table1_pkey">>},
                  {detail, _},
                  {file, _},
                  {line, _},
                  {routine, _},
                  {schema_name, <<"public">>} | _%,
                  %{table_name, <<"test_table1">>}
              ]
          }} = Module:execute(C, S, 0),
          {error, sync_required} = Module:bind(C, S, [3, <<"quux">>]),
          ok = Module:sync(C),
          ok = Module:bind(C, S, [3, <<"quux">>]),
          {ok, _} = Module:execute(C, S, 0),
          {ok, 1} = Module:squery(C, "delete from test_table1 where id = 3")
    end).

describe(Config) ->
    Module = ?config(module, Config),
    epgsql_ct:with_connection(Config, fun(C) ->
        {ok, S} = Module:parse(C, "select * from test_table1"),
        [#column{name = <<"id">>}, #column{name = <<"value">>}] = S#statement.columns,
        {ok, S} = Module:describe(C, S),
        ok = Module:close(C, S),
        ok = Module:sync(C)
    end).

describe_with_param(Config) ->
    Module = ?config(module, Config),
    epgsql_ct:with_connection(Config, fun(C) ->
        {ok, S} = Module:parse(C, "select id from test_table1 where id = $1"),
        ?assertEqual([int4], S#statement.types),
        ?assertMatch([#column{name = <<"id">>}], S#statement.columns),
        {ok, S} = Module:describe(C, S),
        ok = Module:close(C, S),
        ok = Module:sync(C)
    end).

describe_named(Config) ->
    Module = ?config(module, Config),
    epgsql_ct:with_connection(Config, fun(C) ->
        {ok, S} = Module:parse(C, "name", "select * from test_table1", []),
        [#column{name = <<"id">>}, #column{name = <<"value">>}] = S#statement.columns,
        {ok, S} = Module:describe(C, S),
        ok = Module:close(C, S),
        ok = Module:sync(C)
    end).

describe_error(Config) ->
    Module = ?config(module, Config),
    epgsql_ct:with_connection(Config, fun(C) ->
        {error, #error{}} = Module:describe(C, statement, ""),
        {ok, S} = Module:parse(C, "select * from test_table1"),
        {ok, S} = Module:describe(C, statement, ""),
        ok = Module:sync(C)

    end).

describe_portal(Config) ->
    Module = ?config(module, Config),
    epgsql_ct:with_connection(Config, fun(C) ->
        {ok, Stmt} = Module:parse(C, "my_stmt", "select * from test_table1 WHERE id = $1", []),
        ok = Module:bind(C, Stmt, "my_portal", [1]),
        {ok, Columns} = Module:describe(C, portal, "my_portal"),
        ?assertMatch(
           [#column{name = <<"id">>,
                    type = int4},
            #column{name = <<"value">>,
                    type = text}],
           Columns
          ),
        ok = Module:close(C, Stmt),
        ok = Module:sync(C)
    end).

portal(Config) ->
    Module = ?config(module, Config),
    epgsql_ct:with_connection(Config, fun(C) ->
        {ok, S} = Module:parse(C, "select value from test_table1"),
        ok = Module:bind(C, S, []),
        {partial, [{<<"one">>}]} = Module:execute(C, S, 1),
        {partial, [{<<"two">>}]} = Module:execute(C, S, 1),
        {ok, []} = Module:execute(C, S,1),
        ok = Module:close(C, S),
        ok = Module:sync(C)
    end).

returning(Config) ->
    Module = ?config(module, Config),
    epgsql_ct:with_rollback(Config, fun(C) ->
        {ok, S} = Module:parse(C, "update test_table1 set value = $1 returning id"),
        ok = Module:bind(C, S, ["foo"]),
        {ok, 2, [{1}, {2}]} = Module:execute(C, S),
        ok = Module:sync(C)
    end).

multiple_statement(Config) ->
    Module = ?config(module, Config),
    epgsql_ct:with_connection(Config, fun(C) ->
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

multiple_portal(Config) ->
    Module = ?config(module, Config),
    epgsql_ct:with_connection(Config, fun(C) ->
        {ok, S} = Module:parse(C, "select value from test_table1 where id = $1"),
        ok = Module:bind(C, S, "one", [1]),
        ok = Module:bind(C, S, "two", [2]),
        {ok, [{<<"one">>}]} = Module:execute(C, S, "one", 0),
        {ok, [{<<"two">>}]} = Module:execute(C, S, "two", 0),
        ok = Module:close(C, S),
        ok = Module:sync(C)
    end).

execute_function(Config) ->
    Module = ?config(module, Config),
    epgsql_ct:with_rollback(Config, fun(C) ->
        {ok, _Cols1, [{3}]} = Module:equery(C, "select insert_test1(3, 'three')"),
        {ok, _Cols2, [{<<>>}]} = Module:equery(C, "select do_nothing()")
    end).

parameter_get(Config) ->
    Module = ?config(module, Config),
    epgsql_ct:with_connection(Config, fun(C) ->
        {ok, <<"off">>} = Module:get_parameter(C, "is_superuser")
    end).

parameter_set(Config) ->
    Module = ?config(module, Config),
    epgsql_ct:with_connection(Config, fun(C) ->
        {ok, [], []} = Module:squery(C, "set DateStyle = 'ISO, MDY'"),
        {ok, <<"ISO, MDY">>} = Module:get_parameter(C, "DateStyle"),
        {ok, _Cols, [{<<"2000-01-02">>}]} = Module:squery(C, "select '2000-01-02'::date"),
        {ok, [], []} = Module:squery(C, "set DateStyle = 'German'"),
        {ok, <<"German, DMY">>} = Module:get_parameter(C, "DateStyle"),
        {ok, _Cols, [{<<"02.01.2000">>}]} = Module:squery(C, "select '2000-01-02'::date")
    end).

numeric_type(Config) ->
    check_type(Config, int2, "1", 1, [0, 256, -32768, +32767]),
    check_type(Config, int4, "1", 1, [0, 512, -2147483648, +2147483647]),
    check_type(Config, int8, "1", 1, [0, 1024, -9223372036854775808, +9223372036854775807]),
    check_type(Config, float4, "1.0", 1.0, [0.0, 1.23456, -1.23456]),
    check_type(Config, float4, "'-Infinity'", minus_infinity, [minus_infinity, plus_infinity, nan]),
    check_type(Config, float8, "1.0", 1.0, [0.0, 1.23456789012345, -1.23456789012345]),
    check_type(Config, float8, "'nan'", nan, [minus_infinity, plus_infinity, nan]),
    %% Check overflow protection. Connection just crashes for now instead of silently
    %% truncating the data. Some cleaner behaviour can be introduced later.
    epgsql_ct:with_connection(Config, fun(C) ->
        Module = ?config(module, Config),
        Trap = process_flag(trap_exit, true),
        try Module:equery(C, "SELECT $1::int2", [32768]) of
          {error, closed} ->
                %% epgsqla/epgsqli
                ok
        catch exit:Reason ->
                %% epgsql
                ?assertMatch({{{integer_overflow, int2, _}, _}, _}, Reason),
                receive {'EXIT', C, _} -> ok
                after 1000 -> error(timeout)
                end
        end,
        process_flag(trap_exit, Trap)
    end).

character_type(Config) ->
    Alpha = unicode:characters_to_binary([16#03B1]),
    Ka    = unicode:characters_to_binary([16#304B]),
    One   = unicode:characters_to_binary([16#10D360]),
    check_type(Config, bpchar, "'A'", $A, [1, $1, 16#7F, Alpha, Ka, One], "c_char"),
    check_type(Config, text, "'hi'", <<"hi">>, [<<"">>, <<"hi">>]),
    check_type(Config, varchar, "'hi'", <<"hi">>, [<<"">>, <<"hi">>]),
    epgsql_ct:with_connection(
      Config,
      fun(C) ->
              Module = ?config(module, Config),
              %% IOlists
              ?assertMatch({ok, _, [{<<1087/utf8, 1088/utf8, 1080/utf8,
                                        1074/utf8, 1077/utf8, 1090/utf8>>}]},
                           Module:equery(C, "SELECT $1::text", [[1087,1088,1080,1074,1077,1090]])),
              %% Deprecated casts
              ?assertMatch({ok, _, [{<<"my_atom">>}]},
                           Module:equery(C, "SELECT $1::varchar", [my_atom])),
              ?assertMatch({ok, _, [{<<"12345">>}]},
                           Module:equery(C, "SELECT $1::varchar", [12345])),
              FloatBin = erlang:float_to_binary(1.2345),
              ?assertMatch({ok, _, [{FloatBin}]},
                           Module:equery(C, "SELECT $1::varchar", [1.2345])),
              %% String bpchar
              ?assertMatch({ok, _, [{<<"hello world">>}]},
                           Module:equery(C, "SELECT $1::bpchar", ["hello world"]))
      end).

uuid_type(Config) ->
    check_type(Config, uuid,
               io_lib:format("'~s'", [uuid_to_bin_string(?UUID1)]),
               uuid_to_bin_string(?UUID1), []).

point_type(Config) ->
    check_type(Config, point, "'(23.15, 100)'", {23.15, 100.0}, []).

geometry_type(Config) ->
    check_type(Config, geometry, "'COMPOUNDCURVE(CIRCULARSTRING(0 0,1 1,1 0),(1 0,0 1))'",
        {compound_curve,'2d', [
            {circular_string,'2d', [
                {point,'2d',0.0,0.0,undefined,undefined},
                {point,'2d',1.0,1.0,undefined,undefined},
                {point,'2d',1.0,0.0,undefined,undefined}
            ]},
            {line_string,'2d', [
                {point,'2d',1.0,0.0,undefined,undefined},
                {point,'2d',0.0,1.0,undefined,undefined}
            ]}
        ]}, []).

uuid_select(Config) ->
    Module = ?config(module, Config),
    epgsql_ct:with_rollback(Config, fun(C) ->
        U1 = uuid_to_bin_string(?UUID1),
        U2 = uuid_to_bin_string(?UUID2),
        U3 = uuid_to_bin_string(?UUID3),
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
        ?assertMatch(
           {ok,[#column{name = <<"c_varchar">>, type = varchar},
                #column{name = <<"c_uuid">>, type = uuid}],
            [{<<"UUID1">>, U1},
             {<<"UUID2">>, U2}]}, Res)
    end).

date_time_type(Config) ->
    Module = ?config(module, Config),
    epgsql_ct:with_connection(Config, fun(C) ->
        case Module:get_parameter(C, "integer_datetimes") of
            {ok, <<"on">>}  -> MaxTsDate = 294276;
            {ok, <<"off">>} -> MaxTsDate = 5874897
        end,

        check_type(Config, date, "'2008-01-02'", {2008,1,2}, [{-4712,1,1}, {5874897,1,1}]),
        check_type(Config, time, "'00:01:02'", {0,1,2.0}, [{0,0,0.0}, {24,0,0.0}]),
        check_type(Config, timetz, "'00:01:02-01'", {{0,1,2.0},1*60*60},
                   [{{0,0,0.0},0}, {{24,0,0.0},-13*60*60}]),
        check_type(Config, timestamp, "'2008-01-02 03:04:05'", {{2008,1,2},{3,4,5.0}},
                   [{{-4712,1,1},{0,0,0.0}}, {{MaxTsDate,12,31}, {23,59,59.0}}, {1322,334285,440966}]),
        check_type(Config, timestamptz, "'2011-01-02 03:04:05+3'", {{2011, 1, 2}, {0, 4, 5.0}}, [{1324,875970,286983}]),
        check_type(Config, interval, "'1 hour 2 minutes 3.1 seconds'", {{1,2,3.1},0,0},
                   [{{0,0,0.0},0,-178000000 * 12}, {{0,0,0.0},0,178000000 * 12}])
    end).

json_type(Config) ->
    Module = ?config(module, Config),
    epgsql_ct:with_connection(Config, fun(C) ->
        check_type(Config, json, "'{}'", <<"{}">>,
                [<<"{}">>, <<"[]">>, <<"1">>, <<"1.0">>, <<"true">>, <<"\"string\"">>, <<"{\"key\": []}">>],
                get_type_col(json), C),
        check_type(Config, jsonb, "'{}'", <<"{}">>,
                [<<"{}">>, <<"[]">>, <<"1">>, <<"1.0">>, <<"true">>, <<"\"string\"">>, <<"{\"key\": []}">>],
                get_type_col(jsonb), C),
        epgsql:update_type_cache(C, [{epgsql_codec_json, epgsql_fake_json_mod}]),
        RowId = "json_type_custom_mod_" ++ atom_to_list(Module),
        {ok, 1} = Module:equery(C, "insert into test_table2 (c_varchar, c_json, c_jsonb) values ($1, $2, $3)", [RowId, {"{}"}, {"{}"}]),
        Res = Module:equery(C, "select c_json, c_jsonb from test_table2 where c_varchar = $1", [RowId]),
        ?assertMatch(
            {ok, [#column{name = <<"c_json">>, type = json}, #column{name = <<"c_jsonb">>, type = jsonb}],
                 [{{<<"{}">>}, {<<"{}">>}}]},
            Res
        )
    end).

misc_type(Config) ->
    check_type(Config, bool, "true", true, [true, false]),
    check_type(Config, bytea, "E'\001\002'", <<1,2>>, [<<>>, <<0,128,255>>]).

hstore_type(Config) ->
    Module = ?config(module, Config),
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
    check_type(Config, hstore, "''", {[]}, []),
    check_type(Config, hstore,
               "'a => 1, b => 2.0, c => null'",
               {[{<<"a">>, <<"1">>}, {<<"b">>, <<"2.0">>}, {<<"c">>, null}]}, Values),
    epgsql_ct:with_connection(
      Config,
      fun(C) ->
              %% Maps as input
              [begin
                   {ok, _, [{Res}]} = Module:equery(C, "select $1::hstore", [maps:from_list(KV)]),
                   ?assert(compare(hstore, Res, Jiffy))
               end || {KV} = Jiffy <- Values],
              %% Maps as output
              {ok, [hstore]} = epgsql:update_type_cache(
                                 C, [{epgsql_codec_hstore, #{return => map}}]),
              [begin
                   {ok, _, [{Res}]} = Module:equery(C, "select $1::hstore", [maps:from_list(KV)]),
                   HstoreMap = maps:from_list([{format_hstore_key(K), format_hstore_value(V)} || {K, V} <- KV]),
                   ?assertEqual(HstoreMap, Res)
               end || {KV} <- Values],
              %% Proplist as output
              {ok, [hstore]} = epgsql:update_type_cache(
                                 C, [{epgsql_codec_hstore, #{return => proplist}}]),
              [begin
                   {ok, _, [{Res}]} = Module:equery(C, "select $1::hstore", [Jiffy]),
                   HstoreProplist = [{format_hstore_key(K), format_hstore_value(V)} || {K, V} <- KV],
                   ?assertEqual(lists:sort(HstoreProplist), lists:sort(Res))
               end || {KV} = Jiffy <- Values],
              %% Custom nulls
              Nulls = [nil, 'NULL', aaaa],
              {ok, [hstore]} = epgsql:update_type_cache(
                                 C, [{epgsql_codec_hstore, #{return => map,
                                                             nulls => Nulls}}]),
              K = <<"k">>,
              [begin
                   {ok, _, [{Res}]} = Module:equery(C, "select $1::hstore", [#{K => V}]),
                   ?assertEqual(#{K => nil}, Res)
               end || V <- Nulls]
      end).

net_type(Config) ->
    check_type(Config, cidr, "'127.0.0.1/32'", {{127,0,0,1}, 32}, [{{127,0,0,1}, 32}, {{0,0,0,0,0,0,0,1}, 128}]),
    check_type(Config, inet, "'127.0.0.1'", {127,0,0,1}, [{127,0,0,1}, {0,0,0,0,0,0,0,1}]),
    %% macaddr8 available only on PG>=10
    check_type(Config, macaddr,
               "'FF:FF:FF:FF:FF:FF'", {255, 255, 255, 255, 255, 255},
               [{255, 255, 255, 255, 255, 255},
                {6, 0, 0, 0, 0, 0}]).

array_type(Config) ->
    Module = ?config(module, Config),
    epgsql_ct:with_connection(Config, fun(C) ->
        {ok, _, [{[1, 2]}]} = Module:equery(C, "select ($1::int[])[1:2]", [[1, 2, 3]]),
        {ok, _, [{[{1, <<"one">>}, {2, <<"two">>}]}]} =
            Module:equery(C, "select Array(select (id, value) from test_table1)", []),
        {ok, _, [{ [[1], [null], [3], [null]] }]} =
            Module:equery(C, "select $1::int2[]", [ [[1], [null], [3], [undefined]] ]),
        Select = fun(Type, AIn) ->
            Query = "select $1::" ++ atom_to_list(Type) ++ "[]",
            {ok, _Cols, [{AOut}]} = Module:equery(C, Query, [AIn]),
            case lists:all(fun({VIn, VOut}) ->
                                   compare(Type, VIn, VOut)
                           end, lists:zip(AIn, AOut)) of
                true  -> ok;
                false -> ?assertEqual(AIn, AOut)
            end
        end,
        Select(int2,   []),
        Select(int2,   [1, 2, 3, 4]),
        Select(int2,   [[1], [2], [3], [4]]),
        Select(int2,   [[[[[[1, 2]]]]]]),
        Select(int2,   [1, null, 3, undefined]),
        Select(int2,   [[1], [null], [3], [null]]),
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
        Select(inet, [{127,0,0,1}, {0,0,0,0,0,0,0,1}]),
        Select(json, [<<"{}">>, <<"[]">>, <<"1">>, <<"1.0">>, <<"true">>, <<"\"string\"">>, <<"{\"key\": []}">>]),
        Select(jsonb, [<<"{}">>, <<"[]">>, <<"1">>, <<"1.0">>, <<"true">>, <<"\"string\"">>, <<"{\"key\": []}">>])
    end).

record_type(Config) ->
    Module = ?config(module, Config),
    epgsql_ct:with_connection(Config, fun(C) ->
        Select = fun(Sql, Expected) ->
            {ok, _Columns, [Row]} = Module:equery(C, Sql, []),
            ?assertMatch(Expected, Row)
        end,

        %% Simple record
        Select("select (1,2)", {{1, 2}}),

        %% Record inside other record
        Select("select (1, (select (2,3)))", {{1, {2, 3}}}),

        %% Array inside record
        Select("select (1, '{2,3}'::int[])", {{1, [2, 3]}}),

        %% Array of records inside record
        Select("select (0, ARRAY(select (id, value) from test_table1))", {{0,[{1,<<"one">>},{2,<<"two">>}]}}),

        %% Record with NULLs
        Select("select (1, NULL::integer, 2)", {{1, null, 2}})
    end).

custom_types(Config) ->
    Module = ?config(module, Config),
    epgsql_ct:with_connection(Config, fun(C) ->
        Module:squery(C, "drop table if exists t_foo;"),
        Module:squery(C, "drop type if exists my_type;"),
        {ok, [], []} = Module:squery(C, "create type my_type as enum('foo', 'bar');"),
        {ok, [my_type]} = epgsql:update_type_cache(C, [{epgsql_codec_test_enum, [foo, bar]}]),
        {ok, [], []} = Module:squery(C, "create table t_foo (col my_type);"),
        {ok, S} = Module:parse(C, "insert_foo", "insert into t_foo values ($1)", [my_type]),
        ok = Module:bind(C, S, [bar]),
        {ok, 1} = Module:execute(C, S),
        ?assertMatch({ok, _, [{bar}]}, Module:equery(C, "SELECT col FROM t_foo"))
    end).

custom_null(Config) ->
    Module = ?config(module, Config),
    epgsql_ct:with_connection(Config, fun(C) ->
        Test3 = fun(Type, In, Out) ->
                        Q = ["SELECT $1::", Type],
                        {ok, _, [{Res}]} = Module:equery(C, Q, [In]),
                        ?assertEqual(Out, Res)
                end,
        Test = fun(Type, In) ->
                       Test3(Type, In, In)
               end,
        Test("int2", nil),
        Test3("int2", 'NULL', nil),
        Test("text", nil),
        Test3("text", 'NULL', nil),
        Test3("text", null, <<"null">>),
        Test("int2[]", [nil, 1, nil, 2]),
        Test3("text[]", [null, <<"ok">>], [<<"null">>, <<"ok">>]),
        Test3("int2[]", ['NULL', 1, nil, 2], [nil, 1, nil, 2]),
        Test("int2[]", [[nil], [1], [nil], [2]]),
        Test3("int2[]", [['NULL'], [1], [nil], [2]], [[nil], [1], [nil], [2]]),
        ?assertMatch(
           {ok, _, [{ {1, nil, {2, nil, 3}} }]},
           Module:equery(C, "SELECT (1, NULL, (2, NULL, 3))", []))
    end,
    [{nulls, [nil, 'NULL']}]).

text_format(Config) ->
    Module = ?config(module, Config),
    epgsql_ct:with_connection(Config, fun(C) ->
        Select = fun(Type, V) ->
            V2 = list_to_binary(V),
            Query = "select $1::" ++ Type,
            ?assertMatch({ok, _Cols, [{V2}]}, Module:equery(C, Query, [V])),
            ?assertMatch({ok, _Cols, [{V2}]}, Module:equery(C, Query, [V2]))
        end,
        Select("numeric", "123456")
    end).

query_timeout(Config) ->
    Module = ?config(module, Config),
    epgsql_ct:with_connection(Config, fun(C) ->
        {ok, _, _} = Module:squery(C, "SET statement_timeout = 500"),
        ?assertMatch(?TIMEOUT_ERROR, Module:squery(C, "SELECT pg_sleep(1)")),
        ?assertMatch(?TIMEOUT_ERROR, Module:equery(C, "SELECT pg_sleep(2)")),
        {ok, _Cols, [{1}]} = Module:equery(C, "SELECT 1")
    end, []).

execute_timeout(Config) ->
    Module = ?config(module, Config),
    epgsql_ct:with_connection(Config, fun(C) ->
        {ok, _, _} = Module:squery(C, "SET statement_timeout = 500"),
        {ok, S} = Module:parse(C, "select pg_sleep($1)"),
        ok = Module:bind(C, S, [2]),
        ?assertMatch(?TIMEOUT_ERROR, Module:execute(C, S, 0)),
        ok = Module:sync(C),
        ok = Module:bind(C, S, [0]),
        {ok, [{<<>>}]} = Module:execute(C, S, 0),
        ok = Module:close(C, S),
        ok = Module:sync(C)
    end, []).

connection_closed(Config) ->
    {Host, Port} = epgsql_ct:connection_data(Config),
    Module = ?config(module, Config),
    P = self(),
    spawn_link(fun() ->
        process_flag(trap_exit, true),
        {ok, C} = Module:connect(Host, "epgsql_test",[
            {port, Port},
            {database, "epgsql_test_db1"}
        ]),
        P ! {connected, C},
        receive
            Any -> P ! Any
        end
    end),
    receive
        {connected, C} ->
            timer:sleep(100),
            Module:close(C),
            {'EXIT', C, _} = receive R -> R end
    end,
    epgsql_ct:flush().

connection_closed_by_server(Config) ->
    Module = ?config(module, Config),
    epgsql_ct:with_connection(Config, fun(C1) ->
        P = self(),
        spawn_link(fun() ->
            process_flag(trap_exit, true),
            epgsql_ct:with_connection(Config, fun(C2) ->
                {ok, _, [{Pid}]} = Module:equery(C2, "select pg_backend_pid()"),
                % emulate of disconnection
                {ok, _, [{true}]} = Module:equery(C1,
                "select pg_terminate_backend($1)", [Pid]),
                receive
                    {'EXIT', C2, {shutdown, #error{code = <<"57P01">>}}} ->
                        P ! ok;
                    Other ->
                        ct:pal("Unexpected msg: ~p~n", [Other]),
                        P ! error
                end
            end)
        end),
        receive ok -> ok end
    end).

active_connection_closed(Config) ->
    {Host, Port} = epgsql_ct:connection_data(Config),
    Module = ?config(module, Config),
    P = self(),
    F = fun() ->
          process_flag(trap_exit, true),
          {ok, C} = Module:connect(Host, [
              {database, "epgsql_test_db1"},
              {port, Port}
          ]),
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
    epgsql_ct:flush().

warning_notice(Config) ->
    Module = ?config(module, Config),
    epgsql_ct:with_connection(Config, fun(C) ->
        Q = "create function pg_temp.raise() returns void as $$
             begin
               raise warning 'oops';
             end;
             $$ language plpgsql;
             select pg_temp.raise()",
        [{ok, _, _}, _] = Module:squery(C, Q),
        receive
            {epgsql, C, {notice, #error{message = <<"oops">>, extra = Extra}}} ->
                ?assertMatch([{file, _},{line, _},{routine, _} | _], Extra),
                ok
        after
            100 -> erlang:error(didnt_receive_notice)
        end
    end, [{async, self()}]).

listen_notify(Config) ->
    Module = ?config(module, Config),
    epgsql_ct:with_connection(Config, fun(C) ->
        {ok, [], []}     = Module:squery(C, "listen epgsql_test"),
        {ok, _, [{Pid}]} = Module:equery(C, "select pg_backend_pid()"),
        {ok, [], []}     = Module:squery(C, "notify epgsql_test"),
        receive
            {epgsql, C, {notification, <<"epgsql_test">>, Pid, <<>>}} -> ok
        after
            100 -> erlang:error(didnt_receive_notification)
        end
    end, [{async, self()}]).

listen_notify_payload(Config) ->
    Module = ?config(module, Config),
    epgsql_ct:with_min_version(Config, [9, 0], fun(C) ->
        {ok, [], []}     = Module:squery(C, "listen epgsql_test"),
        {ok, _, [{Pid}]} = Module:equery(C, "select pg_backend_pid()"),
        {ok, [], []}     = Module:squery(C, "notify epgsql_test, 'test!'"),
        receive
            {epgsql, C, {notification, <<"epgsql_test">>, Pid, <<"test!">>}} -> ok
        after
            100 -> erlang:error(didnt_receive_notification)
        end
    end, [{async, self()}]).

set_notice_receiver(Config) ->
    Module = ?config(module, Config),
    epgsql_ct:with_min_version(Config, [9, 0], fun(C) ->
        {ok, [], []}     = Module:squery(C, "listen epgsql_test"),
        {ok, _, [{Pid}]} = Module:equery(C, "select pg_backend_pid()"),

        EnsureNoNotification = fun(Payload) ->
            {ok, [], []}     = Module:squery(C, ["notify epgsql_test, '", Payload, "'"]),
            receive
                {epgsql, _, _} -> erlang:error(got_unexpected_notification)
            after
                10 -> ok
            end
        end,
        EnsureNotification = fun(Payload) ->
            {ok, [], []}     = Module:squery(C, ["notify epgsql_test, '", Payload, "'"]),
            receive
                {epgsql, C, {notification, <<"epgsql_test">>, Pid, Payload}} -> ok
            after
                100 -> erlang:error(didnt_receive_notification)
            end
        end,
        Self = self(),

        EnsureNoNotification(<<"test1">>),

        % Set pid()
        {ok, undefined} = Module:set_notice_receiver(C, Self),
        EnsureNotification(<<"test2">>),

        %% test PL/PgSQL NOTICE
        {ok, [], []} = Module:squery(C, ["DO $$ BEGIN RAISE WARNING 'test notice'; END $$;"]),
        receive
            {epgsql, C, {notice, #error{severity = warning,
                         code = <<"01000">>,
                         message = <<"test notice">>,
                         extra = _}}} -> ok
        after
           100 -> erlang:error(didnt_receive_notice)
        end,

        % set registered pid
        Receiver = pg_notification_receiver,
        register(Receiver, Self),
        {ok, Self} = Module:set_notice_receiver(C, Receiver),
        EnsureNotification(<<"test3">>),

        % make registered name invalid
        unregister(Receiver),
        EnsureNoNotification(<<"test4">>),

        % disable
        {ok, Receiver} = Module:set_notice_receiver(C, undefined),
        EnsureNoNotification(<<"test5">>)
    end, []).

get_cmd_status(Config) ->
    Module = ?config(module, Config),
    epgsql_ct:with_connection(Config, fun(C) ->
        {ok, [], []} = Module:squery(C, "BEGIN"),
        ?assertEqual({ok, 'begin'}, Module:get_cmd_status(C)),
        {ok, [], []} = epgsql:equery(C, "CREATE TEMPORARY TABLE cmd_status_t(col INTEGER)"),
        ?assertEqual({ok, 'create'}, Module:get_cmd_status(C)),
        %% Some commands have number of affected rows in status
        {ok, N} = Module:squery(C, "INSERT INTO cmd_status_t (col) VALUES (1), (2)"),
        ?assertEqual({ok, {'insert', N}}, Module:get_cmd_status(C)),
        {ok, 1} = Module:squery(C, "UPDATE cmd_status_t SET col=3 WHERE col=1"),
        ?assertEqual({ok, {'update', 1}}, Module:get_cmd_status(C)),
        %% Failed queries have no status
        {error, _} = Module:squery(C, "DELETE FROM cmd_status_t WHERE not_col=2"),
        ?assertEqual({ok, undefined}, Module:get_cmd_status(C)),
        %% if COMMIT failed, status will be 'rollback'
        {ok, [], []} = Module:squery(C, "COMMIT"),
        ?assertEqual({ok, 'rollback'}, Module:get_cmd_status(C)),
        %% Only last command's status returned
        [_, _, _] = Module:squery(C, "BEGIN; SELECT 1; COMMIT"),
        ?assertEqual({ok, 'commit'}, Module:get_cmd_status(C))
    end).

get_backend_pid(Config) ->
    Module = ?config(module, Config),
    epgsql_ct:with_connection(Config, fun(C) ->
        {ok, [#column{}], [{PidBin}]} = Module:squery(C, "SELECT pg_backend_pid()"),
        Pid = Module:get_backend_pid(C),
        ?assertEqual(PidBin, integer_to_binary(Pid))
    end).

range_type(Config) ->
    epgsql_ct:with_min_version(Config, [9, 2], fun(_C) ->
        check_type(Config, int4range, "int4range(10, 20)", {10, 20}, [
            {1, 58}, {-1, 12}, {-985521, 5412687}, {minus_infinity, 0},
            {984655, plus_infinity}, {minus_infinity, plus_infinity}
        ]),
        check_type(Config, int4range, "int4range(10, 10)", empty, [])
   end, []).

range8_type(Config) ->
    epgsql_ct:with_min_version(Config, [9, 2], fun(_C) ->
        check_type(Config, int8range, "int8range(10, 20)", {10, 20}, [
            {1, 58}, {-1, 12}, {-9223372036854775808, 5412687},
            {minus_infinity, 9223372036854775807},
            {984655, plus_infinity}, {minus_infinity, plus_infinity}
        ]),
        check_type(Config, int8range, "int8range(10, 10)", empty, [])
    end, []).

date_time_range_type(Config) ->
    epgsql_ct:with_min_version(Config, [9, 2], fun(_C) ->
        check_type(Config, tsrange, "tsrange('2008-01-02 03:04:05', '2008-02-02 03:04:05')", {{{2008,1,2},{3,4,5.0}}, {{2008,2,2},{3,4,5.0}}}, []),
        check_type(Config, tsrange, "tsrange('2008-01-02 03:04:05', '2008-02-02 03:04:05', '[]')", {{{2008,1,2},{3,4,5.0}}, {{2008,2,2},{3,4,5.0}}}, []),
        check_type(Config, tsrange, "tsrange('2008-01-02 03:04:05', '2008-02-02 03:04:05', '()')", {{{2008,1,2},{3,4,5.0}}, {{2008,2,2},{3,4,5.0}}}, []),
        check_type(Config, tsrange, "tsrange('2008-01-02 03:04:05', '2008-02-02 03:04:05', '[)')", {{{2008,1,2},{3,4,5.0}}, {{2008,2,2},{3,4,5.0}}}, []),
        check_type(Config, tsrange, "tsrange('2008-01-02 03:04:05', '2008-02-02 03:04:05', '(]')", {{{2008,1,2},{3,4,5.0}}, {{2008,2,2},{3,4,5.0}}}, []),
        check_type(Config, tsrange, "tsrange('2008-01-02 03:04:05', '2008-01-02 03:04:05')", empty, []),
        check_type(Config, daterange, "daterange('2008-01-02', '2008-02-02')", {{2008,1,2}, {2008, 2, 2}}, [{{-4712,1,1}, {5874897,1,1}}]),
        check_type(Config, tstzrange, "tstzrange('2011-01-02 03:04:05+3', '2011-01-02 04:04:05+3')", {{{2011, 1, 2}, {0, 4, 5.0}}, {{2011, 1, 2}, {1, 4, 5.0}}}, [{{{2011, 1, 2}, {0, 4, 5.0}}, {{2011, 1, 2}, {1, 4, 5.0}}}]),
        check_type(Config, tstzrange, "tstzrange('2008-01-02 03:04:05', null)", {{{2008,1,2},{3,4,5.0}}, plus_infinity}, []),
        check_type(Config, tstzrange, "tstzrange('2008-01-02 03:04:05', null, '[]')", {{{2008,1,2},{3,4,5.0}}, plus_infinity}, []),
        check_type(Config, tstzrange, "tstzrange('2008-01-02 03:04:05', null, '()')", {{{2008,1,2},{3,4,5.0}}, plus_infinity}, []),
        check_type(Config, tstzrange, "tstzrange(null, '2008-01-02 03:04:05')", {minus_infinity, {{2008,1,2},{3,4,5.0}}}, []),
        check_type(Config, tstzrange, "tstzrange(null, '2008-01-02 03:04:05', '[]')", {minus_infinity, {{2008,1,2},{3,4,5.0}}}, []),
        check_type(Config, tstzrange, "tstzrange(null, '2008-01-02 03:04:05', '()')", {minus_infinity, {{2008,1,2},{3,4,5.0}}}, [])

   end, []).

with_transaction(Config) ->
    Module = ?config(module, Config),
    epgsql_ct:with_connection(
      Config,
      fun(C) ->
              %% Success case
              ?assertEqual(
                 success, Module:with_transaction(C, fun(_) -> success end)),
              ?assertEqual(
                 success, Module:with_transaction(C, fun(_) -> success end,
                                                  [{ensure_committed, true}])),
              %% begin_opts
              ?assertMatch(
                 [{ok, _, [{<<"serializable">>}]},
                  {ok, _, [{<<"on">>}]}],
                 Module:with_transaction(
                   C, fun(C1) ->
                              Module:squery(C1, ("SHOW transaction_isolation; "
                                                 "SHOW transaction_read_only"))
                      end,
                   [{begin_opts, "READ ONLY ISOLATION LEVEL SERIALIZABLE"}])),
              %% ensure_committed failure
              ?assertError(
                 {ensure_committed_failed, rollback},
                 Module:with_transaction(
                   C, fun(C1) ->
                              {error, _} = Module:squery(C1, "SELECT col FROM _nowhere_"),
                              ok
                      end,
                   [{ensure_committed, true}])),
              %% reraise
              ?assertEqual(
                 {rollback, my_err},
                 Module:with_transaction(
                   C, fun(_) -> error(my_err) end,
                   [{reraise, false}])),
              ?assertError(
                 my_err,
                 Module:with_transaction(
                   C, fun(_) -> error(my_err) end, []))
      end, []).

%% @doc Mixing all 3 API interfaces with same connection
mixed_api(Config) ->
    epgsql = ?config(module, Config),
    epgsql_ct:with_connection(
      Config,
      fun(C) ->
              {ok, Stmt} = epgsql:parse(
                             C, "SELECT id, $1::text AS val FROM generate_series(1, 5) AS t(id)"),
              ABindRef = epgsqla:bind(C, Stmt, "a_portal", [<<"epgsqla">>]),
              IBindRef = epgsqli:bind(C, Stmt, "i_portal", [<<"epgsqli">>]),
              AExecute1Ref = epgsqla:execute(C, Stmt, "a_portal", 3),
              IExecute1Ref = epgsqli:execute(C, Stmt, "i_portal", 3),
              ?assertEqual({partial, [{4, <<"epgsqla">>}]},
                           epgsql:execute(C, Stmt, "a_portal", 1)),
              ?assertEqual({partial, [{4, <<"epgsqli">>}]},
                           epgsql:execute(C, Stmt, "i_portal", 1)),
              %% by the time epgsql:execute returns, we should already have all the asynchronous
              %% responses in our message queue (epgsql:execute uses selective receive),
              %% but let's try to run some more finalizers.
              %% Note: we are calling epgsqla on i_portal and epgsqli on a_portal!
              AExecute2Ref = epgsqla:execute(C, Stmt, "i_portal", 0),
              IExecute2Ref = epgsqli:execute(C, Stmt, "a_portal", 0),
              ok = epgsql:close(C, Stmt),
              ?assertEqual(
                 [{C, ABindRef, ok},
                  {C, IBindRef, ok},
                  {C, AExecute1Ref, {partial, [{1, <<"epgsqla">>},
                                               {2, <<"epgsqla">>},
                                               {3, <<"epgsqla">>}
                                              ]}},
                  {C, IExecute1Ref, {data, {1, <<"epgsqli">>}}},
                  {C, IExecute1Ref, {data, {2, <<"epgsqli">>}}},
                  {C, IExecute1Ref, {data, {3, <<"epgsqli">>}}},
                  {C, IExecute1Ref, suspended},
                  {C, AExecute2Ref, {ok, [{5, <<"epgsqli">>}]}},
                  {C, IExecute2Ref, {data, {5, <<"epgsqla">>}}},
                  {C, IExecute2Ref, {complete, select}}],
                 receive_for_conn(C, 10, 1000))
      end).

pipelined_prepared_query(Config) ->
    epgsql_cast = ?config(module, Config),
    epgsql_ct:with_connection(
      Config,
      fun(C) ->
              {ok, #statement{types = Types} = Stmt} =
                  epgsql_cast:parse(C, "SELECT $1::integer as c1, 'hello' as c2"),
              Refs = [{epgsqla:prepared_query(C, Stmt, lists:zip(Types, [I])), I}
                      || I <- lists:seq(1, 10)],
              Timer = erlang:send_after(5000, self(), timeout),
              [receive
                   {C, Ref, {ok, Columns, Rows}} ->
                       ?assertMatch([#column{name = <<"c1">>, type = int4},
                                     #column{name = <<"c2">>, type = text}], Columns),
                       ?assertEqual([{I, <<"hello">>}], Rows);
                   Other ->
                       %% We expect responses in the same order as we send requests
                       error({unexpected_message, Other})
               end || {Ref, I} <- Refs],
              erlang:cancel_timer(Timer)
      end).

pipelined_parse_batch_execute(Config) ->
    epgsql_cast = ?config(module, Config),
    epgsql_ct:with_connection(
      Config,
      fun(C) ->
              ParseRefs =
                  [begin
                       Name = io_lib:format("stmt_~w", [I]),
                       {epgsqla:parse(C, Name,
                                      io_lib:format("SELECT $1 AS in, ~w00 AS out", [I]),
                                      [int4]),
                        I}
                   end || I <- lists:seq(1, 5)],
              Timer = erlang:send_after(5000, self(), timeout),
              Batch =
                  [receive
                       {C, Ref, {ok, #statement{columns = Cols} = Stmt}} ->
                           ?assertMatch([#column{name = <<"in">>, type = int4},
                                         #column{name = <<"out">>}],
                                        Cols),
                           {Stmt, [I]};
                       Other ->
                           error({unexpected_message, Other})
                   end || {Ref, I} <- ParseRefs],
              ?assertMatch([{ok, [{1, 100}]},
                            {ok, [{2, 200}]},
                            {ok, [{3, 300}]},
                            {ok, [{4, 400}]},
                            {ok, [{5, 500}]}],
                           epgsql:execute_batch(C, Batch)),
              CloseRefs = [epgsqla:close(C, Stmt) || {Stmt, _} <- Batch],
              [receive
                   {C, Ref, ok} ->
                       ok;
                   Other ->
                       error({unexpected_message, Other})
               end || Ref <- CloseRefs],
              erlang:cancel_timer(Timer)
      end).

incremental_sock_active_n(Config) ->
    epgsql_incremental = ?config(module, Config),
    Q = "SELECT *, 'Hello world' FROM generate_series(0, 10240)",
    epgsql_ct:with_connection(Config,
         fun(C) ->
             Ref = epgsqli:squery(C, Q),
             {done, NumPassive, Others, Rows} = recv_incremental_active_n(C, Ref),
             ?assertMatch([{columns, _}, {complete, _}], Others),
             ?assert(NumPassive > 0),
             ?assertMatch([{<<"0">>, <<"Hello world">>},
                           {<<"1">>, <<"Hello world">>} | _], Rows),
             ?assertEqual(10241, length(Rows))
         end,
         "epgsql_test",
         [{socket_active, 2}]).

-ifdef(OTP_RELEASE).
incremental_sock_active_n_ssl(Config) ->
    epgsql_incremental = ?config(module, Config),
    Q = "SELECT *, 'Hello world' FROM generate_series(0, 10240)",
    epgsql_ct:with_connection(Config,
         fun(C) ->
             Ref = epgsqli:squery(C, Q),
             {done, NumPassive, Others, Rows} = recv_incremental_active_n(C, Ref),
             ?assertMatch([{columns, _}, {complete, _}], Others),
             ?assert(NumPassive > 0),
             ?assertMatch([{<<"0">>, <<"Hello world">>},
                           {<<"1">>, <<"Hello world">>} | _], Rows),
             ?assertEqual(10241, length(Rows))
         end,
         "epgsql_test",
         [{ssl, true}, {socket_active, 2}]).
-else.
%% {active, N} for SSL is only supported on OTP-21+
incremental_sock_active_n_ssl(_Config) ->
    noop.
-endif.

recv_incremental_active_n(C, Ref) ->
    recv_incremental_active_n(C, Ref, 0, [], []).

recv_incremental_active_n(C, Ref, NumPassive, Rows, Others) ->
    receive
        {C, Ref, {data, Row}} ->
            recv_incremental_active_n(C, Ref, NumPassive, [Row | Rows], Others);
        {epgsql, C, socket_passive} ->
            ok = epgsql:activate(C),
            recv_incremental_active_n(C, Ref, NumPassive + 1, Rows, Others);
        {C, Ref, {error, _} = E} ->
            E;
        {C, Ref, done} ->
            {done, NumPassive, lists:reverse(Others), lists:reverse(Rows)};
        {C, Ref, Other} ->
            recv_incremental_active_n(C, Ref, NumPassive, Rows, [Other | Others]);
        Other ->
            recv_incremental_active_n(C, Ref, NumPassive, Rows, [Other | Others])
    after 5000 ->
            error({timeout, NumPassive, Others, Rows})
    end.

redacted_state(Config) ->
    case erlang:system_info(otp_release) of
      V = [_, _] when V > "20" ->
        redacted_state_(Config);
      V ->
        {skip, {"Logger facility is available starting OTP 21, running on OTP " ++ V}}
    end.

redacted_state_(Config) ->
    _Handle = ct:timetrap({seconds, 3}),
    try
      logger:add_handler(?MODULE, ?MODULE, #{relay_to => self()}),
      C = epgsql_ct:connect(Config),
      true = unlink(C),
      Reason = {please, ?MODULE, ?FUNCTION_NAME},
      ok = proc_lib:stop(C, Reason, 1000),
      receive
        {log, Message} ->
            ?assertMatch({report, #{label := {gen_server, terminate},
                                    reason := Reason,
                                    state := _}},
                         Message),
            {report, #{state := State}} = Message,
            ?assertMatch(#{rows := information_redacted},
                         epgsql_sock:state_to_map(State))
      end
    after
      logger:remove_handler(?MODULE)
    end.

%% =============================================================================
%% Logger handler
%% ============================================================================

log(#{msg := Msg}, #{relay_to := Pid}) ->
    Pid ! {log, Msg};
log(_, _) ->
    ok.

%% =============================================================================
%% Internal functions
%% ============================================================================

receive_for_conn(_, 0, _) -> [];
receive_for_conn(C, N, Timeout) ->
    receive
        {C, _, _} = Msg ->
            [Msg | receive_for_conn(C, N - 1, Timeout)];
        Other ->
            error({unexpected_msg, Other})
    after Timeout ->
            error({timeout, {remaining_msgs, N}})
    end.

get_type_col(Type) ->
    "c_" ++ atom_to_list(Type).

check_type(Config, Type, In, Out, Values) ->
    Column = get_type_col(Type),
    check_type(Config, Type, In, Out, Values, Column).

check_type(Config, Type, In, Out, Values, Column) ->
    epgsql_ct:with_connection(Config, fun(C) ->
        check_type(Config, Type, In, Out, Values, Column, C)
    end).

check_type(Config, Type, In, Out, Values, Column, C) ->
    Module = ?config(module, Config),
    Select = io_lib:format("select ~s::~w", [In, Type]),
    Res = Module:equery(C, Select),
    ?assertMatch({ok, [#column{type = Type}], [{Out}]}, Res),
    Sql = io_lib:format("insert into test_table2 (~s) values ($1) returning ~s", [Column, Column]),
    {ok, #statement{columns = [#column{type = Type}]} = S} = Module:parse(C, Sql),
    Insert = fun(V) ->
        ok = Module:bind(C, S, [V]),
        {ok, 1, [{V2}]} = Module:execute(C, S),
        case compare(Type, V, V2) of
            true  -> ok;
            false -> error({write_read_compare_failed, io_lib:format("~p =/= ~p", [V, V2])})
        end,
        ok = Module:sync(C)
    end,
    lists:foreach(Insert, [null, undefined | Values]).

compare(_Type, null, null)      -> true;
compare(_Type, undefined, null) -> true;
compare(float4, V1, V2) when is_float(V1) -> abs(V2 - V1) < 0.000001;
compare(float8, V1, V2) when is_float(V1) -> abs(V2 - V1) < 0.000000000000001;
compare(hstore, {V1}, V2)       -> compare(hstore, V1, V2);
compare(hstore, V1, {V2})       -> compare(hstore, V1, V2);
compare(hstore, V1, V2)         ->
    orddict:from_list(format_hstore(V1)) =:= orddict:from_list(format_hstore(V2));
compare(Type, V1 = {_, _, MS}, {D2, {H2, M2, S2}}) when Type == timestamp;
                                                        Type == timestamptz ->
    {D1, {H1, M1, S1}} = calendar:now_to_universal_time(V1),
    ({D1, H1, M1} =:= {D2, H2, M2}) and (abs(S1 + MS/1000000 - S2) < 0.000000000000001);
compare(_Type, V1, V2)     -> V1 =:= V2.

format_hstore({Hstore}) -> Hstore;
format_hstore(Hstore) ->
    [{format_hstore_key(Key), format_hstore_value(Value)} || {Key, Value} <- Hstore].

format_hstore_key(Key) -> format_hstore_string(Key).

format_hstore_value(null) -> null;
format_hstore_value(undefined) -> null;
format_hstore_value(Value) -> format_hstore_string(Value).

format_hstore_string(Num) when is_number(Num) -> iolist_to_binary(io_lib:format("~w", [Num]));
format_hstore_string(Str) -> iolist_to_binary(io_lib:format("~s", [Str])).
