-module(epgsql_replication_SUITE).
-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").
-include("epgsql.hrl").

-export([
    init_per_suite/1,
    all/0,
    end_per_suite/1,

    connect_in_repl_mode/1,
    create_drop_replication_slot/1,
    replication_sync/1,
    replication_async/1,

    %% Callbacks
    handle_x_log_data/4
]).

init_per_suite(Config) ->
    [{module, epgsql}|Config].

end_per_suite(_Config) ->
    ok.

all() ->
    [
     connect_in_repl_mode,
     create_drop_replication_slot,
     replication_async,
     replication_sync
    ].

connect_in_repl_mode(Config) ->
    epgsql_ct:connect_only(Config, ["epgsql_test_replication",
        "epgsql_test_replication",
        [{database, "epgsql_test_db1"}, {replication, "database"}]]).

create_drop_replication_slot(Config) ->
    Module = ?config(module, Config),
    epgsql_ct:with_connection(
        Config,
        fun(C) ->
            {ok, Cols, Rows} = Module:squery(C, "CREATE_REPLICATION_SLOT ""epgsql_test"" LOGICAL ""test_decoding"""),
            [#column{name = <<"slot_name">>}, #column{name = <<"consistent_point">>},
                #column{name = <<"snapshot_name">>}, #column{name = <<"output_plugin">>}] = Cols,
            [{<<"epgsql_test">>, _, _, <<"test_decoding">>}] = Rows,
            [{ok, _, _}, {ok, _, _}] = Module:squery(C, "DROP_REPLICATION_SLOT ""epgsql_test""")
        end,
        "epgsql_test_replication",
        [{replication, "database"}]).

replication_async(Config) ->
    replication_test_run(Config, self()).

replication_sync(Config) ->
    replication_test_run(Config, ?MODULE).

replication_test_run(Config, Callback) ->
    Module = ?config(module, Config),
    epgsql_ct:with_connection(
        Config,
        fun(C) ->
            {ok, _, _} = Module:squery(C, "CREATE_REPLICATION_SLOT ""epgsql_test"" LOGICAL ""test_decoding"""),

            %% new connection because main id in a replication mode
            epgsql_ct:with_connection(
                Config,
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
    epgsql_ct:with_connection(
        Config,
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
