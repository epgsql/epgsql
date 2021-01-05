-module(epgsql_copy_SUITE).
-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").
-include("epgsql.hrl").

-export([
    init_per_suite/1,
    all/0,
    end_per_suite/1,

    from_stdin_text/1
]).

init_per_suite(Config) ->
    [{module, epgsql}|Config].

end_per_suite(_Config) ->
    ok.

all() ->
    [
     from_stdin_text%% ,
     %% from_stdin_csv,
     %% from_stdin_io_apis,
     %% from_stdin_fragmented,
     %% from_stdin_with_terminator,
     %% from_stdin_corrupt_data
    ].

from_stdin_text(Config) ->
    Module = ?config(module, Config),
    epgsql_ct:with_connection(
        Config,
        fun(C) ->
                ?assertEqual(
                   {ok, [text, text]},
                   Module:copy_from_stdin(
                     C, "COPY test_table1 (id, value) FROM STDIN WITH (FORMAT text)")),
                ?assertEqual(
                   ok,
                   io:put_chars(C,
                                "10\thello world\n"
                                "11\t\\N\n"
                                "12\tline 12\n")),
                ?assertEqual(
                   ok,
                   io:put_chars(C, "13\tline 13\n")),
                ?assertEqual(
                   {ok, 4},
                   Module:copy_done(C)),
                ?assertMatch(
                   {ok, _, [{10, <<"hello world">>},
                            {11, null},
                            {12, <<"line 12">>},
                            {13, <<"line 13">>}]},
                   Module:equery(C,
                                 "SELECT id, value FROM test_table1"
                                 " WHERE id IN (10, 11, 12, 13) ORDER BY id"))
        end).
