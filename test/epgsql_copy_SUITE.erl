-module(epgsql_copy_SUITE).
-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").
-include("epgsql.hrl").

-export([
    init_per_suite/1,
    all/0,
    end_per_suite/1,

    from_stdin_text/1,
    from_stdin_csv/1,
    from_stdin_binary/1,
    from_stdin_io_apis/1,
    from_stdin_with_terminator/1,
    from_stdin_corrupt_data/1
]).

init_per_suite(Config) ->
    [{module, epgsql}|Config].

end_per_suite(_Config) ->
    ok.

all() ->
    [
     from_stdin_text,
     from_stdin_csv,
     from_stdin_binary,
     from_stdin_io_apis,
     from_stdin_with_terminator,
     from_stdin_corrupt_data
    ].

%% @doc Test that COPY in text format works
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
                   ok,
                   io:put_chars(C, "14\tli")),
                ?assertEqual(
                   ok,
                   io:put_chars(C, "ne 14\n")),
                ?assertEqual(
                   {ok, 5},
                   Module:copy_done(C)),
                ?assertMatch(
                   {ok, _, [{10, <<"hello world">>},
                            {11, null},
                            {12, <<"line 12">>},
                            {13, <<"line 13">>},
                            {14, <<"line 14">>}]},
                   Module:equery(C,
                                 "SELECT id, value FROM test_table1"
                                 " WHERE id IN (10, 11, 12, 13, 14) ORDER BY id"))
        end).

%% @doc Test that COPY in CSV format works
from_stdin_csv(Config) ->
    Module = ?config(module, Config),
    epgsql_ct:with_connection(
        Config,
        fun(C) ->
                ?assertEqual(
                   {ok, [text, text]},
                   Module:copy_from_stdin(
                     C, "COPY test_table1 (id, value) FROM STDIN WITH (FORMAT csv, QUOTE '''')")),
                ?assertEqual(
                   ok,
                   io:put_chars(C,
                                "20,'hello world'\n"
                                "21,\n"
                                "22,line 22\n")),
                ?assertEqual(
                   ok,
                   io:put_chars(C, "23,'line 23'\n")),
                ?assertEqual(
                   ok,
                   io:put_chars(C, "24,'li")),
                ?assertEqual(
                   ok,
                   io:put_chars(C, "ne 24'\n")),
                ?assertEqual(
                   {ok, 5},
                   Module:copy_done(C)),
                ?assertMatch(
                   {ok, _, [{20, <<"hello world">>},
                            {21, null},
                            {22, <<"line 22">>},
                            {23, <<"line 23">>},
                            {24, <<"line 24">>}]},
                   Module:equery(C,
                                 "SELECT id, value FROM test_table1"
                                 " WHERE id IN (20, 21, 22, 23, 24) ORDER BY id"))
        end).

%% @doc Test that COPY in binary format works
from_stdin_binary(Config) ->
    Module = ?config(module, Config),
    epgsql_ct:with_connection(
        Config,
        fun(C) ->
                ?assertEqual(
                   {ok, [binary, binary]},
                   Module:copy_from_stdin(
                     C, "COPY test_table1 (id, value) FROM STDIN WITH (FORMAT binary)",
                     {binary, [int4, text]})),
                %% Batch of rows
                ?assertEqual(
                   ok,
                   Module:copy_send_rows(
                     C,
                     [{60, <<"hello world">>},
                      {61, null},
                      {62, "line 62"}],
                     5000)),
                %% Single row
                ?assertEqual(
                   ok,
                   Module:copy_send_rows(
                     C,
                     [{63, <<"line 63">>}],
                     1000)),
                %% Rows as lists
                ?assertEqual(
                   ok,
                   Module:copy_send_rows(
                     C,
                     [
                      [64, <<"line 64">>],
                      [65, <<"line 65">>]
                     ],
                     infinity)),
                ?assertEqual({ok, 6}, Module:copy_done(C)),
                ?assertMatch(
                   {ok, _, [{60, <<"hello world">>},
                            {61, null},
                            {62, <<"line 62">>},
                            {63, <<"line 63">>},
                            {64, <<"line 64">>},
                            {65, <<"line 65">>}]},
                   Module:equery(C,
                                 "SELECT id, value FROM test_table1"
                                 " WHERE id IN (60, 61, 62, 63, 64, 65) ORDER BY id"))
        end).

%% @doc Tests that different IO-protocol APIs work
from_stdin_io_apis(Config) ->
    Module = ?config(module, Config),
    epgsql_ct:with_connection(
        Config,
        fun(C) ->
                ?assertEqual(
                   {ok, [text, text]},
                   Module:copy_from_stdin(
                     C, "COPY test_table1 (id, value) FROM STDIN WITH (FORMAT text)")),
                ?assertEqual(ok, io:format(C, "30\thello world\n", [])),
                ?assertEqual(ok, io:format(C, "~b\t~s\n", [31, "line 31"])),
                %% Output "32\thello\n" in multiple calls
                ?assertEqual(ok, io:write(C, 32)),
                ?assertEqual(ok, io:put_chars(C, "\t")),
                ?assertEqual(ok, io:write(C, hello)),
                ?assertEqual(ok, io:nl(C)),
                %% Using `file` API
                ?assertEqual(ok, file:write(C, "33\tline 33\n34\tline 34\n")),
                %% Binary
                ?assertEqual(ok, io:put_chars(C, <<"35\tline 35\n">>)),
                ?assertEqual(ok, file:write(C, <<"36\tline 36\n">>)),
                %% IoData
                ?assertEqual(ok, io:put_chars(C, [<<"37">>, $\t, <<"line 37">>, <<$\n>>])),
                ?assertEqual(ok, file:write(C, [["38", <<$\t>>], [<<"line 38">>, $\n]])),
                %% Raw IO-protocol message-passing
                Ref = erlang:make_ref(),
                C ! {io_request, self(), Ref, {put_chars, unicode, "39\tline 39\n"}},
                ?assertEqual(ok, receive {io_reply, Ref, Resp} -> Resp
                                 after 5000 ->
                                         timeout
                                 end),
                %% Not documented!
                ?assertEqual(ok, io:requests(
                                   C,
                                   [{put_chars, unicode, "40\tline 40\n"},
                                    {put_chars, latin1, "41\tline 41\n"},
                                    {format, "~w\t~s", [42, "line 42"]},
                                    nl])),
                ?assertEqual(
                   {ok, 13},
                   Module:copy_done(C)),
                ?assertMatch(
                   {ok, _, [{30, <<"hello world">>},
                            {31, <<"line 31">>},
                            {32, <<"hello">>},
                            {33, <<"line 33">>},
                            {34, <<"line 34">>},
                            {35, <<"line 35">>},
                            {36, <<"line 36">>},
                            {37, <<"line 37">>},
                            {38, <<"line 38">>},
                            {39, <<"line 39">>},
                            {40, <<"line 40">>},
                            {41, <<"line 41">>},
                            {42, <<"line 42">>}
                            ]},
                   Module:equery(
                     C,
                     "SELECT id, value FROM test_table1"
                     " WHERE id IN (30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40, 41, 42)"
                     " ORDER BY id"))
        end).

%% @doc Tests that "end-of-data" terminator is successfully ignored
from_stdin_with_terminator(Config) ->
    Module = ?config(module, Config),
    epgsql_ct:with_connection(
        Config,
        fun(C) ->
                %% TEXT
                ?assertEqual(
                   {ok, [text, text]},
                   Module:copy_from_stdin(
                     C, "COPY test_table1 (id, value) FROM STDIN WITH (FORMAT text)")),
                ?assertEqual(ok, io:put_chars(
                                   C,
                                   "50\tline 50\n"
                                   "51\tline 51\n"
                                   "\\.\n")),
                ?assertEqual({ok, 2}, Module:copy_done(C)),
                %% CSV
                ?assertEqual(
                   {ok, [text, text]},
                   Module:copy_from_stdin(
                     C, "COPY test_table1 (id, value) FROM STDIN WITH (FORMAT csv)")),
                ?assertEqual(ok, io:put_chars(
                                   C,
                                   "52,line 52\n"
                                   "53,line 53\n"
                                   "\\.\n")),
                ?assertEqual({ok, 2}, Module:copy_done(C)),
                ?assertMatch(
                   {ok, _, [{50, <<"line 50">>},
                            {51, <<"line 51">>},
                            {52, <<"line 52">>},
                            {53, <<"line 53">>}
                            ]},
                   Module:equery(C,
                                 "SELECT id, value FROM test_table1"
                                 " WHERE id IN (50, 51, 52, 53) ORDER BY id"))
        end).

from_stdin_corrupt_data(Config) ->
    Module = ?config(module, Config),
    epgsql_ct:with_connection(
        Config,
        fun(C) ->
                ?assertEqual(
                   {ok, [text, text]},
                   Module:copy_from_stdin(
                     C, "COPY test_table1 (id, value) FROM STDIN WITH (FORMAT text)")),
                %% Wrong number of arguments to io:format
                Fmt = "~w\t~s\n",
                ?assertMatch({error, {fun_exception, {error, badarg, _Stack}}},
                             io:request(C, {format, Fmt, []})),
                ?assertError(badarg, io:format(C, Fmt, [])),
                %% Wrong return value from IO function
                ?assertEqual({error, {fun_return_not_characters, node()}},
                             io:request(C, {put_chars, unicode, erlang, node, []})),
                ?assertEqual({ok, 0}, Module:copy_done(C)),
                %%
                %% Corrupt text format
                ?assertEqual(
                   {ok, [text, text]},
                   Module:copy_from_stdin(
                     C, "COPY test_table1 (id, value) FROM STDIN WITH (FORMAT text)")),
                ?assertEqual(ok, io:put_chars(
                                   C,
                                   "42\n43\nwasd\n")),
                ?assertMatch(
                   #error{codename = bad_copy_file_format,
                          severity = error},
                   receive
                       {epgsql, C, {error, Err}} ->
                           Err
                   after 5000 ->
                           timeout
                   end),
                ?assertEqual({error, not_in_copy_mode},
                             io:request(C, {put_chars, unicode, "queque\n"})),
                ?assertError(badarg, io:format(C, "~w\n~s\n", [60, "wasd"])),
                %%
                %% Corrupt CSV format
                ?assertEqual(
                   {ok, [text, text]},
                   Module:copy_from_stdin(
                     C, "COPY test_table1 (id, value) FROM STDIN WITH (FORMAT csv)")),
                ?assertEqual(ok, io:put_chars(
                                   C,
                                   "42\n43\nwasd\n")),
                ?assertMatch(
                   #error{codename = bad_copy_file_format,
                          severity = error},
                   receive
                       {epgsql, C, {error, Err}} ->
                           Err
                   after 5000 ->
                           timeout
                   end),
                %%
                %% Corrupt binary format
                ?assertEqual(
                   {ok, [binary, binary]},
                   Module:copy_from_stdin(
                     C, "COPY test_table1 (id, value) FROM STDIN WITH (FORMAT binary)",
                     {binary, [int4, text]})),
                ?assertEqual(
                   ok,
                   Module:copy_send_rows(C, [{44, <<"line 44">>}], 1000)),
                ?assertEqual(ok, io:put_chars(C, "45\tThis is not ok!\n")),
                ?assertMatch(
                   #error{codename = bad_copy_file_format,
                          severity = error},
                   receive
                       {epgsql, C, {error, Err}} ->
                           Err
                   after 5000 ->
                           timeout
                   end),
                %% Connection is still usable
                ?assertMatch(
                   {ok, _, [{1}]},
                   Module:equery(C, "SELECT 1", []))
        end).
