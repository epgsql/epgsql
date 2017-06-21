-module(epgsql_perf_SUITE).
-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

-export([
    init_per_suite/1,
    all/0,
    end_per_suite/1,

    prepare_data/0,
    prepare_data/1,
    get_data/0,
    get_data/1,
    drop_data/1
]).

init_per_suite(Config) ->
    Config.

end_per_suite(_Config) ->
    ok.

all() ->
    [
     prepare_data,
     get_data,
     drop_data
    ].

%% =============================================================================
%% Test cases
%% =============================================================================

-define(noise_size, 10000000).

prepare_data() -> [{timetrap, {seconds, 60}}].
prepare_data(Config) ->
    with_connection(Config, fun (C) ->
        Noise = noise(?noise_size),
        {ok, [], []} = epgsql:squery(C, "create table test_big_blobs (id int4 primary key, noise bytea)"),
        {ok, 1} = epgsql:equery(C, "insert into test_big_blobs (id, noise) values (1, $1)", [Noise])
    end).

get_data() -> [{timetrap, {seconds, 60}}].
get_data(Config) ->
    with_connection(Config, fun (C) ->
        {ok, _, [{Noise}]} = epgsql:equery(C, "select noise from test_big_blobs"),
        ?assertEqual(?noise_size, byte_size(Noise))
    end).

drop_data(Config) ->
    with_connection(Config, fun (C) ->
        {ok, [], []} = epgsql:squery(C, "drop table test_big_blobs")
    end).

%% =============================================================================
%% Internal functions
%% =============================================================================

noise(N) ->
    crypto:strong_rand_bytes(N).

with_connection(Config, F) ->
    with_connection(Config, F, "epgsql_test", []).

with_connection(Config, F, Username, Args) ->
    {Host, Port} = epgsql_ct:connection_data(Config),

    Args2 = [{port, Port}, {database, "epgsql_test_db1"} | Args],
    fun () ->
        {ok, C} = epgsql:connect(Host, Username, Args2),
        try
            F(C)
        after
            epgsql:close(C)
        end
    end.
