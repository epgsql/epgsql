%%% Copyright (C) 2008 - Will Glozer.  All rights reserved.
%%% Copyright (C) 2011 - Anton Lebedevich.  All rights reserved.

-module(pgsql).

-export([connect/2, connect/3, connect/4, connect/5,
         close/1,
         get_parameter/2,
         squery/2,
         equery/2, equery/3,
         parse/2, parse/3, parse/4,
         describe/2, describe/3,
         bind/3, bind/4,
         execute/2, execute/3, execute/4,
         execute_batch/2,
         close/2, close/3,
         sync/1,
         cancel/1,
         with_transaction/2,
         sync_on_error/2]).

-include("pgsql.hrl").

%% -- client interface --

connect(Host, Opts) ->
    connect(Host, os:getenv("USER"), "", Opts).

connect(Host, Username, Opts) ->
    connect(Host, Username, "", Opts).

connect(Host, Username, Password, Opts) ->
    {ok, C} = pgsql_sock:start_link(),
    connect(C, Host, Username, Password, Opts).

connect(C, Host, Username, Password, Opts) ->
    %% TODO connect timeout
    case gen_server:call(C,
                         {connect, Host, Username, Password, Opts},
                         infinity) of
        connected ->
            {ok, C};
        Error = {error, _} ->
            Error
    end.

close(C) ->
    pgsql_sock:close(C).

get_parameter(C, Name) ->
    pgsql_sock:get_parameter(C, Name).

squery(C, Sql) ->
    gen_server:call(C, {squery, Sql}, infinity).

equery(C, Sql) ->
    equery(C, Sql, []).

%% TODO add fast_equery command that doesn't need parsed statement
equery(C, Sql, Parameters) ->
    case parse(C, Sql) of
        {ok, #statement{types = Types} = S} ->
            Typed_Parameters = lists:zip(Types, Parameters),
            gen_server:call(C, {equery, S, Typed_Parameters}, infinity);
        Error ->
            Error
    end.

%% parse

parse(C, Sql) ->
    parse(C, "", Sql, []).

parse(C, Sql, Types) ->
    parse(C, "", Sql, Types).

parse(C, Name, Sql, Types) ->
    sync_on_error(C, gen_server:call(C, {parse, Name, Sql, Types}, infinity)).

%% bind

bind(C, Statement, Parameters) ->
    bind(C, Statement, "", Parameters).

bind(C, Statement, PortalName, Parameters) ->
    sync_on_error(
      C,
      gen_server:call(C, {bind, Statement, PortalName, Parameters}, infinity)).

%% execute

execute(C, S) ->
    execute(C, S, "", 0).

execute(C, S, N) ->
    execute(C, S, "", N).

execute(C, S, PortalName, N) ->
    gen_server:call(C, {execute, S, PortalName, N}, infinity).

execute_batch(C, Batch) ->
    gen_server:call(C, {execute_batch, Batch}, infinity).

%% statement/portal functions

describe(C, #statement{name = Name}) ->
    describe(C, statement, Name).

describe(C, statement, Name) ->
    sync_on_error(C, gen_server:call(C, {describe_statement, Name}, infinity));

%% TODO unknown result format of Describe portal
describe(C, portal, Name) ->
    sync_on_error(C, gen_server:call(C, {describe_portal, Name}, infinity)).

close(C, #statement{name = Name}) ->
    close(C, statement, Name).

close(C, Type, Name) ->
    gen_server:call(C, {close, Type, Name}).

sync(C) ->
    gen_server:call(C, sync).

cancel(C) ->
    pgsql_sock:cancel(C).

%% misc helper functions
with_transaction(C, F) ->
    try {ok, [], []} = squery(C, "BEGIN"),
        R = F(C),
        {ok, [], []} = squery(C, "COMMIT"),
        R
    catch
        _:Why ->
            squery(C, "ROLLBACK"),
            %% TODO hides error stacktrace
            {rollback, Why}
    end.

sync_on_error(C, Error = {error, _}) ->
    ok = sync(C),
    Error;

sync_on_error(_C, R) ->
    R.

