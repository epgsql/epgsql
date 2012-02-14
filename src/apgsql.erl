%%% Copyright (C) 2011 - Anton Lebedevich.  All rights reserved.

-module(apgsql).

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
         cancel/1]).

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
    cast(C, {connect, Host, Username, Password, Opts}).

close(C) ->
    pgsql_sock:close(C).

get_parameter(C, Name) ->
    pgsql_sock:get_parameter(C, Name).

squery(C, Sql) ->
    cast(C, {squery, Sql}).

equery(C, Sql) ->
    equery(C, Sql, []).

equery(C, Statement, Parameters) ->
    cast(C, {equery, Statement, Parameters}).

parse(C, Sql) ->
    parse(C, "", Sql, []).

parse(C, Sql, Types) ->
    parse(C, "", Sql, Types).

parse(C, Name, Sql, Types) ->
    cast(C, {parse, Name, Sql, Types}).

bind(C, Statement, Parameters) ->
    bind(C, Statement, "", Parameters).

bind(C, Statement, PortalName, Parameters) ->
    cast(C, {bind, Statement, PortalName, Parameters}).

execute(C, S) ->
    execute(C, S, "", 0).

execute(C, S, N) ->
    execute(C, S, "", N).

execute(C, Statement, PortalName, MaxRows) ->
    cast(C, {execute, Statement, PortalName, MaxRows}).

execute_batch(C, Batch) ->
    cast(C, {execute_batch, Batch}).

describe(C, #statement{name = Name}) ->
    describe(C, statement, Name).

describe(C, statement, Name) ->
    cast(C, {describe_statement, Name});

describe(C, portal, Name) ->
    cast(C, {describe_portal, Name}).

close(C, #statement{name = Name}) ->
    close(C, statement, Name).

close(C, Type, Name) ->
    cast(C, {close, Type, Name}).

sync(C) ->
    cast(C, sync).

cancel(C) ->
    pgsql_sock:cancel(C).

%% -- internal functions --

cast(C, Command) ->
    Ref = make_ref(),
    gen_server:cast(C, {{cast, self(), Ref}, Command}),
    Ref.
