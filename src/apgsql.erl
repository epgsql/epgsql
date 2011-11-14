%%% Copyright (C) 2011 - Anton Lebedevich.  All rights reserved.

-module(apgsql).

-export([start_link/0,
         connect/5,
         close/1,
         get_parameter/2,
         squery/2,
         equery/3,
         parse/4,
         bind/4,
         execute/4,
         describe/3,
         close/3,
         sync/1,
         cancel/1]).

%% -- client interface --

start_link() ->
    pgsq_sock:start_link().

connect(C, Host, Username, Password, Opts) ->
    cast(C, {connect, Host, Username, Password, Opts}).

close(C) ->
    pgsql_sock:close(C).

get_parameter(C, Name) ->
    pgsql_sock:get_parameter(C, Name).

squery(C, Sql) ->
    cast(C, {squery, Sql}).

equery(C, Statement, Parameters) ->
    cast(C, {equery, Statement, Parameters}).

parse(C, Name, Sql, Types) ->
    cast(C, {parse, Name, Sql, Types}).

bind(C, Statement, PortalName, Parameters) ->
    cast(C, {bind, Statement, PortalName, Parameters}).

execute(C, Statement, PortalName, MaxRows) ->
    cast(C, {execute, Statement, PortalName, MaxRows}).

describe(C, statement, Name) ->
    cast(C, {describe_statement, Name});

describe(C, portal, Name) ->
    cast(C, {describe_portal, Name}).

close(C, Type, Name) ->
    cast(C, {close, Type, Name}).

sync(C) ->
    cast(C, sync).

cancel(C) ->
    pgsql_sock:cancel(C).


%% -- internal functions --

cast(C, Command) ->
    Ref = make_ref(),
    gen_server:cast(C, {{self(), Ref}, Command}),
    Ref.
