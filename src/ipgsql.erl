%%% Copyright (C) 2011 - Anton Lebedevich.  All rights reserved.

-module(ipgsql).

-export([start_link/0,
         connect/5,
         close/1,
         get_parameter/2,
         squery/2,
         equery/3,
         parse/4,
         bind/4,
         execute/4,
         execute_batch/2,
         describe/3,
         close/3,
         sync/1,
         cancel/1]).

%% -- client interface --

start_link() ->
    pgsql_sock:start_link().

connect(C, Host, Username, Password, Opts) ->
    incremental(C, {connect, Host, Username, Password, Opts}).

close(C) ->
    pgsql_sock:close(C).

get_parameter(C, Name) ->
    pgsql_sock:get_parameter(C, Name).

squery(C, Sql) ->
    incremental(C, {squery, Sql}).

equery(C, Statement, Parameters) ->
    incremental(C, {equery, Statement, Parameters}).

parse(C, Name, Sql, Types) ->
    incremental(C, {parse, Name, Sql, Types}).

bind(C, Statement, PortalName, Parameters) ->
    incremental(C, {bind, Statement, PortalName, Parameters}).

execute(C, Statement, PortalName, MaxRows) ->
    incremental(C, {execute, Statement, PortalName, MaxRows}).

execute_batch(C, Batch) ->
    incremental(C, {execute_batch, Batch}).

describe(C, statement, Name) ->
    incremental(C, {describe_statement, Name});

describe(C, portal, Name) ->
    incremental(C, {describe_portal, Name}).

close(C, Type, Name) ->
    incremental(C, {close, Type, Name}).

sync(C) ->
    incremental(C, sync).

cancel(C) ->
    pgsql_sock:cancel(C).


%% -- internal functions --

incremental(C, Command) ->
    Ref = make_ref(),
    gen_server:cast(C, {{incremental, self(), Ref}, Command}),
    Ref.
