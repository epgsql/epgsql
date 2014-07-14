%%% Copyright (C) 2011 - Anton Lebedevich.  All rights reserved.

-module(epgsqli).

-export([start_link/0,
         connect/2, connect/3, connect/4, connect/5,
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

-include("epgsql.hrl").

%% -- client interface --

start_link() ->
    epgsql_sock:start_link().

connect(Host, Opts) ->
    connect(Host, os:getenv("USER"), "", Opts).

connect(Host, Username, Opts) ->
    connect(Host, Username, "", Opts).

connect(Host, Username, Password, Opts) ->
    {ok, C} = epgsql_sock:start_link(),
    connect(C, Host, Username, Password, Opts).

-spec connect(epgsql:connection(), inet:ip_address() | inet:hostname(),
              string(), string(), [epgsql:connect_option()]) -> reference().
connect(C, Host, Username, Password, Opts) ->
    incremental(C, {connect, Host, Username, Password, Opts}).

-spec close(epgsql:connection()) -> ok.
close(C) ->
    epgsql_sock:close(C).

-spec get_parameter(epgsql:connection(), binary()) -> binary() | undefined.
get_parameter(C, Name) ->
    epgsql_sock:get_parameter(C, Name).

-spec squery(epgsql:connection(), string()) -> reference().
squery(C, Sql) ->
    incremental(C, {squery, Sql}).

equery(C, Sql) ->
    equery(C, Sql, []).

-spec equery(epgsql:connection(), #statement{}, [epgsql:bind_param()]) -> reference().
equery(C, Statement, Parameters) ->
    incremental(C, {equery, Statement, Parameters}).

parse(C, Sql) ->
    parse(C, "", Sql, []).

parse(C, Sql, Types) ->
    parse(C, "", Sql, Types).

-spec parse(epgsql:connection(), iolist(), string(), [epgsql_type()]) -> reference().
parse(C, Name, Sql, Types) ->
    incremental(C, {parse, Name, Sql, Types}).

bind(C, Statement, Parameters) ->
    bind(C, Statement, "", Parameters).

-spec bind(epgsql:connection(), #statement{}, string(), [epgsql:bind_param()]) -> reference().
bind(C, Statement, PortalName, Parameters) ->
    incremental(C, {bind, Statement, PortalName, Parameters}).

execute(C, S) ->
    execute(C, S, "", 0).

execute(C, S, N) ->
    execute(C, S, "", N).

-spec execute(epgsql:connection(), #statement{}, string(), non_neg_integer()) -> reference().
execute(C, Statement, PortalName, MaxRows) ->
    incremental(C, {execute, Statement, PortalName, MaxRows}).

-spec execute_batch(epgsql:connection(), [{#statement{}, [epgsql:bind_param()]}]) -> reference().
execute_batch(C, Batch) ->
    incremental(C, {execute_batch, Batch}).

describe(C, #statement{name = Name}) ->
    describe(C, statement, Name).

describe(C, statement, Name) ->
    incremental(C, {describe_statement, Name});

describe(C, portal, Name) ->
    incremental(C, {describe_portal, Name}).

close(C, #statement{name = Name}) ->
    close(C, statement, Name).

close(C, Type, Name) ->
    incremental(C, {close, Type, Name}).

sync(C) ->
    incremental(C, sync).

-spec cancel(epgsql:connection()) -> ok.
cancel(C) ->
    epgsql_sock:cancel(C).


%% -- internal functions --

incremental(C, Command) ->
    Ref = make_ref(),
    gen_server:cast(C, {{incremental, self(), Ref}, Command}),
    Ref.
