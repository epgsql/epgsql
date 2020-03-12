%%% @doc Incremental interface
%%%
%%% All the functions return `reference()' immediately. Each data row as well
%%% as metadata are delivered as separate messages in a form of
%%% `{connection(), reference(), Payload}' where `Payload' depends on command
%%% being executed.
%%% @end
%%% Copyright (C) 2011 - Anton Lebedevich.  All rights reserved.

-module(epgsqli).

-export([start_link/0,
         connect/1, connect/2, connect/3, connect/4, connect/5,
         close/1,
         get_parameter/2,
         set_notice_receiver/2,
         get_cmd_status/1,
         squery/2,
         equery/2, equery/3,
         prepared_query/3,
         parse/2, parse/3, parse/4,
         describe/2, describe/3,
         bind/3, bind/4,
         execute/2, execute/3, execute/4,
         execute_batch/2, execute_batch/3,
         close/2, close/3,
         sync/1,
         cancel/1]).

-include("epgsql.hrl").

%% -- client interface --

start_link() ->
    epgsql_sock:start_link().

connect(Opts) ->
    {ok, C} = epgsql_sock:start_link(),
    call_connect(C, Opts).

connect(Host, Opts) ->
    connect(Host, os:getenv("USER"), "", Opts).

connect(Host, Username, Opts) ->
    connect(Host, Username, "", Opts).

connect(Host, Username, Password, Opts) ->
    {ok, C} = epgsql_sock:start_link(),
    connect(C, Host, Username, Password, Opts).

-spec connect(epgsql:connection(), inet:ip_address() | inet:hostname(),
              string(), string(), epgsql:connect_opts()) -> reference().
connect(C, Host, Username, Password, Opts) ->
    Opts1 = maps:merge(epgsql:to_map(Opts),
                       #{host => Host,
                         username => Username,
                         password => Password}),
    call_connect(C, Opts1).

call_connect(C, Opts) ->
    Opts1 = epgsql_cmd_connect:opts_hide_password(epgsql:to_map(Opts)),
    epgsqla:complete_connect(
      C, incremental(C, epgsql_cmd_connect, Opts1), Opts1).


-spec close(epgsql:connection()) -> ok.
close(C) ->
    epgsql_sock:close(C).

-spec get_parameter(epgsql:connection(), binary()) -> binary() | undefined.
get_parameter(C, Name) ->
    epgsql_sock:get_parameter(C, Name).

-spec set_notice_receiver(epgsql:connection(), undefined | pid() | atom()) ->
                                 {ok, Previous :: pid() | atom()}.
set_notice_receiver(C, PidOrName) ->
    epgsql_sock:set_notice_receiver(C, PidOrName).

-spec get_cmd_status(epgsql:connection()) -> {ok, Status} when
          Status :: undefined | atom() | {atom(), integer()}.
get_cmd_status(C) ->
    epgsql_sock:get_cmd_status(C).

-spec squery(epgsql:connection(), epgsql:sql_query()) -> reference().
squery(C, Sql) ->
    incremental(C, epgsql_cmd_squery, Sql).

-spec equery(epgsql:connection(), epgsql:statement()) -> reference().
equery(C, Statement) ->
    equery(C, Statement, []).

-spec equery(epgsql:connection(), epgsql:statement(), [epgsql:typed_param()]) -> reference().
equery(C, Statement, TypedParameters) ->
    incremental(C, epgsql_cmd_equery, {Statement, TypedParameters}).

-spec prepared_query(epgsql:connection(), epgsql:statement(), [epgsql:typed_param()]) -> reference().
prepared_query(C, Statement, TypedParameters) ->
    incremental(C, epgsql_cmd_prepared_query, {Statement, TypedParameters}).

-spec parse(epgsql:connection(), epgsql:sql_query()) -> reference().
parse(C, Sql) ->
    parse(C, "", Sql, []).

-spec parse(epgsql:connection(), epgsql:sql_query(), [epgsql:epgsql_type()]) -> reference().
parse(C, Sql, Types) ->
    parse(C, "", Sql, Types).

-spec parse(epgsql:connection(), iolist(), epgsql:sql_query(), [epgsql:epgsql_type()]) -> reference().
parse(C, Name, Sql, Types) ->
    incremental(C, epgsql_cmd_parse, {Name, Sql, Types}).

bind(C, Statement, Parameters) ->
    bind(C, Statement, "", Parameters).

-spec bind(epgsql:connection(), epgsql:statement(), string(), [epgsql:bind_param()]) -> reference().
bind(C, Statement, PortalName, Parameters) ->
    incremental(C, epgsql_cmd_bind, {Statement, PortalName, Parameters}).

execute(C, S) ->
    execute(C, S, "", 0).

execute(C, S, N) ->
    execute(C, S, "", N).

-spec execute(epgsql:connection(), epgsql:statement(), string(), non_neg_integer()) -> reference().
execute(C, Statement, PortalName, MaxRows) ->
    incremental(C, epgsql_cmd_execute, {Statement, PortalName, MaxRows}).

-spec execute_batch(epgsql:connection(), [{epgsql:statement(), [epgsql:bind_param()]}]) -> reference().
execute_batch(C, Batch) ->
    incremental(C, epgsql_cmd_batch, Batch).

-spec execute_batch(epgsql:connection(), epgsql:statement(), [ [epgsql:bind_param()] ]) -> reference().
execute_batch(C, #statement{} = Statement, Batch) ->
    incremental(C, epgsql_cmd_batch, {Statement, Batch}).

describe(C, #statement{name = Name}) ->
    describe(C, statement, Name).

describe(C, statement, Name) ->
    incremental(C, epgsql_cmd_describe_statement, Name);

describe(C, portal, Name) ->
    incremental(C, epgsql_cmd_describe_portal, Name).

close(C, #statement{name = Name}) ->
    close(C, statement, Name).

close(C, Type, Name) ->
    incremental(C, epgsql_cmd_close, {Type, Name}).

sync(C) ->
    incremental(C, epgsql_cmd_sync, []).

-spec cancel(epgsql:connection()) -> ok.
cancel(C) ->
    epgsql_sock:cancel(C).


%% -- internal functions --

incremental(C, Command, Args) ->
    epgsql_sock:async_command(C, incremental, Command, Args).
