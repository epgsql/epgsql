%%% Copyright (C) 2008 - Will Glozer.  All rights reserved.
%%% Copyright (C) 2011 - Anton Lebedevich.  All rights reserved.

-module(pgsql).

-export([connect/2, connect/3, connect/4, connect/5,
         close/1,
         get_parameter/2,
         squery/2,
         equery/2, equery/3, equery/4,
         parse/2, parse/3, parse/4,
         describe/2, describe/3,
         bind/3, bind/4,
         execute/2, execute/3, execute/4,
         execute_batch/2,
         close/2, close/3,
         sync/1,
         cancel/1,
         update_type_cache/1,
         with_transaction/2,
         sync_on_error/2]).

-export_type([connection/0, connect_option/0,
              connect_error/0, query_error/0,
              bind_param/0,
              squery_row/0, equery_row/0, ok_reply/1]).

-include("pgsql.hrl").

-type connection() :: pid().
-type connect_option() :: {database, string()}
                          | {port, inet:port_number()}
                          | {ssl, boolean() | required}
                          | {ssl_opts, list()} % ssl:option(), see OTP ssl_api.hrl
                          | {timeout, timeout()}
                          | {async, pid()}.
-type connect_error() :: #error{}.
-type query_error() :: #error{}.

-type bind_param() ::
        null
        | boolean()
        | string()
        | binary()
        | integer()
        | float()
        | calendar:date()
        | calendar:time()                       %actualy, `Seconds' may be float()
        | calendar:datetime()
        | {calendar:time(), Days::non_neg_integer(), Months::non_neg_integer()}
        | [bind_param()].                       %array (maybe nested)

-type squery_row() :: {binary()}.
-type equery_row() :: {bind_param()}.
-type ok_reply(RowType) :: {ok, [#column{}], [RowType]}  % SELECT
                         | {ok, non_neg_integer()}   % UPDATE / INSERT
                         | {ok, non_neg_integer(), [#column{}], [RowType]}. % UPDATE / INSERT + RETURNING

%% -- client interface --

connect(Host, Opts) ->
    connect(Host, os:getenv("USER"), "", Opts).

connect(Host, Username, Opts) ->
    connect(Host, Username, "", Opts).

connect(Host, Username, Password, Opts) ->
    {ok, C} = pgsql_sock:start_link(),
    connect(C, Host, Username, Password, Opts).

-spec connect(connection(), inet:ip_address() | inet:hostname(),
              string(), string(), [connect_option()]) ->
                     {ok, pid()} | {error, connect_error()}.
connect(C, Host, Username, Password, Opts) ->
    %% TODO connect timeout
    case gen_server:call(C,
                         {connect, Host, Username, Password, Opts},
                         infinity) of
        connected ->
            update_type_cache(C),
            {ok, C};
        Error = {error, _} ->
            Error
    end.

-spec update_type_cache(connection()) -> ok.
update_type_cache(C) ->
    DynamicTypes = [<<"hstore">>],
    Query = "SELECT typname, oid::int4, typarray::int4"
            " FROM pg_type"
            " WHERE typname = ANY($1::varchar[])",
    {ok, _, TypeInfos} = equery(C, Query, [DynamicTypes]),
    ok = gen_server:call(C, {update_type_cache, TypeInfos}).

-spec close(connection()) -> ok.
close(C) ->
    pgsql_sock:close(C).

-spec get_parameter(connection(), binary()) -> binary() | undefined.
get_parameter(C, Name) ->
    pgsql_sock:get_parameter(C, Name).

-spec squery(connection(), string() | iodata()) ->
                    ok_reply(squery_row()) | {error, query_error()} |
                    [ok_reply(squery_row()) | {error, query_error()}].
squery(C, Sql) ->
    gen_server:call(C, {squery, Sql}, infinity).

equery(C, Sql) ->
    equery(C, Sql, []).

%% TODO add fast_equery command that doesn't need parsed statement
equery(C, Sql, Parameters) ->
    case parse(C, "", Sql, []) of
        {ok, #statement{types = Types} = S} ->
            Typed_Parameters = lists:zip(Types, Parameters),
            gen_server:call(C, {equery, S, Typed_Parameters}, infinity);
        Error ->
            Error
    end.

-spec equery(connection(), string(), string() | iodata(), [bind_param()]) ->
                    ok_reply(equery_row()) | {error, query_error()}.
equery(C, Name, Sql, Parameters) ->
    case parse(C, Name, Sql, []) of
        {ok, #statement{types = Types} = S} ->
            Typed_Parameters = lists:zip(Types, Parameters),
            gen_server:call(C, {equery, S, Typed_Parameters}, infinity);
        Error ->
            Error
    end.

%% parse

parse(C, Sql) ->
    parse(C, Sql, []).

parse(C, Sql, Types) ->
    parse(C, "", Sql, Types).

-spec parse(connection(), iolist(), string() | iodata(), [epgsql_type()]) ->
                   {ok, #statement{}} | {error, query_error()}.
parse(C, Name, Sql, Types) ->
    sync_on_error(C, gen_server:call(C, {parse, Name, Sql, Types}, infinity)).

%% bind

bind(C, Statement, Parameters) ->
    bind(C, Statement, "", Parameters).

-spec bind(connection(), #statement{}, string(), [bind_param()]) ->
                  ok | {error, query_error()}.
bind(C, Statement, PortalName, Parameters) ->
    sync_on_error(
      C,
      gen_server:call(C, {bind, Statement, PortalName, Parameters}, infinity)).

%% execute

execute(C, S) ->
    execute(C, S, "", 0).

execute(C, S, N) ->
    execute(C, S, "", N).

-spec execute(connection(), #statement{}, string(), non_neg_integer()) -> Reply
                                                                              when
      Reply :: {ok | partial, [equery_row()]}
             | {ok, non_neg_integer()}
             | {ok, non_neg_integer(), [equery_row()]}
             | {error, query_error()}.
execute(C, S, PortalName, N) ->
    gen_server:call(C, {execute, S, PortalName, N}, infinity).

-spec execute_batch(connection(), [{#statement{}, [bind_param()]}]) ->
                           [ok_reply(equery_row()) | {error, query_error()}].
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

-spec cancel(connection()) -> ok.
cancel(C) ->
    pgsql_sock:cancel(C).

%% misc helper functions
-spec with_transaction(connection(), fun((connection()) -> Reply)) ->
                              Reply | {rollback, any()}
                                  when
      Reply :: any().
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

