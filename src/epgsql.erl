%%% Copyright (C) 2008 - Will Glozer.  All rights reserved.
%%% Copyright (C) 2011 - Anton Lebedevich.  All rights reserved.

-module(epgsql).

-export([connect/1, connect/2, connect/3, connect/4, connect/5,
         close/1,
         get_parameter/2,
         set_notice_receiver/2,
         squery/2,
         equery/2, equery/3, equery/4,
         prepared_query/3,
         parse/2, parse/3, parse/4,
         describe/2, describe/3,
         bind/3, bind/4,
         execute/2, execute/3, execute/4,
         execute_batch/2,
         close/2, close/3,
         sync/1,
         cancel/1,
         update_type_cache/1,
         update_type_cache/2,
         with_transaction/2,
         sync_on_error/2,
         standby_status_update/3,
         start_replication/5,
         start_replication/6,
         to_proplist/1]).

-export_type([connection/0, connect_option/0, connect_opts/0,
              connect_error/0, query_error/0,
              sql_query/0, bind_param/0, typed_param/0,
              squery_row/0, equery_row/0, reply/1]).

-include("epgsql.hrl").

-type sql_query() :: string() | iodata().
-type host() :: inet:ip_address() | inet:hostname().
-type connection() :: pid().
-type connect_option() ::
    {host, host()}                                 |
    {username, string()}                           |
    {password, string()}                           |
    {database, DBName     :: string()}             |
    {port,     PortNum    :: inet:port_number()}   |
    {ssl,      IsEnabled  :: boolean() | required} |
    {ssl_opts, SslOptions :: [ssl:ssl_option()]}   | % @see OTP ssl app, ssl_api.hrl
    {timeout,  TimeoutMs  :: timeout()}            | % default: 5000 ms
    {async,    Receiver   :: pid() | atom()}       | % process to receive LISTEN/NOTIFY msgs
    {replication, Replication :: string()}. % Pass "database" to connect in replication mode

-ifdef(have_maps).
-type connect_opts() ::
        [connect_option()]
      | #{host => host(),
          username => string(),
          password => string(),
          database => string(),
          port => inet:port_number(),
          ssl => boolean() | required,
          ssl_opts => [ssl:ssl_option()],
          timeout => timeout(),
          async => pid(),
          replication => string()}.
-else.
-type connect_opts() :: [connect_option()].
-endif.

-type connect_error() ::
        #error{}
      | {unsupported_auth_method, atom()}
      | invalid_authorization_specification
      | invalid_password.
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
        | {list({binary(), binary() | null})}   % hstore
        | [bind_param()].                       %array (maybe nested)

-type typed_param() ::
    {epgsql_type(), bind_param()}.

-type squery_row() :: tuple(). % tuple of binary().
-type equery_row() :: tuple(). % tuple of bind_param().
-type ok_reply(RowType) ::
    {ok, ColumnsDescription :: [#column{}], RowsValues :: [RowType]} |                            % select
    {ok, Count :: non_neg_integer()} |                                                            % update/insert/delete
    {ok, Count :: non_neg_integer(), ColumnsDescription :: [#column{}], RowsValues :: [RowType]}. % update/insert/delete + returning
-type error_reply() :: {error, query_error()}.
-type reply(RowType) :: ok_reply(RowType) | error_reply().
-type lsn() :: integer().
-type cb_state() :: term().

%% -- behaviour callbacks --

%% Handles a XLogData Message (StartLSN, EndLSN, WALRecord, CbState).
%% Return: {ok, LastFlushedLSN, LastAppliedLSN, NewCbState}
-callback handle_x_log_data(lsn(), lsn(), binary(), cb_state()) -> {ok, lsn(), lsn(), cb_state()}.
%% -------------

%% -- client interface --
-spec connect(connect_opts())
        -> {ok, Connection :: connection()} | {error, Reason :: connect_error()}.
connect(Settings0) ->
    Settings = to_proplist(Settings0),
	Host = proplists:get_value(host, Settings, "localhost"),
	Username = proplists:get_value(username, Settings, os:getenv("USER")),
	Password = proplists:get_value(password, Settings, ""),
	connect(Host, Username, Password, Settings).

connect(Host, Opts) ->
    connect(Host, os:getenv("USER"), "", Opts).

connect(Host, Username, Opts) ->
    connect(Host, Username, "", Opts).

-spec connect(host(), string(), string(), connect_opts())
        -> {ok, Connection :: connection()} | {error, Reason :: connect_error()}.
%% @doc connects to Postgres
%% where
%% `Host'     - host to connect to
%% `Username' - username to connect as, defaults to `$USER'
%% `Password' - optional password to authenticate with
%% `Opts'     - proplist of extra options
%% returns `{ok, Connection}' otherwise `{error, Reason}'
connect(Host, Username, Password, Opts) ->
    {ok, C} = epgsql_sock:start_link(),
    connect(C, Host, Username, Password, Opts).

-spec connect(connection(), host(), string(), string(), connect_opts())
        -> {ok, Connection :: connection()} | {error, Reason :: connect_error()}.
connect(C, Host, Username, Password, Opts0) ->
    Opts = to_proplist(Opts0),
    %% TODO connect timeout
    case gen_server:call(C,
                         {connect, Host, Username, Password, Opts},
                         infinity) of
        connected ->
            case proplists:get_value(replication, Opts, undefined) of
                undefined ->
                    update_type_cache(C),
                    {ok, C};
                _ -> {ok, C} %% do not update update_type_cache if connection is in replication mode
            end;
        Error = {error, _} ->
            Error
    end.

-spec update_type_cache(connection()) -> ok.
update_type_cache(C) ->
    update_type_cache(C, [<<"hstore">>,<<"geometry">>]).

-spec update_type_cache(connection(), [binary()]) -> ok.
update_type_cache(C, DynamicTypes) ->
    Query = "SELECT typname, oid::int4, typarray::int4"
            " FROM pg_type"
            " WHERE typname = ANY($1::varchar[])",
    case equery(C, Query, [DynamicTypes]) of
        {ok, _, TypeInfos} ->
            ok = gen_server:call(C, {update_type_cache, TypeInfos});
        {error, {error, error, _, _,
                 <<"column \"typarray\" does not exist in pg_type">>, _}} ->
            %% Do not fail connect if pg_type table in not in the expected
            %% format. Known to happen for Redshift which is based on PG v8.0.2
            ok
    end.

-spec close(connection()) -> ok.
close(C) ->
    epgsql_sock:close(C).

-spec get_parameter(connection(), binary()) -> binary() | undefined.
get_parameter(C, Name) ->
    epgsql_sock:get_parameter(C, Name).

-spec set_notice_receiver(connection(), undefined | pid() | atom()) ->
                                 {ok, Previous :: pid() | atom()}.
set_notice_receiver(C, PidOrName) ->
    epgsql_sock:set_notice_receiver(C, PidOrName).

-spec squery(connection(), sql_query()) -> reply(squery_row()) | [reply(squery_row())].
%% @doc runs simple `SqlQuery' via given `Connection'
squery(Connection, SqlQuery) ->
    gen_server:call(Connection, {squery, SqlQuery}, infinity).

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

-spec equery(connection(), string(), sql_query(), [bind_param()]) -> reply(equery_row()).
equery(C, Name, Sql, Parameters) ->
    case parse(C, Name, Sql, []) of
        {ok, #statement{types = Types} = S} ->
            Typed_Parameters = lists:zip(Types, Parameters),
            gen_server:call(C, {equery, S, Typed_Parameters}, infinity);
        Error ->
            Error
    end.

-spec prepared_query(C::connection(), Name::string(), Parameters::[bind_param()]) -> reply(equery_row()).
prepared_query(C, Name, Parameters) ->
    case describe(C, statement, Name) of
        {ok, #statement{types = Types} = S} ->
            Typed_Parameters = lists:zip(Types, Parameters),
            gen_server:call(C, {prepared_query, S, Typed_Parameters}, infinity);
        Error ->
            Error
    end.


%% parse

parse(C, Sql) ->
    parse(C, Sql, []).

parse(C, Sql, Types) ->
    parse(C, "", Sql, Types).

-spec parse(connection(), iolist(), sql_query(), [epgsql_type()]) ->
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

-spec execute_batch(connection(), [{#statement{}, [bind_param()]}]) -> [reply(equery_row())].
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
    epgsql_sock:cancel(C).

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

-spec standby_status_update(connection(), lsn(), lsn()) -> ok | error_reply().
%% @doc sends last flushed and applied WAL positions to the server in a standby status update message via given `Connection'
standby_status_update(Connection, FlushedLSN, AppliedLSN) ->
    gen_server:call(Connection, {standby_status_update, FlushedLSN, AppliedLSN}).

-spec start_replication(connection(), string(), Callback, cb_state(), string(), string()) -> ok | error_reply() when
    Callback :: module() | pid().
%% @doc instructs Postgres server to start streaming WAL for logical replication
%% where
%% `Connection'      - connection in replication mode
%% `ReplicationSlot' - the name of the replication slot to stream changes from
%% `Callback'        - Callback module which should have the callback functions implemented for message processing.
%%                      or a process which should be able to receive replication messages.
%% `CbInitState'     - Callback Module's initial state
%% `WALPosition'     - the WAL position XXX/XXX to begin streaming at.
%%                      "0/0" to let the server determine the start point.
%% `PluginOpts'      - optional options passed to the slot's logical decoding plugin.
%%                      For example: "option_name1 'value1', option_name2 'value2'"
%% returns `ok' otherwise `{error, Reason}'
start_replication(Connection, ReplicationSlot, Callback, CbInitState, WALPosition, PluginOpts) ->
    gen_server:call(Connection, {start_replication, ReplicationSlot, Callback, CbInitState, WALPosition, PluginOpts}).
start_replication(Connection, ReplicationSlot, Callback, CbInitState, WALPosition) ->
    start_replication(Connection, ReplicationSlot, Callback, CbInitState, WALPosition, []).

%% @private
to_proplist(List) when is_list(List) ->
    List;
to_proplist(Map) ->
    maps:to_list(Map).
