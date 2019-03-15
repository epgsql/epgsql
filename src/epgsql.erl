%%% Copyright (C) 2008 - Will Glozer.  All rights reserved.
%%% Copyright (C) 2011 - Anton Lebedevich.  All rights reserved.

-module(epgsql).

-export([connect/1, connect/2, connect/3, connect/4, connect/5,
         close/1,
         get_parameter/2,
         set_notice_receiver/2,
         get_cmd_status/1,
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
         with_transaction/3,
         sync_on_error/2,
         standby_status_update/3,
         start_replication/5,
         start_replication/6,
         start_replication/7,
         to_map/1]).
-export([handle_x_log_data/5]).                 % private

-export_type([connection/0, connect_option/0, connect_opts/0,
              connect_error/0, query_error/0, sql_query/0, column/0,
              type_name/0, epgsql_type/0, statement/0]).

%% Deprecated types
-export_type([bind_param/0, typed_param/0,
              squery_row/0, equery_row/0, reply/1,
              pg_time/0, pg_date/0, pg_datetime/0, pg_interval/0]).

-include("epgsql.hrl").

-type sql_query() :: iodata().
-type host() :: inet:ip_address() | inet:hostname().
-type password() :: string() | iodata() | fun( () -> iodata() ).
-type connection() :: pid().
-type connect_option() ::
    {host, host()}                                 |
    {username, string()}                           |
    {password, password()}                         |
    {database, DBName     :: string()}             |
    {port,     PortNum    :: inet:port_number()}   |
    {ssl,      IsEnabled  :: boolean() | required} |
    {ssl_opts, SslOptions :: [ssl:ssl_option()]}   | % see OTP ssl app, ssl_api.hrl
    {timeout,  TimeoutMs  :: timeout()}            | % default: 5000 ms
    {async,    Receiver   :: pid() | atom()}       | % process to receive LISTEN/NOTIFY msgs
    {codecs,   Codecs     :: [{epgsql_codec:codec_mod(), any()}]} |
    {replication, Replication :: string()}. % Pass "database" to connect in replication mode

-type connect_opts() ::
        [connect_option()]
      | #{host => host(),
          username => string(),
          password => password(),
          database => string(),
          port => inet:port_number(),
          ssl => boolean() | required,
          ssl_opts => [ssl:ssl_option()],
          timeout => timeout(),
          async => pid() | atom(),
          codecs => [{epgsql_codec:codec_mod(), any()}],
          replication => string()}.

-type connect_error() :: epgsql_cmd_connect:connect_error().
-type query_error() :: #error{}.


-type type_name() :: atom().
-type epgsql_type() :: type_name()
                     | {array, type_name()}
                     | {unknown_oid, integer()}.

%% Deprecated
-type pg_date() :: epgsql_codec_datetime:pg_date().
-type pg_time() :: epgsql_codec_datetime:pg_time().
-type pg_datetime() :: epgsql_codec_datetime:pg_datetime().
-type pg_interval() :: epgsql_codec_datetime:pg_interval().

%% Deprecated
-type bind_param() :: any().

-type typed_param() :: {epgsql_type(), bind_param()}.

-type column() :: #column{}.
-type statement() :: #statement{}.
-type squery_row() :: tuple(). % tuple of binary().
-type equery_row() :: tuple(). % tuple of bind_param().
-type ok_reply(RowType) ::
        %% select
    {ok, ColumnsDescription :: [column()], RowsValues :: [RowType]} |
        %% update/insert/delete
    {ok, Count :: non_neg_integer()} |
        %% update/insert/delete + returning
    {ok, Count :: non_neg_integer(), ColumnsDescription :: [column()], RowsValues :: [RowType]}.
-type error_reply() :: {error, query_error()}.
-type reply(RowType) :: ok_reply(RowType) | error_reply().
-type lsn() :: integer().
-type cb_state() :: term().

%% See https://github.com/erlang/rebar3/pull/1773
-ifndef(OTP_RELEASE).                           % pre-OTP21
-define(WITH_STACKTRACE(T, R, S), T:R -> S = erlang:get_stacktrace(), ).
-else.
-define(WITH_STACKTRACE(T, R, S), T:R:S ->).
-endif.

%% -- behaviour callbacks --

%% Handles a XLogData Message (StartLSN, EndLSN, WALRecord, CbState).
%% Return: {ok, LastFlushedLSN, LastAppliedLSN, NewCbState}
-callback handle_x_log_data(lsn(), lsn(), binary(), cb_state()) -> {ok, lsn(), lsn(), cb_state()}.
%% -------------

%% -- client interface --
-spec connect(connect_opts())
        -> {ok, Connection :: connection()} | {error, Reason :: connect_error()}.
connect(Opts) ->
    {ok, C} = epgsql_sock:start_link(),
    call_connect(C, Opts).

connect(Host, Opts) ->
    connect(Host, os:getenv("USER"), "", Opts).

connect(Host, Username, Opts) ->
    connect(Host, Username, "", Opts).

-spec connect(host(), string(), password(), connect_opts())
        -> {ok, Connection :: connection()} | {error, Reason :: connect_error()}.
%% @doc connects to Postgres
%% where
%% `Host'     - host to connect to
%% `Username' - username to connect as, defaults to `$USER'
%% `Password' - optional password to authenticate with
%% `Opts'     - proplist or map of extra options
%% returns `{ok, Connection}' otherwise `{error, Reason}'
connect(Host, Username, Password, Opts) ->
    {ok, C} = epgsql_sock:start_link(),
    connect(C, Host, Username, Password, Opts).

-spec connect(connection(), host(), string(), password(), connect_opts())
        -> {ok, Connection :: connection()} | {error, Reason :: connect_error()}.
connect(C, Host, Username, Password, Opts) ->
    Opts1 = maps:merge(to_map(Opts),
                       #{host => Host,
                         username => Username,
                         password => Password}),
    call_connect(C, Opts1).

-spec call_connect(connection(), connect_opts())
       -> {ok, Connection :: connection()} | {error, Reason :: connect_error()}.
call_connect(C, Opts) ->
    Opts1 = epgsql_cmd_connect:opts_hide_password(to_map(Opts)),
    case epgsql_sock:sync_command(
           C, epgsql_cmd_connect, Opts1) of
        connected ->
            %% If following call fails for you, try to add {codecs, []} connect option
            {ok, _} = maybe_update_typecache(C, Opts1),
            {ok, C};
        Error = {error, _} ->
            Error
    end.


maybe_update_typecache(C, Opts) ->
    maybe_update_typecache(C, maps:get(replication, Opts, undefined), maps:get(codecs, Opts, undefined)).

maybe_update_typecache(C, undefined, undefined) ->
    %% TODO: don't execute 'update_type_cache' when `codecs` is undefined.
    %% This will break backward compatibility
    update_type_cache(C);
maybe_update_typecache(C, undefined, [_ | _] = Codecs) ->
    update_type_cache(C, Codecs);
maybe_update_typecache(_, _, _) ->
    {ok, []}.

update_type_cache(C) ->
    update_type_cache(C, [{epgsql_codec_hstore, []},
                          {epgsql_codec_postgis, []}]).

-spec update_type_cache(connection(), [{epgsql_codec:codec_mod(), Opts :: any()}]) ->
                               epgsql_cmd_update_type_cache:response() | {error, empty}.
update_type_cache(_C, []) ->
    {error, empty};
update_type_cache(C, Codecs) ->
    %% {error, #error{severity = error,
    %%                message = <<"column \"typarray\" does not exist in pg_type">>}}
    %% Do not fail connect if pg_type table in not in the expected
    %% format. Known to happen for Redshift which is based on PG v8.0.2
    epgsql_sock:sync_command(C, epgsql_cmd_update_type_cache, Codecs).

%% @doc close connection
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

%% @doc Returns last command status message
%% If multiple queries were executed using `squery/2', separated by semicolon,
%% only the last query's status will be available.
%% See https://www.postgresql.org/docs/current/static/libpq-exec.html#LIBPQ-PQCMDSTATUS
-spec get_cmd_status(connection()) -> {ok, Status}
                                          when
      Status :: undefined | atom() | {atom(), integer()}.
get_cmd_status(C) ->
    epgsql_sock:get_cmd_status(C).

-spec squery(connection(), sql_query()) -> epgsql_cmd_squery:response().
%% @doc runs simple `SqlQuery' via given `Connection'
squery(Connection, SqlQuery) ->
    epgsql_sock:sync_command(Connection, epgsql_cmd_squery, SqlQuery).

equery(C, Sql) ->
    equery(C, Sql, []).

%% TODO add fast_equery command that doesn't need parsed statement
equery(C, Sql, Parameters) ->
    case parse(C, "", Sql, []) of
        {ok, #statement{types = Types} = S} ->
            TypedParameters = lists:zip(Types, Parameters),
            epgsql_sock:sync_command(C, epgsql_cmd_equery, {S, TypedParameters});
        Error ->
            Error
    end.

-spec equery(connection(), string(), sql_query(), [bind_param()]) ->
                    epgsql_cmd_equery:response().
equery(C, Name, Sql, Parameters) ->
    case parse(C, Name, Sql, []) of
        {ok, #statement{types = Types} = S} ->
            TypedParameters = lists:zip(Types, Parameters),
            epgsql_sock:sync_command(C, epgsql_cmd_equery, {S, TypedParameters});
        Error ->
            Error
    end.

-spec prepared_query(C::connection(), Name::string(), Parameters::[bind_param()]) ->
                            epgsql_cmd_prepared_query:response().
prepared_query(C, Name, Parameters) ->
    case describe(C, statement, Name) of
        {ok, #statement{types = Types} = S} ->
            TypedParameters = lists:zip(Types, Parameters),
            epgsql_sock:sync_command(C, epgsql_cmd_prepared_query, {S, TypedParameters});
        Error ->
            Error
    end.


%% parse

parse(C, Sql) ->
    parse(C, Sql, []).

parse(C, Sql, Types) ->
    parse(C, "", Sql, Types).

-spec parse(connection(), iolist(), sql_query(), [epgsql_type()]) ->
                   epgsql_cmd_parse:response().
parse(C, Name, Sql, Types) ->
    sync_on_error(
      C, epgsql_sock:sync_command(
           C, epgsql_cmd_parse, {Name, Sql, Types})).

%% bind

bind(C, Statement, Parameters) ->
    bind(C, Statement, "", Parameters).

-spec bind(connection(), statement(), string(), [bind_param()]) ->
                  epgsql_cmd_bind:response().
bind(C, Statement, PortalName, Parameters) ->
    sync_on_error(
      C,
      epgsql_sock:sync_command(
        C, epgsql_cmd_bind, {Statement, PortalName, Parameters})).

%% execute

execute(C, S) ->
    execute(C, S, "", 0).

execute(C, S, N) ->
    execute(C, S, "", N).

-spec execute(connection(), statement(), string(), non_neg_integer()) -> Reply when
      Reply :: epgsql_cmd_execute:response().
execute(C, S, PortalName, N) ->
    epgsql_sock:sync_command(C, epgsql_cmd_execute, {S, PortalName, N}).

-spec execute_batch(connection(), [{statement(), [bind_param()]}]) ->
                           epgsql_cmd_batch:response().
execute_batch(C, Batch) ->
    epgsql_sock:sync_command(C, epgsql_cmd_batch, Batch).

%% statement/portal functions
-spec describe(connection(), statement()) -> epgsql_cmd_describe_statement:response().
describe(C, #statement{name = Name}) ->
    describe(C, statement, Name).

-spec describe(connection(), portal, iodata()) -> epgsql_cmd_describe_portal:response();
              (connection(), statement, iodata()) -> epgsql_cmd_describe_statement:response().
describe(C, statement, Name) ->
    sync_on_error(
      C, epgsql_sock:sync_command(
           C, epgsql_cmd_describe_statement, Name));

describe(C, portal, Name) ->
    sync_on_error(
      C, epgsql_sock:sync_command(
           C, epgsql_cmd_describe_portal, Name)).

%% @doc close statement
-spec close(connection(), statement()) -> epgsql_cmd_close:response().
close(C, #statement{name = Name}) ->
    close(C, statement, Name).

-spec close(connection(), statement | portal, iodata()) -> epgsql_cmd_close:response().
close(C, Type, Name) ->
    epgsql_sock:sync_command(C, epgsql_cmd_close, {Type, Name}).

-spec sync(connection()) -> epgsql_cmd_sync:response().
sync(C) ->
    epgsql_sock:sync_command(C, epgsql_cmd_sync, []).

-spec cancel(connection()) -> ok.
cancel(C) ->
    epgsql_sock:cancel(C).

%% misc helper functions
-spec with_transaction(connection(), fun((connection()) -> Reply)) ->
                              Reply | {rollback, any()}
                                  when
      Reply :: any().
with_transaction(C, F) ->
    with_transaction(C, F, [{reraise, false}]).

%% @doc Execute callback function with connection in a transaction.
%% Transaction will be rolled back in case of exception.
%% Options (proplist or map):
%% - reraise (true): when set to true, exception will be re-thrown, otherwise
%%   {rollback, ErrorReason} will be returned
%% - ensure_comitted (false): even when callback returns without exception,
%%   check that transaction was comitted by checking CommandComplete status
%%   of "COMMIT" command. In case when transaction was rolled back, status will be
%%   "rollback" instead of "commit".
%% - begin_opts (""): append extra options to "BEGIN" command (see
%%   https://www.postgresql.org/docs/current/static/sql-begin.html)
%%   Beware of SQL injections! No escaping is made on begin_opts!
-spec with_transaction(
        connection(), fun((connection()) -> Reply), Opts) -> Reply | {rollback, any()} | no_return() when
      Reply :: any(),
      Opts :: [{reraise, boolean()} |
               {ensure_committed, boolean()} |
               {begin_opts, iodata()}].
with_transaction(C, F, Opts0) ->
    Opts = to_map(Opts0),
    Begin = case Opts of
                #{begin_opts := BeginOpts} ->
                    [<<"BEGIN ">> | BeginOpts];
                _ -> <<"BEGIN">>
            end,
    try
        {ok, [], []} = squery(C, Begin),
        R = F(C),
        {ok, [], []} = squery(C, <<"COMMIT">>),
        case Opts of
            #{ensure_committed := true} ->
                {ok, CmdStatus} = get_cmd_status(C),
                (commit == CmdStatus) orelse error({ensure_committed_failed, CmdStatus});
            _ -> ok
        end,
        R
    catch
        ?WITH_STACKTRACE(Type, Reason, Stack)
            squery(C, "ROLLBACK"),
            case maps:get(reraise, Opts, true) of
                true ->
                    erlang:raise(Type, Reason, Stack);
                false ->
                    {rollback, Reason}
            end
    end.

sync_on_error(C, Error = {error, _}) ->
    ok = sync(C),
    Error;

sync_on_error(_C, R) ->
    R.

-spec standby_status_update(connection(), lsn(), lsn()) -> ok.
%% @doc sends last flushed and applied WAL positions to the server in a standby status update message via
%% given `Connection'
standby_status_update(Connection, FlushedLSN, AppliedLSN) ->
    gen_server:call(Connection, {standby_status_update, FlushedLSN, AppliedLSN}).

handle_x_log_data(Mod, StartLSN, EndLSN, WALRecord, Repl) ->
    Mod:handle_x_log_data(StartLSN, EndLSN, WALRecord, Repl).

-type replication_option() ::
    {align_lsn, boolean()}. %% Align last applied and flushed LSN with last received LSN
                            %%  after Primary keepalive message with ReplyRequired flag

-type replication_opts() ::
    [replication_option()]
    | #{align_lsn => boolean()}.

-spec start_replication(connection(), string(), Callback, cb_state(), string(), string(), replication_opts()) ->
    Response when
    Response :: epgsql_cmd_start_replication:response(),
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
%% `Opts'            - options of logical replication
%% returns `ok' otherwise `{error, Reason}'
start_replication(Connection, ReplicationSlot, Callback, CbInitState, WALPosition, PluginOpts, Opts) ->
    Command = {ReplicationSlot, Callback, CbInitState, WALPosition, PluginOpts, to_map(Opts)},
    epgsql_sock:sync_command(Connection, epgsql_cmd_start_replication, Command).
start_replication(Connection, ReplicationSlot, Callback, CbInitState, WALPosition, PluginOpts) ->
    start_replication(Connection, ReplicationSlot, Callback, CbInitState, WALPosition, PluginOpts, []).
start_replication(Connection, ReplicationSlot, Callback, CbInitState, WALPosition) ->
    start_replication(Connection, ReplicationSlot, Callback, CbInitState, WALPosition, [], []).

%% @private
-spec to_map([{any(), any()}] | map()) -> map().
to_map(Map) when is_map(Map) ->
    Map;
to_map(List) when is_list(List) ->
    maps:from_list(List).
