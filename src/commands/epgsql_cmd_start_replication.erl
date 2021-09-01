%% @doc Requests server to start sending replication packets
%%
%% See {@link epgsql:connect/1} `replication' parameter.
%% ```
%% > SimpleQuery "START_REPLICATION ..."
%% < CopyBothResponse | Error
%% '''
-module(epgsql_cmd_start_replication).
-behaviour(epgsql_command).
-export([init/1, execute/2, handle_message/4]).
-export_type([response/0]).

-type response() :: ok | {error, epgsql:query_error()}.

-include("epgsql.hrl").
-include("protocol.hrl").
-include("../epgsql_replication.hrl").

-record(start_repl,
        {slot,
         callback,
         cb_state,
         wal_pos,
         plugin_opts,
         opts}).

init({ReplicationSlot, Callback, CbInitState, WALPosition, PluginOpts, Opts}) ->
    #start_repl{slot = ReplicationSlot,
                callback = Callback,
                cb_state = CbInitState,
                wal_pos = WALPosition,
                plugin_opts = PluginOpts,
                opts = Opts}.

execute(Sock, #start_repl{slot = ReplicationSlot, callback = Callback,
                          cb_state = CbInitState, wal_pos = WALPosition,
                          plugin_opts = PluginOpts, opts = Opts} = St) ->
    %% Connection should be started with 'replication' option. Then
    %% 'replication_state' will be initialized
    Repl = #repl{} = epgsql_sock:get_subproto_state(Sock),
    Sql1 = ["START_REPLICATION SLOT ", ReplicationSlot, " LOGICAL ", WALPosition],
    Sql2 =
        case PluginOpts of
            [] -> Sql1;
            PluginOpts -> [Sql1 , " (", PluginOpts, ")"]
        end,

    Repl2 =
        case Callback of
            Pid when is_pid(Pid) -> Repl#repl{receiver = Pid};
            Module -> Repl#repl{cbmodule = Module, cbstate = CbInitState}
        end,

    Hex = [H || H <- WALPosition, H =/= $/],
    {ok, [LSN], _} = io_lib:fread("~16u", Hex),
    AlignLsn = maps:get(align_lsn, Opts, false),
    Repl3 = Repl2#repl{last_flushed_lsn = LSN,
                       last_applied_lsn = LSN,
                       align_lsn = AlignLsn},
    Sock2 = epgsql_sock:set_attr(subproto_state, Repl3, Sock),
                         %% handler = on_replication},
    {PktType, PktData} = epgsql_wire:encode_query(Sql2),
    {send, PktType, PktData, Sock2, St}.

%% CopyBothResponse
handle_message(?COPY_BOTH_RESPONSE, _Data, Sock, _State) ->
    {finish, ok, ok, epgsql_sock:set_packet_handler(on_replication, Sock)};
handle_message(?ERROR, Error, _Sock, _State) ->
    Result = {error, Error},
    {sync_required, Result};
handle_message(_, _, _, _) ->
    unknown.
