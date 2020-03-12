%% @doc Synchronize client and server states for multi-command combinations
%%
%% Should be executed if APIs start to return `{error, sync_required}'.
%% ```
%% > Sync
%% < ReadyForQuery
%% '''
-module(epgsql_cmd_sync).
-behaviour(epgsql_command).
-export([init/1, execute/2, handle_message/4]).
-export_type([response/0]).

-type response() :: ok | {error, epgsql:query_error()}.

-include("epgsql.hrl").
-include("protocol.hrl").


init(_) ->
    undefined.

execute(Sock, St) ->
    epgsql_sock:send(Sock, ?SYNC, []),
    Sock1 = epgsql_sock:set_attr(sync_required, false, Sock),
    {ok, Sock1, St}.

handle_message(?READY_FOR_QUERY, _, Sock, _State) ->
    {finish, ok, ok, Sock};
handle_message(?ERROR, Error, _Sock, _State) ->
    {sync_required, {error, Error}};
handle_message(_, _, _, _) ->
    unknown.
