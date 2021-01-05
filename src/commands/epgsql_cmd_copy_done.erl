%%% @doc Tells server that the transfer of COPY data is done.
%%%
%%% It makes server to "commit" the data to the table and switch to the normal command-processing
%%% mode.
%%%
%%% @see epgsql_cmd_copy_from_stdin

-module(epgsql_cmd_copy_done).
-behaviour(epgsql_command).
-export([init/1, execute/2, handle_message/4]).
-export_type([response/0]).

-type response() :: {ok, Count :: non_neg_integer()}
                  | {error, epgsql:query_error()}.

%% -include("epgsql.hrl").
-include("protocol.hrl").

init(_) ->
    [].

execute(Sock0, St) ->
    {PktType, PktData} = epgsql_wire:encode_copy_done(),
    Sock1 = epgsql_sock:set_packet_handler(on_message, Sock0),
    Sock = epgsql_sock:set_attr(subproto_state, undefined, Sock1),
    {send, PktType, PktData, Sock, St}.

handle_message(?COMMAND_COMPLETE, Bin, Sock, St) ->
    Complete = epgsql_wire:decode_complete(Bin),
    Res = case Complete of
        {copy, Count} -> {ok, Count};
        copy -> ok
    end,
    {add_result, Res, {complete, Complete}, Sock, St};
handle_message(?ERROR, Error, Sock, St) ->
    Result = {error, Error},
    {add_result, Result, Result, Sock, St};
handle_message(?READY_FOR_QUERY, _Status, Sock, _State) ->
    [Result] = epgsql_sock:get_results(Sock),
    {finish, Result, done, Sock};
handle_message(_, _, _, _) ->
    unknown.
