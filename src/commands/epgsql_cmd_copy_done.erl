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
-include("../epgsql_copy.hrl").

init(_) ->
    [].

execute(Sock0, St) ->
    #copy{format = Format} = epgsql_sock:get_subproto_state(Sock0), % assert we are in copy-mode
    Sock1 = epgsql_sock:set_packet_handler(on_message, Sock0),
    Sock = epgsql_sock:set_attr(subproto_state, undefined, Sock1),
    {PktType, PktData} = epgsql_wire:encode_copy_done(),
    case Format of
        text ->
            {send, PktType, PktData, Sock, St};
        binary ->
            Pkts = [{?COPY_DATA, epgsql_wire:encode_copy_trailer()},
                    {PktType, PktData}],
            {send_multi, Pkts, Sock, St}
    end.

handle_message(?COMMAND_COMPLETE, Bin, Sock, St) ->
    Complete = {copy, Count} = epgsql_wire:decode_complete(Bin),
    {add_result, {ok, Count}, {complete, Complete}, Sock, St};
handle_message(?ERROR, Error, Sock, St) ->
    Result = {error, Error},
    {add_result, Result, Result, Sock, St};
handle_message(?READY_FOR_QUERY, _Status, Sock, _State) ->
    [Result] = epgsql_sock:get_results(Sock),
    {finish, Result, done, Sock};
handle_message(_, _, _, _) ->
    unknown.
