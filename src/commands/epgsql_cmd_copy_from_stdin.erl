%%% @doc Tells server to switch to "COPY-in" mode
%%%
%%% See [https://www.postgresql.org/docs/current/sql-copy.html].
%%% See [https://www.postgresql.org/docs/current/protocol-flow.html#PROTOCOL-COPY].
%%%
%%% The copy data can then be delivered using Erlang
%%% <a href="https://erlang.org/doc/apps/stdlib/io_protocol.html">io protocol</a>.
%%% See {@link file:write/2}, {@link io:put_chars/2}.
%%%
%%% "End-of-data" marker `\.' at the end of TEXT or CSV data stream is not needed,
%%% {@link epgsql_cmd_copy_done} should be called in the end.
%%%
%%% This command should not be used with command pipelining!
%%%
%%% ```
%%% > SQuery COPY ... FROM STDIN ...
%%% < CopyInResponse
%%% > CopyData*            -- implemented in io protocol, not here
%%% > CopyDone | CopyFail  -- implemented in epgsql_cmd_copy_done
%%% < CommandComplete      -- implemented in epgsql_cmd_copy_done
%%% '''
-module(epgsql_cmd_copy_from_stdin).
-behaviour(epgsql_command).
-export([init/1, execute/2, handle_message/4]).
-export_type([response/0]).

-type response() :: ok | {error, epgsql:query_error()}.

-include("epgsql.hrl").
-include("protocol.hrl").
-include("../epgsql_copy.hrl").

-record(copy_stdin,
        {query :: iodata(), initiator :: pid()}).

init({SQL, Initiator}) ->
    #copy_stdin{query = SQL, initiator = Initiator}.

execute(Sock, #copy_stdin{query = SQL} = St) ->
    undefined = epgsql_sock:get_subproto_state(Sock), % assert we are not in copy-mode already
    {PktType, PktData} = epgsql_wire:encode_query(SQL),
    {send, PktType, PktData, Sock, St}.

%% CopyBothResponseщ
handle_message(?COPY_IN_RESPONSE, <<BinOrText, NumColumns:?int16, Formats/binary>>, Sock,
               #copy_stdin{initiator = Initiator}) ->
    ColumnFormats =
        [case Format of
             0 -> text;
             1 -> binary
         end || <<Format:?int16>> <= Formats],
    length(ColumnFormats) =:= NumColumns orelse error(invalid_copy_in_response),
    case BinOrText of
        0 ->
            %% When BinOrText is 0, all "columns" should be 0 format as well.
            %% See https://www.postgresql.org/docs/current/protocol-message-formats.html
            %% CopyInResponse
            (lists:member(binary, ColumnFormats) == false)
                orelse error(invalid_copy_in_response);
        _ ->
            ok
    end,
    CopyState = #copy{initiator = Initiator},
    Sock1 = epgsql_sock:set_attr(subproto_state, CopyState, Sock),
    Res = {ok, ColumnFormats},
    {finish, Res, Res, epgsql_sock:set_packet_handler(on_copy_from_stdin, Sock1)};
handle_message(?ERROR, Error, _Sock, _State) ->
    Result = {error, Error},
    {sync_required, Result};
handle_message(_, _, _, _) ->
    unknown.
