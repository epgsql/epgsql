%%% @doc Tells server to switch to "COPY-in" mode
%%%
%%% See [https://www.postgresql.org/docs/current/sql-copy.html].
%%% See [https://www.postgresql.org/docs/current/protocol-flow.html#PROTOCOL-COPY].
%%%
%%% When `Format' is `text', copy data should then be delivered using Erlang
%%% <a href="https://erlang.org/doc/apps/stdlib/io_protocol.html">io protocol</a>.
%%% See {@link file:write/2}, {@link io:put_chars/2}.
%%% "End-of-data" marker `\.' at the end of TEXT or CSV data stream is not needed.
%%%
%%% When `Format' is `{binary, [epgsql_type()]}', recommended way to deliver data is
%%% {@link epgsql:copy_send_rows/3}. IO-protocol can be used as well, as long as you can
%%% do proper binary encoding of data tuples (header and trailer are sent automatically),
%%% see [https://www.postgresql.org/docs/current/sql-copy.html#id-1.9.3.55.9.4.6].
%%% When you don't know what are the correct type names for your columns, you could try to
%%% construct equivalent `INSERT' or `SELECT' statement and call {@link epgsql:parse/2} command.
%%% It will return `#statement{columns = [#column{type = TypeName}]}' with correct type names.
%%%
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

-type response() :: {ok, [text | binary]} | {error, epgsql:query_error()}.

-include("epgsql.hrl").
-include("protocol.hrl").
-include("../epgsql_copy.hrl").

-record(copy_stdin,
        {query :: iodata(),
         initiator :: pid(),
         format :: {binary, [epgsql:epgsql_type()]} | text}).

init({SQL, Initiator, Format}) ->
    #copy_stdin{query = SQL, initiator = Initiator, format = Format}.

execute(Sock, #copy_stdin{query = SQL, format = Format} = St) ->
    undefined = epgsql_sock:get_subproto_state(Sock), % assert we are not in copy-mode already
    {PktType, PktData} = epgsql_wire:encode_query(SQL),
    case Format of
        text ->
            {send, PktType, PktData, Sock, St};
        {binary, _} ->
            Header = epgsql_wire:encode_copy_header(),
            {send_multi, [{PktType, PktData},
                          {?COPY_DATA, Header}], Sock, St}
    end.

%% CopyBothResponses
handle_message(?COPY_IN_RESPONSE, <<BinOrText, NumColumns:?int16, Formats/binary>>, Sock,
               #copy_stdin{initiator = Initiator, format = RequestedFormat}) ->
    ColumnFormats = [format_to_atom(Format) || <<Format:?int16>> <= Formats],
    length(ColumnFormats) =:= NumColumns orelse error(invalid_copy_in_response),
    CopyState = init_copy_state(format_to_atom(BinOrText), RequestedFormat, ColumnFormats, Initiator),
    Sock1 = epgsql_sock:set_attr(subproto_state, CopyState, Sock),
    Res = {ok, ColumnFormats},
    {finish, Res, Res, epgsql_sock:set_packet_handler(on_copy_from_stdin, Sock1)};
handle_message(?ERROR, Error, _Sock, _State) ->
    Result = {error, Error},
    {sync_required, Result};
handle_message(_, _, _, _) ->
    unknown.

init_copy_state(text, text, ColumnFormats, Initiator) ->
    %% When BinOrText is `text', all "columns" should be `text' format as well.
    %% See https://www.postgresql.org/docs/current/protocol-message-formats.html
    %% CopyInResponse
    (lists:member(binary, ColumnFormats) == false)
        orelse error(invalid_copy_in_response),
    #copy{initiator = Initiator, format = text};
init_copy_state(binary, {binary, ColumnTypes}, ColumnFormats, Initiator) ->
    %% https://www.postgresql.org/docs/current/protocol-flow.html#PROTOCOL-COPY
    %% "As of the present implementation, all columns in a given COPY operation will use the same
    %% format, but the message design does not assume this."
    (lists:member(text, ColumnFormats) == false)
        orelse error(invalid_copy_in_response),
    NumColumns = length(ColumnFormats),
    %% Eg, `epgsql:copy_from_stdin(C, "COPY tab (a, b, c) WITH (FORMAT binary)", {binary, [int2, int4]})'
    %% so number of columns in SQL is not same as number of types in `binary'
    (NumColumns == length(ColumnTypes))
        orelse error({column_count_mismatch, ColumnTypes, NumColumns}),
    #copy{initiator = Initiator, format = binary, binary_types = ColumnTypes};
init_copy_state(ServerExpectedFormat, RequestedFormat, _, _Initiator) ->
    %% Eg, `epgsql:copy_from_stdin(C, "COPY ... WITH (FORMAT text)", {binary, ...})' or
    %% `epgsql:copy_from_stdin(C, "COPY ... WITH (FORMAT binary)", text)' or maybe PostgreSQL
    %% got some new format epgsql is not aware of
    error({format_mismatch, RequestedFormat, ServerExpectedFormat}).

format_to_atom(0) -> text;
format_to_atom(1) -> binary.
