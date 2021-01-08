%%% @doc
%%% Interface to encoder/decoder for postgresql
%%% <a href="https://www.postgresql.org/docs/current/protocol-message-formats.html">wire-protocol</a>
%%%
%%% See also `include/protocol.hrl'.
%%% @end
%%% Copyright (C) 2009 - Will Glozer.  All rights reserved.
%%% Copyright (C) 2011 - Anton Lebedevich.  All rights reserved.

-module(epgsql_wire).

-export([decode_message/1,
         decode_error/1,
         decode_strings/1,
         decode_columns/3,
         decode_parameters/2,
         encode_command/1,
         encode_command/2,
         build_decoder/2,
         decode_data/2,
         decode_complete/1,
         encode_types/2,
         encode_formats/1,
         format/2,
         encode_parameters/2,
         encode_standby_status_update/3,
         encode_copy_header/0,
         encode_copy_row/3,
         encode_copy_trailer/0]).
%% Encoders for Client -> Server packets
-export([encode_query/1,
         encode_parse/3,
         encode_describe/2,
         encode_bind/4,
         encode_copy_done/0,
         encode_execute/2,
         encode_close/2,
         encode_flush/0,
         encode_sync/0]).

-export_type([row_decoder/0, packet_type/0]).

-include("epgsql.hrl").
-include("protocol.hrl").

-opaque row_decoder() :: {[epgsql_binary:decoder()], [epgsql:column()], epgsql_binary:codec()}.
-type packet_type() :: byte().                 % see protocol.hrl
%% -type packet_type(Exact) :: Exact.   % TODO: uncomment when OTP-18 is dropped

%% @doc tries to extract single postgresql packet from TCP stream
-spec decode_message(binary()) -> {packet_type(), binary(), binary()} | binary().
decode_message(<<Type:8, Len:?int32, Rest/binary>> = Bin) ->
    Len2 = Len - 4,
    case Rest of
        <<Data:Len2/binary, Tail/binary>> ->
            {Type, Data, Tail};
        _Other ->
            Bin
    end;
decode_message(Bin) ->
    Bin.

%% @doc decode a single null-terminated string
-spec decode_string(binary()) -> [binary(), ...].
decode_string(Bin) ->
    binary:split(Bin, <<0>>).

%% @doc decode multiple null-terminated string
-spec decode_strings(binary()) -> [binary(), ...].
decode_strings(Bin) ->
    %% Assert the last byte is what we want it to be
    %% Remove that byte from the Binary, so the zero
    %% terminators are separators. Then apply
    %% binary:split/3 directly on the remaining Subj
    Sz = byte_size(Bin) - 1,
    <<Subj:Sz/binary, 0>> = Bin,
    binary:split(Subj, <<0>>, [global]).

-spec encode_string(iodata()) -> iodata().
encode_string(Val) ->
    [Val, 0].

%% @doc decode error's field
-spec decode_fields(binary()) -> [{byte(), binary()}].
decode_fields(Bin) ->
    decode_fields(Bin, []).

decode_fields(<<0>>, Acc) ->
    Acc;
decode_fields(<<Type:8, Rest/binary>>, Acc) ->
    [Str, Rest2] = decode_string(Rest),
    decode_fields(Rest2, [{Type, Str} | Acc]).

%% @doc decode ErrorResponse
%% See [http://www.postgresql.org/docs/current/interactive/protocol-error-fields.html]
-spec decode_error(binary()) -> epgsql:query_error().
decode_error(Bin) ->
    Fields = decode_fields(Bin),
    ErrCode = proplists:get_value($C, Fields),
    ErrName = epgsql_errcodes:to_name(ErrCode),
    {ErrSeverity, Extra} = case proplists:get_value($V, Fields) of
        undefined ->
            {proplists:get_value($S, Fields), []};
        Severity ->
            {Severity, [{severity, proplists:get_value($S, Fields)}]}
    end,
    Error = #error{
      severity = lower_atom(ErrSeverity),
      code     = ErrCode,
      codename = ErrName,
      message  = proplists:get_value($M, Fields),
      extra    = lists:sort(Extra ++ lists:foldl(fun decode_error_extra/2, [], Fields))},
    Error.

%% consider updating #error.extra typespec when changing/adding extras
decode_error_extra({$D, Val}, Acc) ->
    [{detail, Val} | Acc];
decode_error_extra({$H, Val}, Acc) ->
    [{hint, Val} | Acc];
decode_error_extra({$P, Val}, Acc) ->
    [{position, Val} | Acc];
decode_error_extra({$p, Val}, Acc) ->
    [{internal_position, Val} | Acc];
decode_error_extra({$q, Val}, Acc) ->
    [{internal_query, Val} | Acc];
decode_error_extra({$W, Val}, Acc) ->
    [{where, Val} | Acc];
decode_error_extra({$s, Val}, Acc) ->
    [{schema_name, Val} | Acc];
decode_error_extra({$t, Val}, Acc) ->
    [{table_name, Val} | Acc];
decode_error_extra({$c, Val}, Acc) ->
    [{column_name, Val} | Acc];
decode_error_extra({$d, Val}, Acc) ->
    [{data_type_name, Val} | Acc];
decode_error_extra({$n, Val}, Acc) ->
    [{constraint_name, Val} | Acc];
decode_error_extra({$F, Val}, Acc) ->
    [{file, Val} | Acc];
decode_error_extra({$L, Val}, Acc) ->
    [{line, Val} | Acc];
decode_error_extra({$R, Val}, Acc) ->
    [{routine, Val} | Acc];
decode_error_extra({_, _}, Acc) ->
    Acc.

lower_atom(Str) when is_binary(Str) ->
    lower_atom(binary_to_list(Str));
lower_atom(Str) when is_list(Str) ->
    list_to_atom(string:to_lower(Str)).


%% @doc Build decoder for DataRow
-spec build_decoder([epgsql:column()], epgsql_binary:codec()) -> row_decoder().
build_decoder(Columns, Codec) ->
    Decoders = lists:map(
                 fun(#column{oid = Oid, format = Format}) ->
                         Fmt = case Format of
                                   1 -> binary;
                                   0 -> text
                               end,
                         epgsql_binary:oid_to_decoder(Oid, Fmt, Codec)
                 end, Columns),
    {Decoders, Columns, Codec}.

%% @doc decode row data
-spec decode_data(binary(), row_decoder()) -> tuple().
decode_data(Bin, {Decoders, _Columns, Codec}) ->
    list_to_tuple(decode_data(Bin, Decoders, Codec)).

decode_data(_, [], _) -> [];
decode_data(<<-1:?int32, Rest/binary>>, [_Dec | Decs], Codec) ->
    [epgsql_binary:null(Codec) | decode_data(Rest, Decs, Codec)];
decode_data(<<Len:?int32, Value:Len/binary, Rest/binary>>, [Decoder | Decs], Codec) ->
    [epgsql_binary:decode(Value, Decoder)
     | decode_data(Rest, Decs, Codec)].

%% @doc decode RowDescription column information
-spec decode_columns(non_neg_integer(), binary(), epgsql_binary:codec()) -> [epgsql:column()].
decode_columns(0, _Bin, _Codec) -> [];
decode_columns(Count, Bin, Codec) ->
    [Name, Rest] = decode_string(Bin),
    <<TableOid:?int32, AttribNum:?int16, TypeOid:?int32,
      Size:?int16, Modifier:?int32, Format:?int16, Rest2/binary>> = Rest,
    %% TODO: get rid of this 'type' (extra oid_db lookup)
    Type = epgsql_binary:oid_to_name(TypeOid, Codec),
    Desc = #column{
      name     = Name,
      type     = Type,
      oid      = TypeOid,
      size     = Size,
      modifier = Modifier,
      format   = Format,
      table_oid = TableOid,
      table_attr_number = AttribNum},
    [Desc | decode_columns(Count - 1, Rest2, Codec)].

%% @doc decode ParameterDescription
-spec decode_parameters(binary(), epgsql_binary:codec()) ->
                               [epgsql_oid_db:type_info() | {unknown_oid, epgsql_oid_db:oid()}].
decode_parameters(<<_Count:?int16, Bin/binary>>, Codec) ->
    [case epgsql_binary:oid_to_info(Oid, Codec)  of
         undefined -> {unknown_oid, Oid};
         TypeInfo -> TypeInfo
     end || <<Oid:?int32>> <= Bin].

%% @doc decode CcommandComplete msg
decode_complete(<<"SELECT", 0>>)        -> select;
decode_complete(<<"SELECT", _/binary>>) -> select;
decode_complete(<<"BEGIN", 0>>)         -> 'begin';
decode_complete(<<"ROLLBACK", 0>>)      -> rollback;
decode_complete(Bin) ->
    [Str, _] = decode_string(Bin),
    case string:tokens(binary_to_list(Str), " ") of
        ["INSERT", _Oid, Rows] -> {insert, list_to_integer(Rows)};
        ["UPDATE", Rows]       -> {update, list_to_integer(Rows)};
        ["DELETE", Rows]       -> {delete, list_to_integer(Rows)};
        ["MOVE", Rows]         -> {move, list_to_integer(Rows)};
        ["FETCH", Rows]        -> {fetch, list_to_integer(Rows)};
        ["COPY", Rows]         -> {copy, list_to_integer(Rows)};
        [Type | _Rest]         -> lower_atom(Type)
    end.


%% @doc encode types
encode_types(Types, Codec) ->
    encode_types(Types, 0, <<>>, Codec).

encode_types([], Count, Acc, _Codec) ->
    <<Count:?int16, Acc/binary>>;

encode_types([Type | T], Count, Acc, Codec) ->
    Oid = case Type of
              undefined -> 0;
              _Any -> epgsql_binary:type_to_oid(Type, Codec)
    end,
    encode_types(T, Count + 1, <<Acc/binary, Oid:?int32>>, Codec).

%% @doc encode expected column formats
-spec encode_formats([epgsql:column()]) -> binary().
encode_formats(Columns) ->
    encode_formats(Columns, 0, <<>>).

encode_formats([], Count, Acc) ->
    <<Count:?int16, Acc/binary>>;

encode_formats([#column{format = Format} | T], Count, Acc) ->
    encode_formats(T, Count + 1, <<Acc/binary, Format:?int16>>).

%% @doc Returns 1 if Codec knows how to decode binary format of the type provided and 0 otherwise
format({unknown_oid, _}, _) -> 0;
format(#column{oid = Oid}, Codec) ->
    case epgsql_binary:supports(Oid, Codec) of
        true  -> 1;                             %binary
        false -> 0                              %text
    end.

%% @doc encode parameters for 'Bind'
-spec encode_parameters([{epgsql:epgsql_type(), epgsql:bind_param()}],
                        epgsql_binary:codec()) -> iolist().
encode_parameters(Parameters, Codec) ->
    encode_parameters(Parameters, 0, <<>>, [], Codec).

encode_parameters([], Count, Formats, Values, _Codec) ->
    [<<Count:?int16>>, Formats, <<Count:?int16>> | lists:reverse(Values)];

encode_parameters([P | T], Count, Formats, Values, Codec) ->
    {Format, Value} = encode_parameter(P, Codec),
    Formats2 = <<Formats/binary, Format:?int16>>,
    Values2 = [Value | Values],
    encode_parameters(T, Count + 1, Formats2, Values2, Codec).

%% @doc encode single 'typed' parameter
-spec encode_parameter({Type, Val :: any()},
                       epgsql_binary:codec()) -> {0..1, iolist()} when
      Type :: epgsql:type_name()
            | {array, epgsql:type_name()}
            | {unknown_oid, epgsql_oid_db:oid()}.
encode_parameter({Type, Value}, Codec) ->
    case epgsql_binary:is_null(Value, Codec) of
        false ->
            encode_parameter(Type, Value, Codec);
        true ->
            {1, <<-1:?int32>>}
    end.

encode_parameter({unknown_oid, _Oid}, Value, _Codec) ->
    {0, encode_text(Value)};
encode_parameter(Type, Value, Codec) ->
    {1, epgsql_binary:encode(Type, Value, Codec)}.

encode_text(B) when is_binary(B)  -> encode_bin(B);
encode_text(A) when is_atom(A)    -> encode_bin(atom_to_binary(A, utf8));
encode_text(I) when is_integer(I) -> encode_bin(integer_to_binary(I));
encode_text(F) when is_float(F)   -> encode_bin(float_to_binary(F));
encode_text(L) when is_list(L)    -> encode_bin(list_to_binary(L)).

encode_bin(Bin) ->
    <<(byte_size(Bin)):?int32, Bin/binary>>.

%% @doc Encode iodata with size-prefix (used for `StartupMessage' and `SSLRequest' packets)
encode_command(Data) ->
    Size = iolist_size(Data),
    [<<(Size + 4):?int32>> | Data].

%% @doc Encode PG command with type and size prefix
-spec encode_command(packet_type(), iodata()) -> iodata().
encode_command(Type, Data) ->
    Size = iolist_size(Data),
    [<<Type:8, (Size + 4):?int32>> | Data].

%% @doc encode replication status message
encode_standby_status_update(ReceivedLSN, FlushedLSN, AppliedLSN) ->
    {MegaSecs, Secs, MicroSecs} = os:timestamp(),
    %% microseconds since midnight on 2000-01-01
    Timestamp = ((MegaSecs * 1000000 + Secs) * 1000000 + MicroSecs) - 946684800*1000000,
    <<$r:8, ReceivedLSN:?int64, FlushedLSN:?int64, AppliedLSN:?int64, Timestamp:?int64, 0:8>>.

%% @doc encode binary copy data file header
%%
%% See [https://www.postgresql.org/docs/current/sql-copy.html#id-1.9.3.55.9.4.5]
encode_copy_header() ->
    <<
      "PGCOPY\n", 8#377, "\r\n", 0,             % "signature"
      0:?int32,                                 % flags
      0:?int32                                  % length of the extensions area
    >>.

%% @doc encode binary copy data file row / tuple
%%
%% See [https://www.postgresql.org/docs/current/sql-copy.html#id-1.9.3.55.9.4.6]
encode_copy_row(ValuesTuple, Types, Codec) when is_tuple(ValuesTuple) ->
    encode_copy_row(tuple_to_list(ValuesTuple), Types, Codec);
encode_copy_row(Values, Types, Codec) ->
    NumCols = length(Types),
    [<<NumCols:?int16>>
    | lists:zipwith(
        fun(Type, Value) ->
                case epgsql_binary:is_null(Value, Codec) of
                    true ->
                        <<-1:?int32>>;
                    false ->
                        epgsql_binary:encode(Type, Value, Codec)
                end
        end, Types, Values)
    ].

%% @doc encode binary copy data file header
%%
%% See [https://www.postgresql.org/docs/current/sql-copy.html#id-1.9.3.55.9.4.7]
encode_copy_trailer() ->
    <<-1:?int16>>.

%%
%% Encoders for various PostgreSQL protocol client-side packets
%% See https://www.postgresql.org/docs/current/protocol-message-formats.html
%%

%% @doc encodes simple 'Query' packet.
encode_query(SQL) ->
    {?SIMPLEQUERY, encode_string(SQL)}.

%% @doc encodes 'Parse' packet.
%%
%% Results in `ParseComplete' response.
%%
%% @param ColumnEncoding see {@link encode_types/2}
-spec encode_parse(iodata(), iodata(), iodata()) -> {packet_type(), iodata()}.
encode_parse(Name, SQL, ColumnEncoding) ->
    {?PARSE, [encode_string(Name), encode_string(SQL), ColumnEncoding]}.

%% @doc encodes `Describe' packet.
%%
%% @param What might be `?PORTAL' (results in `RowDescription' response) or `?PREPARED_STATEMENT'
%%   (results in `ParameterDescription' followed by `RowDescription' or `NoData' response)
-spec encode_describe(byte() | statement | portal, iodata()) ->
          {packet_type(), iodata()}.
encode_describe(What, Name) when What =:= ?PREPARED_STATEMENT;
                                 What =:= ?PORTAL ->
    {?DESCRIBE, [What, encode_string(Name)]};
encode_describe(What, Name) when is_atom(What) ->
    encode_describe(obj_atom_to_byte(What), Name).

%% @doc encodes `Bind' packet.
%%
%% @param BinParams see {@link encode_parameters/2}.
%% @param BinFormats  see {@link encode_formats/1}
-spec encode_bind(iodata(), iodata(), iodata(), iodata()) -> {packet_type(), iodata()}.
encode_bind(PortalName, StmtName, BinParams, BinFormats) ->
    {?BIND, [encode_string(PortalName), encode_string(StmtName), BinParams, BinFormats]}.

%% @doc encodes `Execute' packet.
%%
%% Results in 0 or up to `MaxRows' packets of `DataRow' type followed by `CommandComplete' (when no
%% more rows are available) or `PortalSuspend' (repeated `Execute' will return more rows)
%%
%% @param PortalName  might be an empty string (anonymous portal) or name of the named portal
%% @param MaxRows  how many rows server should send (0 means all of them)
-spec encode_execute(iodata(), non_neg_integer()) -> {packet_type(), iodata()}.
encode_execute("", 0) ->
    %% optimization: literal for most common case
    {?EXECUTE, [0, <<0:?int32>>]};
encode_execute(PortalName, MaxRows) ->
    {?EXECUTE, [encode_string(PortalName), <<MaxRows:?int32>>]}.

%% @doc encodes `Close' packet.
%%
%% Results in `CloseComplete' response
%%
%% @param What see {@link encode_describe/2}
-spec encode_close(byte() | statement | portal, iodata()) ->
          {packet_type(), iodata()}.
encode_close(What, Name) when What =:= ?PREPARED_STATEMENT;
                              What =:= ?PORTAL ->
    {?CLOSE, [What, encode_string(Name)]};
encode_close(What, Name) when is_atom(What) ->
    encode_close(obj_atom_to_byte(What), Name).

%% @doc encodes `Flush' packet.
%%
%% It doesn't cause any specific response packet, but tells PostgreSQL server to flush it's send
%% network buffers
-spec encode_flush() -> {packet_type(), iodata()}.
encode_flush() ->
    {?FLUSH, []}.

%% @doc encodes `Sync' packet.
%%
%% Results in `ReadyForQuery' response
-spec encode_sync() -> {packet_type(), iodata()}.
encode_sync() ->
    {?SYNC, []}.

%% @doc encodes `CopyDone' packet.
-spec encode_copy_done() -> {packet_type(), iodata()}.
encode_copy_done() ->
    {?COPY_DONE, []}.

obj_atom_to_byte(statement) -> ?PREPARED_STATEMENT;
obj_atom_to_byte(portal) -> ?PORTAL.
