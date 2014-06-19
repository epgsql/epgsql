%%% Copyright (C) 2008 - Will Glozer.  All rights reserved.

-module(pgsql_binary).

-export([encode/2, decode/2, supports/1]).

-include("pgsql_binary.hrl").

-define(datetime, (get(datetime_mod))).

encode(_Any, null)                          -> <<-1:?int32>>;
encode(bool, true)                          -> <<1:?int32, 1:1/big-signed-unit:8>>;
encode(bool, false)                         -> <<1:?int32, 0:1/big-signed-unit:8>>;
encode(int2, N)                             -> <<2:?int32, N:1/big-signed-unit:16>>;
encode(int4, N)                             -> <<4:?int32, N:1/big-signed-unit:32>>;
encode(int8, N)                             -> <<8:?int32, N:1/big-signed-unit:64>>;
encode(float4, N)                           -> <<4:?int32, N:1/big-float-unit:32>>;
encode(float8, N)                           -> <<8:?int32, N:1/big-float-unit:64>>;
encode(bpchar, C) when is_integer(C)        -> <<1:?int32, C:1/big-unsigned-unit:8>>;
encode(bpchar, B) when is_binary(B)         -> <<(byte_size(B)):?int32, B/binary>>;
encode(time = Type, B)                      -> ?datetime:encode(Type, B);
encode(timetz = Type, B)                    -> ?datetime:encode(Type, B);
encode(date = Type, B)                      -> ?datetime:encode(Type, B);
encode(timestamp = Type, B)                 -> ?datetime:encode(Type, B);
encode(timestamptz = Type, B)               -> ?datetime:encode(Type, B);
encode(interval = Type, B)                  -> ?datetime:encode(Type, B);
encode(bytea, B) when is_binary(B)          -> <<(byte_size(B)):?int32, B/binary>>;
encode(text, B) when is_binary(B)           -> <<(byte_size(B)):?int32, B/binary>>;
encode(varchar, B) when is_binary(B)        -> <<(byte_size(B)):?int32, B/binary>>;
encode(uuid, B) when is_binary(B)           -> encode_uuid(B);
encode(hstore, {L}) when is_list(L)         -> encode_hstore(L);
encode({array, char}, L) when is_list(L)    -> encode_array(bpchar, L);
encode({array, Type}, L) when is_list(L)    -> encode_array(Type, L);
encode(Type, L) when is_list(L)             -> encode(Type, list_to_binary(L));
encode(_Type, _Value)                       -> {error, unsupported}.

decode(bool, <<1:1/big-signed-unit:8>>)     -> true;
decode(bool, <<0:1/big-signed-unit:8>>)     -> false;
decode(bpchar, <<C:1/big-unsigned-unit:8>>) -> C;
decode(int2, <<N:1/big-signed-unit:16>>)    -> N;
decode(int4, <<N:1/big-signed-unit:32>>)    -> N;
decode(int8, <<N:1/big-signed-unit:64>>)    -> N;
decode(float4, <<N:1/big-float-unit:32>>)   -> N;
decode(float8, <<N:1/big-float-unit:64>>)   -> N;
decode(record, <<_:?int32, Rest/binary>>)   -> list_to_tuple(decode_record(Rest, []));
decode(time = Type, B)                      -> ?datetime:decode(Type, B);
decode(timetz = Type, B)                    -> ?datetime:decode(Type, B);
decode(date = Type, B)                      -> ?datetime:decode(Type, B);
decode(timestamp = Type, B)                 -> ?datetime:decode(Type, B);
decode(timestamptz = Type, B)               -> ?datetime:decode(Type, B);
decode(interval = Type, B)                  -> ?datetime:decode(Type, B);
decode(uuid, B)                             -> decode_uuid(B);
decode(hstore, Hstore)                      -> decode_hstore(Hstore);
decode({array, _Type}, B)                   -> decode_array(B);
decode(_Other, Bin)                         -> Bin.

encode_array(Type, A) ->
    {Data, {NDims, Lengths}} = encode_array(Type, A, 0, []),
    Oid  = pgsql_types:type2oid(Type),
    Lens = [<<N:?int32, 1:?int32>> || N <- lists:reverse(Lengths)],
    Hdr  = <<NDims:?int32, 0:?int32, Oid:?int32>>,
    Bin  = iolist_to_binary([Hdr, Lens, Data]),
    <<(byte_size(Bin)):?int32, Bin/binary>>.

encode_array(_Type, [], NDims, Lengths) ->
    {<<>>, {NDims, Lengths}};
encode_array(Type, [H | _] = Array, NDims, Lengths) when not is_list(H) ->
    F = fun(E, Len) -> {encode(Type, E), Len + 1} end,
    {Data, Len} = lists:mapfoldl(F, 0, Array),
    {Data, {NDims + 1, [Len | Lengths]}};
encode_array(uuid, [_H | _] = Array, NDims, Lengths) ->
    F = fun(E, Len) -> {encode(uuid, E), Len + 1} end,
    {Data, Len} = lists:mapfoldl(F, 0, Array),
    {Data, {NDims + 1, [Len | Lengths]}};
encode_array(Type, Array, NDims, Lengths) ->
    Lengths2 = [length(Array) | Lengths],
    F = fun(A2, {_NDims, _Lengths}) -> encode_array(Type, A2, NDims, Lengths2) end,
    {Data, {NDims2, Lengths3}} = lists:mapfoldl(F, {NDims, Lengths2}, Array),
    {Data, {NDims2 + 1, Lengths3}}.

encode_uuid(U) when is_binary(U) ->
    encode_uuid(binary_to_list(U));
encode_uuid(U) ->
    Hex = [H || H <- U, H =/= $-],
    {ok, [Int], _} = io_lib:fread("~16u", Hex),
    <<16:?int32,Int:128>>.

encode_hstore(HstoreEntries) ->
    Body = << <<(encode_hstore_entry(Entry))/binary>> || Entry <- HstoreEntries >>,
    <<(byte_size(Body) + 4):?int32, (length(HstoreEntries)):?int32, Body/binary>>.

encode_hstore_entry({Key, Value}) ->
    <<(encode_hstore_key(Key))/binary, (encode_hstore_value(Value))/binary>>.

encode_hstore_key(Key) -> encode_hstore_string(Key).

encode_hstore_value(null) -> <<-1:?int32>>;
encode_hstore_value(Val) -> encode_hstore_string(Val).

encode_hstore_string(Str) when is_list(Str) -> encode_hstore_string(list_to_binary(Str));
encode_hstore_string(Str) when is_atom(Str) -> encode_hstore_string(atom_to_binary(Str, utf8));
encode_hstore_string(Str) when is_integer(Str) ->
    encode_hstore_string(erlang:integer_to_binary(Str));
encode_hstore_string(Str) when is_float(Str) ->
    encode_hstore_string(iolist_to_binary(io_lib:format("~w", [Str])));
encode_hstore_string(Str) when is_binary(Str) -> <<(byte_size(Str)):?int32, Str/binary>>.

decode_array(<<NDims:?int32, _HasNull:?int32, Oid:?int32, Rest/binary>>) ->
    {Dims, Data} = erlang:split_binary(Rest, NDims * 2 * 4),
    Lengths = [Len || <<Len:?int32, _LBound:?int32>> <= Dims],
    Type = pgsql_types:oid2type(Oid),
    {Array, <<>>} = decode_array(Data, Type, Lengths),
    Array.

decode_array(Data, _Type, [])  ->
    {[], Data};
decode_array(Data, Type, [Len]) ->
    decode_elements(Data, Type, [], Len);
decode_array(Data, Type, [Len | T]) ->
    F = fun(_N, Rest) -> decode_array(Rest, Type, T) end,
    lists:mapfoldl(F, Data, lists:seq(1, Len)).

decode_elements(Rest, _Type, Acc, 0) ->
    {lists:reverse(Acc), Rest};
decode_elements(<<-1:?int32, Rest/binary>>, Type, Acc, N) ->
    decode_elements(Rest, Type, [null | Acc], N - 1);
decode_elements(<<Len:?int32, Value:Len/binary, Rest/binary>>, Type, Acc, N) ->
    Value2 = decode(Type, Value),
    decode_elements(Rest, Type, [Value2 | Acc], N - 1).

decode_record(<<>>, Acc) ->
    lists:reverse(Acc);
decode_record(<<_Type:?int32, -1:?int32, Rest/binary>>, Acc) ->
    decode_record(Rest, [null | Acc]);
decode_record(<<Type:?int32, Len:?int32, Value:Len/binary, Rest/binary>>, Acc) ->
    Value2 = decode(pgsql_types:oid2type(Type), Value),
    decode_record(Rest, [Value2 | Acc]).

decode_uuid(<<U0:32, U1:16, U2:16, U3:16, U4:48>>) ->
    Format = "~8.16.0b-~4.16.0b-~4.16.0b-~4.16.0b-~12.16.0b",
    iolist_to_binary(io_lib:format(Format, [U0, U1, U2, U3, U4])).

decode_hstore(<<NumElements:?int32, Elements/binary>>) ->
    {decode_hstore1(NumElements, Elements, [])}.

decode_hstore1(0, _Elements, Acc) -> Acc;
decode_hstore1(N, <<KeyLen:?int32, Key:KeyLen/binary, -1:?int32, Rest/binary>>, Acc) ->
    decode_hstore1(N - 1, Rest, [{Key, null} | Acc]);
decode_hstore1(N, <<KeyLen:?int32, Key:KeyLen/binary, ValLen:?int32, Value:ValLen/binary, Rest/binary>>, Acc) ->
    decode_hstore1(N - 1, Rest, [{Key, Value} | Acc]).

supports(bool)    -> true;
supports(bpchar)  -> true;
supports(int2)    -> true;
supports(int4)    -> true;
supports(int8)    -> true;
supports(float4)  -> true;
supports(float8)  -> true;
supports(bytea)   -> true;
supports(text)    -> true;
supports(varchar) -> true;
supports(record)  -> true;
supports(date)    -> true;
supports(time)    -> true;
supports(timetz)  -> true;
supports(timestamp)   -> true;
supports(timestamptz) -> true;
supports(interval)    -> true;
supports(uuid)        -> true;
supports(hstore)      -> true;
supports({array, bool})   -> true;
supports({array, int2})   -> true;
supports({array, int4})   -> true;
supports({array, int8})   -> true;
supports({array, float4}) -> true;
supports({array, float8}) -> true;
supports({array, char})   -> true;
supports({array, text})   -> true;
supports({array, date})   -> true;
supports({array, time})   -> true;
supports({array, timetz}) -> true;
supports({array, timestamp})     -> true;
supports({array, timestamptz})   -> true;
supports({array, interval})      -> true;
supports({array, hstore})        -> true;
supports({array, varchar}) -> true;
supports({array, uuid})   -> true;
supports(_Type)       -> false.
