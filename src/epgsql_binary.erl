%%% Copyright (C) 2008 - Will Glozer.  All rights reserved.

-module(epgsql_binary).

-export([new_codec/1,
         update_type_cache/2,
         type2oid/2, oid2type/2,
         encode/3, decode/3, supports/1]).

-record(codec, {
    type2oid = [],
    oid2type = []
}).

-include("epgsql_binary.hrl").

-define(datetime, (get(datetime_mod))).

new_codec([]) -> #codec{}.

update_type_cache(TypeInfos, Codec) ->
    Type2Oid = lists:flatmap(
        fun({NameBin, ElementOid, ArrayOid}) ->
            Name = erlang:binary_to_atom(NameBin, utf8),
            [{Name, ElementOid}, {{array, Name}, ArrayOid}]
        end,
        TypeInfos),
    Oid2Type = [{Oid, Type} || {Type, Oid} <- Type2Oid],
    Codec#codec{type2oid = Type2Oid, oid2type = Oid2Type}.

oid2type(Oid, #codec{oid2type = Oid2Type}) ->
    case epgsql_types:oid2type(Oid) of
        {unknown_oid, _} ->
            proplists:get_value(Oid, Oid2Type, {unknown_oid, Oid});
        Type -> Type
    end.

type2oid(Type, #codec{type2oid = Type2Oid}) ->
    case epgsql_types:type2oid(Type) of
        {unknown_type, _} ->
            proplists:get_value(Type, Type2Oid, {unknown_type, Type});
        Oid -> Oid
    end.

encode(_Any, null, _)                       -> <<-1:?int32>>;
encode(bool, true, _)                       -> <<1:?int32, 1:1/big-signed-unit:8>>;
encode(bool, false, _)                      -> <<1:?int32, 0:1/big-signed-unit:8>>;
encode(int2, N, _)                          -> <<2:?int32, N:1/big-signed-unit:16>>;
encode(int4, N, _)                          -> <<4:?int32, N:1/big-signed-unit:32>>;
encode(int8, N, _)                          -> <<8:?int32, N:1/big-signed-unit:64>>;
encode(float4, N, _)                        -> <<4:?int32, N:1/big-float-unit:32>>;
encode(float8, N, _)                        -> <<8:?int32, N:1/big-float-unit:64>>;
encode(bpchar, C, _) when is_integer(C)     -> <<1:?int32, C:1/big-unsigned-unit:8>>;
encode(bpchar, B, _) when is_binary(B)      -> <<(byte_size(B)):?int32, B/binary>>;
encode(time = Type, B, _)                   -> ?datetime:encode(Type, B);
encode(timetz = Type, B, _)                 -> ?datetime:encode(Type, B);
encode(date = Type, B, _)                   -> ?datetime:encode(Type, B);
encode(timestamp = Type, B, _)              -> ?datetime:encode(Type, B);
encode(timestamptz = Type, B, _)            -> ?datetime:encode(Type, B);
encode(interval = Type, B, _)               -> ?datetime:encode(Type, B);
encode(bytea, B, _) when is_binary(B)       -> <<(byte_size(B)):?int32, B/binary>>;
encode(text, B, _) when is_binary(B)        -> <<(byte_size(B)):?int32, B/binary>>;
encode(varchar, B, _) when is_binary(B)     -> <<(byte_size(B)):?int32, B/binary>>;
encode(uuid, B, _) when is_binary(B)        -> encode_uuid(B);
encode({array, char}, L, Codec) when is_list(L) -> encode_array(bpchar, type2oid(bpchar, Codec), L, Codec);
encode({array, Type}, L, Codec) when is_list(L) -> encode_array(Type, type2oid(Type, Codec), L, Codec);
encode(hstore, {L}, _) when is_list(L)      -> encode_hstore(L);
encode(point, {X,Y}, _)                     -> encode_point({X,Y});
encode(Type, L, Codec) when is_list(L)      -> encode(Type, list_to_binary(L), Codec);
encode(_Type, _Value, _)                    -> {error, unsupported}.

decode(bool, <<1:1/big-signed-unit:8>>, _)     -> true;
decode(bool, <<0:1/big-signed-unit:8>>, _)     -> false;
decode(bpchar, <<C:1/big-unsigned-unit:8>>, _) -> C;
decode(int2, <<N:1/big-signed-unit:16>>, _)    -> N;
decode(int4, <<N:1/big-signed-unit:32>>, _)    -> N;
decode(int8, <<N:1/big-signed-unit:64>>, _)    -> N;
decode(float4, <<N:1/big-float-unit:32>>, _)   -> N;
decode(float8, <<N:1/big-float-unit:64>>, _)   -> N;
decode(record, <<_:?int32, Rest/binary>>, Codec) -> list_to_tuple(decode_record(Rest, [], Codec));
decode(time = Type, B, _)                      -> ?datetime:decode(Type, B);
decode(timetz = Type, B, _)                    -> ?datetime:decode(Type, B);
decode(date = Type, B, _)                      -> ?datetime:decode(Type, B);
decode(timestamp = Type, B, _)                 -> ?datetime:decode(Type, B);
decode(timestamptz = Type, B, _)               -> ?datetime:decode(Type, B);
decode(interval = Type, B, _)                  -> ?datetime:decode(Type, B);
decode(uuid, B, _)                             -> decode_uuid(B);
decode(hstore, Hstore, _)                      -> decode_hstore(Hstore);
decode({array, _Type}, B, Codec)               -> decode_array(B, Codec);
decode(point, B, _)                            -> decode_point(B);
decode(_Other, Bin, _)                         -> Bin.

encode_array(Type, Oid, A, Codec) ->
    {Data, {NDims, Lengths}} = encode_array(Type, A, 0, [], Codec),
    Lens = [<<N:?int32, 1:?int32>> || N <- lists:reverse(Lengths)],
    Hdr  = <<NDims:?int32, 0:?int32, Oid:?int32>>,
    Bin  = iolist_to_binary([Hdr, Lens, Data]),
    <<(byte_size(Bin)):?int32, Bin/binary>>.

encode_array(_Type, [], NDims, Lengths, _Codec) ->
    {<<>>, {NDims, Lengths}};
encode_array(Type, [H | _] = Array, NDims, Lengths, Codec) when not is_list(H) ->
    F = fun(E, Len) -> {encode(Type, E, Codec), Len + 1} end,
    {Data, Len} = lists:mapfoldl(F, 0, Array),
    {Data, {NDims + 1, [Len | Lengths]}};
encode_array(uuid, [_H | _] = Array, NDims, Lengths, Codec) ->
    F = fun(E, Len) -> {encode(uuid, E, Codec), Len + 1} end,
    {Data, Len} = lists:mapfoldl(F, 0, Array),
    {Data, {NDims + 1, [Len | Lengths]}};
encode_array(Type, Array, NDims, Lengths, Codec) ->
    Lengths2 = [length(Array) | Lengths],
    F = fun(A2, {_NDims, _Lengths}) -> encode_array(Type, A2, NDims, Lengths2, Codec) end,
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

decode_array(<<NDims:?int32, _HasNull:?int32, Oid:?int32, Rest/binary>>, Codec) ->
    {Dims, Data} = erlang:split_binary(Rest, NDims * 2 * 4),
    Lengths = [Len || <<Len:?int32, _LBound:?int32>> <= Dims],
    Type = oid2type(Oid, Codec),
    {Array, <<>>} = decode_array(Data, Type, Lengths, Codec),
    Array.

decode_array(Data, _Type, [], _Codec)  ->
    {[], Data};
decode_array(Data, Type, [Len], Codec) ->
    decode_elements(Data, Type, [], Len, Codec);
decode_array(Data, Type, [Len | T], Codec) ->
    F = fun(_N, Rest) -> decode_array(Rest, Type, T, Codec) end,
    lists:mapfoldl(F, Data, lists:seq(1, Len)).

decode_elements(Rest, _Type, Acc, 0, _Codec) ->
    {lists:reverse(Acc), Rest};
decode_elements(<<-1:?int32, Rest/binary>>, Type, Acc, N, Codec) ->
    decode_elements(Rest, Type, [null | Acc], N - 1, Codec);
decode_elements(<<Len:?int32, Value:Len/binary, Rest/binary>>, Type, Acc, N, Codec) ->
    Value2 = decode(Type, Value, Codec),
    decode_elements(Rest, Type, [Value2 | Acc], N - 1, Codec).

decode_record(<<>>, Acc, _Codec) ->
    lists:reverse(Acc);
decode_record(<<_Type:?int32, -1:?int32, Rest/binary>>, Acc, Codec) ->
    decode_record(Rest, [null | Acc], Codec);
decode_record(<<Type:?int32, Len:?int32, Value:Len/binary, Rest/binary>>, Acc, Codec) ->
    Value2 = decode(oid2type(Type, Codec), Value, Codec),
    decode_record(Rest, [Value2 | Acc], Codec).

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

encode_point({X, Y}) when is_number(X), is_number(Y) ->
    <<X:1/big-float-unit:64, Y:1/big-float-unit:64>>.

decode_point(<<X:1/big-float-unit:64, Y:1/big-float-unit:64>>) ->
    {X, Y}.

supports(point)   -> true;
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
