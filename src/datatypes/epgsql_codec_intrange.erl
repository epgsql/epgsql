%%% @doc
%%% Codec for `int4range', `int8range' types.
%%%
%%% <ul>
%%%   <li>[https://www.postgresql.org/docs/current/static/rangetypes.html#rangetypes-builtin]</li>
%%%   <li>$PG$/src/backend/utils/adt/rangetypes.c</li>
%%% </ul>
%%% @end
%%% @see epgsql_codec_integer
%%% @end
%%% Created : 14 Oct 2017 by Sergey Prokhorov <me@seriyps.ru>
%%% TODO: universal range, based on pg_range table
%%% TODO: inclusive/exclusive ranges `[]' `[)' `(]' `()'

-module(epgsql_codec_intrange).
-behaviour(epgsql_codec).

-export([init/2, names/0, encode/3, decode/3, decode_text/3]).

-include("protocol.hrl").

-export_type([data/0]).

-type data() :: {left(), right()} | empty.

-type left() :: minus_infinity | integer().
-type right() :: plus_infinity | integer().


init(_, _) -> [].

names() ->
    [int4range, int8range].

encode(empty, _, _) ->
    <<1>>;
encode(Range, int4range, _) ->
    encode_int4range(Range);
encode(Range, int8range, _) ->
    encode_int8range(Range).

decode(<<1>>, _, _) ->
    empty;
decode(Bin, int4range, _) ->
    decode_int4range(Bin);
decode(Bin, int8range, _) ->
    decode_int8range(Bin).


encode_int4range({minus_infinity, plus_infinity}) ->
    <<24:1/big-signed-unit:8>>;
encode_int4range({From, plus_infinity}) ->
    FromInt = to_int(From),
    epgsql_codec_integer:check_overflow_int(FromInt),
    <<18:1/big-signed-unit:8, 4:?int32, FromInt:?int32>>;
encode_int4range({minus_infinity, To}) ->
    ToInt = to_int(To),
    epgsql_codec_integer:check_overflow_int(ToInt),
    <<8:1/big-signed-unit:8, 4:?int32, ToInt:?int32>>;
encode_int4range({From, To}) ->
    FromInt = to_int(From),
    ToInt = to_int(To),
    epgsql_codec_integer:check_overflow_int(FromInt),
    epgsql_codec_integer:check_overflow_int(ToInt),
    <<2:1/big-signed-unit:8, 4:?int32, FromInt:?int32, 4:?int32, ToInt:?int32>>.

encode_int8range({minus_infinity, plus_infinity}) ->
    <<24:1/big-signed-unit:8>>;
encode_int8range({From, plus_infinity}) ->
    FromInt = to_int(From),
    epgsql_codec_integer:check_overflow_big(FromInt),
    <<18:1/big-signed-unit:8, 8:?int32, FromInt:?int64>>;
encode_int8range({minus_infinity, To}) ->
    ToInt = to_int(To),
    epgsql_codec_integer:check_overflow_big(ToInt),
    <<8:1/big-signed-unit:8, 8:?int32, ToInt:?int64>>;
encode_int8range({From, To}) ->
    FromInt = to_int(From),
    ToInt = to_int(To),
    epgsql_codec_integer:check_overflow_big(FromInt),
    epgsql_codec_integer:check_overflow_big(ToInt),
    <<2:1/big-signed-unit:8, 8:?int32, FromInt:?int64, 8:?int32, ToInt:?int64>>.

to_int(N) when is_integer(N) -> N;
to_int(S) when is_list(S) -> erlang:list_to_integer(S);
to_int(B) when is_binary(B) -> erlang:binary_to_integer(B).


decode_int4range(<<2:1/big-signed-unit:8, 4:?int32, From:?int32, 4:?int32, To:?int32>>) ->
    {From, To};
decode_int4range(<<8:1/big-signed-unit:8, 4:?int32, To:?int32>>) ->
    {minus_infinity, To};
decode_int4range(<<18:1/big-signed-unit:8, 4:?int32, From:?int32>>) ->
    {From, plus_infinity};
decode_int4range(<<24:1/big-signed-unit:8>>) ->
    {minus_infinity, plus_infinity}.

decode_int8range(<<2:1/big-signed-unit:8, 8:?int32, From:?int64, 8:?int32, To:?int64>>) ->
    {From, To};
decode_int8range(<<8:1/big-signed-unit:8, 8:?int32, To:?int64>>) ->
    {minus_infinity, To};
decode_int8range(<<18:1/big-signed-unit:8, 8:?int32, From:?int64>>) ->
    {From, plus_infinity};
decode_int8range(<<24:1/big-signed-unit:8>>) ->
    {minus_infinity, plus_infinity}.

decode_text(V, _, _) -> V.
