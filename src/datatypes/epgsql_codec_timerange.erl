%%% @doc
%%% Codec for `tsrange', `tstzrange', `daterange' types.
%%%
%%% <ul>
%%%  <li>[https://www.postgresql.org/docs/current/static/rangetypes.html#rangetypes-builtin]</li>
%%%  <li>$PG$/src/backend/utils/adt/rangetypes.c</li>
%%% </ul>
%%% @end
%%% @see epgsql_codec_datetime
%%% @end
%%% Created : 16 Jul 2018 by Vladimir Sekissov <eryx67@gmail.com>
%%% TODO: universal range, based on pg_range table
%%% TODO: inclusive/exclusive ranges `[]' `[)' `(]' `()'

-module(epgsql_codec_timerange).
-behaviour(epgsql_codec).

-export([init/2, names/0, encode/3, decode/3, decode_text/3]).

-include("protocol.hrl").

-export_type([data/0]).

-type data() :: {left(), right()} | empty.

-type left() :: minus_infinity | epgsql_codec_datetime:data().
-type right() :: plus_infinity | epgsql_codec_datetime:data().

init(_, Sock) ->
    case epgsql_sock:get_parameter_internal(<<"integer_datetimes">>, Sock) of
        <<"on">>  -> epgsql_idatetime;
        <<"off">> -> epgsql_fdatetime
    end.

names() ->
    [tsrange, tstzrange, daterange].

encode(empty, _T, _CM) ->
    <<1>>;
encode({minus_infinity, plus_infinity}, _T, _CM) ->
    <<24:1/big-signed-unit:8>>;
encode({From, plus_infinity}, Type, EncMod) ->
    FromBin = encode_member(Type, From, EncMod),
    <<18:1/big-signed-unit:8, (byte_size(FromBin)):?int32, FromBin/binary>>;
encode({minus_infinity, To}, Type, EncMod) ->
    ToBin = encode_member(Type, To, EncMod),
    <<8:1/big-signed-unit:8, (byte_size(ToBin)):?int32, ToBin/binary>>;
encode({From, To}, Type, EncMod) ->
    FromBin = encode_member(Type, From, EncMod),
    ToBin = encode_member(Type, To, EncMod),
    <<2:1/big-signed-unit:8,
      (byte_size(FromBin)):?int32, FromBin/binary,
      (byte_size(ToBin)):?int32, ToBin/binary>>.

decode(<<1>>, _, _) ->
    empty;
decode(<<Flag:1/big-signed-unit:8,
         FromLen:?int32, FromBin:FromLen/binary,
         ToLen:?int32, ToBin:ToLen/binary>>,
       Type, EncMod) when Flag =:= 0; Flag =:= 2; Flag =:= 4; Flag =:= 6 -> %% () [) (] []
    {decode_member(Type, FromBin, EncMod), decode_member(Type, ToBin, EncMod)};
decode(<<Flag:1/big-signed-unit:8, ToLen:?int32, ToBin:ToLen/binary>>,
    Type, EncMod) when Flag =:= 8; Flag =:= 12 -> %% (] ()
    {minus_infinity, decode_member(Type, ToBin, EncMod)};
decode(<<Flag:1/big-signed-unit:8, FromLen:?int32, FromBin:FromLen/binary>>,
    Type, EncMod) when Flag =:= 16; Flag =:= 18 -> %% [) ()
    {decode_member(Type, FromBin, EncMod), plus_infinity};
decode(<<24:1/big-signed-unit:8>>, _, _) ->
    {minus_infinity, plus_infinity}.

decode_text(V, _, _) -> V.

encode_member(Type, Val, epgsql_idatetime) ->
    epgsql_idatetime:encode(member_type(Type), Val);
encode_member(Type, Val, epgsql_fdatetime) ->
    epgsql_fdatetime:encode(member_type(Type), Val).

decode_member(Type, Bin, epgsql_idatetime) ->
    epgsql_idatetime:decode(member_type(Type), Bin);
decode_member(Type, Bin, epgsql_fdatetime) ->
    epgsql_fdatetime:decode(member_type(Type), Bin).

member_type(tsrange) -> timestamp;
member_type(tstzrange) -> timestamptz;
member_type(daterange) -> date.
