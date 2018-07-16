%%% @doc
%%% Codec for `tsrange', `tstzrange', `daterange' types.
%%% https://www.postgresql.org/docs/current/static/rangetypes.html#rangetypes-builtin
%%% $PG$/src/backend/utils/adt/rangetypes.c
%%% @end
%%% Created : 16 Jul 2018 by Vladimir Sekissov <eryx67@gmail.com>
%%% TODO: universal range, based on pg_range table
%%% TODO: inclusive/exclusive ranges `[]' `[)' `(]' `()'

-module(epgsql_codec_timerange).
-behaviour(epgsql_codec).

-export([init/2, names/0, encode/3, decode/3, decode_text/3]).

-include("protocol.hrl").

-export_type([data/0]).

-type data() :: {epgsql_codec_datetime:data(), epgsql_codec_datetime:data()} | empty.

init(_, Sock) ->
    case epgsql_sock:get_parameter_internal(<<"integer_datetimes">>, Sock) of
        <<"on">>  -> epgsql_idatetime;
        <<"off">> -> epgsql_fdatetime
    end.

names() ->
    [tsrange, tstzrange, daterange].

encode(empty, _T, _CM) ->
    <<1>>;
encode({From, To}, Type, CoderMod) ->
    FromBin = CoderMod:encode(member_type(Type), From),
    ToBin = CoderMod:encode(member_type(Type), To),
    <<2:1/big-signed-unit:8,
      (byte_size(FromBin)):?int32, FromBin/binary,
      (byte_size(ToBin)):?int32, ToBin/binary>>.

decode(<<1>>, _, _) ->
    empty;
decode(<<2:1/big-signed-unit:8,
         FromLen:?int32, FromBin:FromLen/binary,
         ToLen:?int32, ToBin:ToLen/binary>>,
       Type, CoderMod) ->
    {CoderMod:decode(member_type(Type), FromBin), CoderMod:decode(member_type(Type), ToBin)}.

decode_text(V, _, _) -> V.

member_type(tsrange) -> timestamp;
member_type(tstzrange) -> timestamptz;
member_type(daterange) -> date.
