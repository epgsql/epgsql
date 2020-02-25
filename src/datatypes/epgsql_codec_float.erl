%%% @doc
%%% Codec for `float4', `float8' (real, double precision).
%%%
%%% <ul>
%%%  <li>[https://www.postgresql.org/docs/current/static/datatype-numeric.html#datatype-float]</li>
%%%  <li>$PG$/src/backend/utils/adt/float.c</li>
%%% </ul>
%%% @end
%%% Created : 12 Oct 2017 by Sergey Prokhorov <me@seriyps.ru>

-module(epgsql_codec_float).
-behaviour(epgsql_codec).

-export([init/2, names/0, encode/3, decode/3, decode_text/3]).

-export_type([data/0]).

-type data() :: in_data() | out_data().
-type in_data() :: integer() | float() | nan | plus_infinity | minus_infinity.
-type out_data() :: float() | nan | plus_infinity | minus_infinity.

-define(POS_INF,  <<0:1, 255:8, 0:23>>).
-define(NEG_INF,  <<1:1, 255:8, 0:23>>).
-define(NAN_PATTERN, <<_:1, 255:8, _:23>>).
-define(NAN, <<0:1, 255:8, 1:1, 0:22>>).

-define(POS_INF8, <<0:1, 2047:11, 0:52>>).
-define(NEG_INF8, <<1:1, 2047:11, 0:52>>).
-define(NAN_PATTERN8, <<_:1, 2047:11, _:52>>).
-define(NAN8, <<0:1, 2047:11, 1:1, 0:51>>).

init(_, _) -> [].

names() ->
    [float4, float8].

encode(Int, Type, State) when is_integer(Int) ->
    encode(Int * 1.0, Type, State);
encode(N, float4, _) when is_float(N) ->
    <<N:1/big-float-unit:32>>;
encode(N, float8, _) when is_float(N) ->
    <<N:1/big-float-unit:64>>;
encode(nan, float4, _) -> ?NAN;
encode(nan, float8, _) -> ?NAN8;
encode(plus_infinity, float4, _) -> ?POS_INF;
encode(plus_infinity, float8, _) -> ?POS_INF8;
encode(minus_infinity, float4, _) -> ?NEG_INF;
encode(minus_infinity, float8, _) -> ?NEG_INF8.


decode(<<N:1/big-float-unit:32>>, float4, _) -> N;
decode(<<N:1/big-float-unit:64>>, float8, _) -> N;
decode(?POS_INF, float4, _) -> plus_infinity;
decode(?POS_INF8, float8, _) -> plus_infinity;
decode(?NEG_INF, float4, _) -> minus_infinity;
decode(?NEG_INF8, float8, _) -> minus_infinity;
decode(?NAN_PATTERN, float4, _) -> nan;
decode(?NAN_PATTERN8, float8, _) -> nan.

decode_text(V, _, _) -> V.
