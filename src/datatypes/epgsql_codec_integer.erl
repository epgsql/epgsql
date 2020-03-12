%%% @doc
%%% Codec for `int2', `int4', `int8' (smallint, integer, bigint).
%%%
%%% <ul>
%%%  <li>[https://www.postgresql.org/docs/current/static/datatype-numeric.html#datatype-int]</li>
%%%  <li>$PG$/src/backend/utils/adt/int.c</li>
%%%  <li>$PG$/src/backend/utils/adt/int8.c</li>
%%% </ul>
%%% @end
%%% Created : 12 Oct 2017 by Sergey Prokhorov <me@seriyps.ru>

-module(epgsql_codec_integer).
-behaviour(epgsql_codec).

-export([init/2, names/0, encode/3, decode/3, decode_text/3]).
-export([check_overflow_small/1, check_overflow_int/1, check_overflow_big/1]).

-export_type([data/0]).

%% See table 8.2
%% https://www.postgresql.org/docs/current/static/datatype-numeric.html
-define(SMALLINT_MAX, 16#7fff).  % 32767, (2^15 - 1)
-define(SMALLINT_MIN, -16#8000). % -32768
-define(INT_MAX, 16#7fffffff).  % 2147483647, (2^31 - 1)
-define(INT_MIN, -16#80000000). % -2147483648
-define(BIGINT_MAX, 16#7fffffffffffffff).  % 9223372036854775807, (2^63 - 1)
-define(BIGINT_MIN, -16#8000000000000000). % -9223372036854775808

-type data() :: ?BIGINT_MIN..?BIGINT_MAX.

check_overflow_small(N) when N >= ?SMALLINT_MIN, N =< ?SMALLINT_MAX -> ok;
check_overflow_small(N) ->
    overflow(N, int2).

check_overflow_int(N) when N >= ?INT_MIN, N =< ?INT_MAX -> ok;
check_overflow_int(N) ->
    overflow(N, int4).

check_overflow_big(N) when N >= ?BIGINT_MIN, N =< ?BIGINT_MAX -> ok;
check_overflow_big(N) ->
    overflow(N, int8).

overflow(N, Type) ->
    error({integer_overflow, Type, N}).

init(_, _) -> [].

names() ->
    [int2, int4, int8].

encode(N, int2, _) ->
    check_overflow_small(N),
    <<N:1/big-signed-unit:16>>;
encode(N, int4, _) ->
    check_overflow_int(N),
    <<N:1/big-signed-unit:32>>;
encode(N, int8, _) ->
    check_overflow_big(N),
    <<N:1/big-signed-unit:64>>.

decode(<<N:1/big-signed-unit:16>>, int2, _)    -> N;
decode(<<N:1/big-signed-unit:32>>, int4, _)    -> N;
decode(<<N:1/big-signed-unit:64>>, int8, _)    -> N.

decode_text(V, _, _) -> V.
