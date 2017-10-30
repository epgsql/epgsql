%%% @doc
%%% Codec for `int2', `int4', `int8' (smallint, integer, bigint).
%%% https://www.postgresql.org/docs/current/static/datatype-numeric.html#datatype-int
%%% $PG$/src/backend/utils/adt/int.c
%%% $PG$/src/backend/utils/adt/int8.c
%%% @end
%%% Created : 12 Oct 2017 by Sergey Prokhorov <me@seriyps.ru>

-module(epgsql_codec_integer).
-behaviour(epgsql_codec).

-export([init/2, names/0, encode/3, decode/3]).

-export_type([data/0]).

%% See table 8.2
%% https://www.postgresql.org/docs/current/static/datatype-numeric.html
-define(BIGINT_MAX, 16#7fffffffffffffff).  % 9223372036854775807, (2^63 - 1)
-define(BIGINT_MIN, -16#7fffffffffffffff). % -9223372036854775807

-type data() :: ?BIGINT_MIN..?BIGINT_MAX.


init(_, _) -> [].

names() ->
    [int2, int4, int8].

encode(N, int2, _) ->
    <<N:1/big-signed-unit:16>>;
encode(N, int4, _) ->
    <<N:1/big-signed-unit:32>>;
encode(N, int8, _) ->
    <<N:1/big-signed-unit:64>>.

decode(<<N:1/big-signed-unit:16>>, int2, _)    -> N;
decode(<<N:1/big-signed-unit:32>>, int4, _)    -> N;
decode(<<N:1/big-signed-unit:64>>, int8, _)    -> N.
