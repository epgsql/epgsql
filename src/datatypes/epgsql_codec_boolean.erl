%%% @doc
%%% Codec for `bool'.
%%%
%%% `unknown' is represented by `null'.
%%% <ul>
%%%  <li>[https://www.postgresql.org/docs/current/static/datatype-boolean.html]</li>
%%%  <li>$PG$/src/backend/utils/adt/bool.c</li>
%%% </ul>
%%% @end
%%% Created : 12 Oct 2017 by Sergey Prokhorov <me@seriyps.ru>

-module(epgsql_codec_boolean).
-behaviour(epgsql_codec).

-export([init/2, names/0, encode/3, decode/3, decode_text/3]).

-export_type([data/0]).

-type data() :: boolean().

init(_, _) -> [].

names() ->
    [bool].

encode(true, bool, _) ->
    <<1:1/big-signed-unit:8>>;
encode(false, bool, _) ->
    <<0:1/big-signed-unit:8>>.

decode(<<1:1/big-signed-unit:8>>, bool, _) -> true;
decode(<<0:1/big-signed-unit:8>>, bool, _) -> false.

decode_text(V, _, _) -> V.
