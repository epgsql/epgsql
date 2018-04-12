%%% @doc
%%% Codec for `json', `jsonb'
%%% https://www.postgresql.org/docs/current/static/datatype-json.html
%%% $PG$/src/backend/utils/adt/json.c // `json'
%%% $PG$/src/backend/utils/adt/jsonb.c // `jsonb'
%%% @end
%%% Created : 12 Oct 2017 by Sergey Prokhorov <me@seriyps.ru>

-module(epgsql_codec_json).
-behaviour(epgsql_codec).

-export([init/2, names/0, encode/3, decode/3, decode_text/3]).

-export_type([data/0]).

-type data() :: binary().

-define(JSONB_VERSION_1, 1).

%% TODO: JSON encode/decode `fun Mod:Name/1` / `{Mod, Name}` as option.
%% Shall not pass `fun(_) -> .. end`, because of hot code upgrade problems.
init(_, _) -> [].

names() ->
    [json, jsonb].

encode(Bin, json, _) ->
    Bin;
encode(Bin, jsonb, _) ->
    [<<?JSONB_VERSION_1:8>> | Bin].

decode(Bin, json, _) ->
    Bin;
decode(<<?JSONB_VERSION_1:8, Bin/binary>>, jsonb, _) ->
    Bin.

decode_text(V, _, _) -> V.
