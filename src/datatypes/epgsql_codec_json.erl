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

%% JsonMod shall either be a module with encode/1 and decode/1, where:
%%
%% - encode/1 expects erlang-formatted JSON, e.g. a map or a proplist,
%%   and returns a valid JSON binary string.
%% - decode/1 expects a valid JSON binary string, and returns erlang-formatted JSON.
%%
%% or a tuple {Mod, EncodeOpts, DecodeOpts} where Mod implements encode/2 and
%% decode/2 with the same interface semantics as encode/1 and decode/1, while
%% accepting Opts as options to pass into the encode/2 and decode/2 functions.
%%
%% Tip: if you require special behavior that a third-party JSON module doesn't provide
%%      for you, or which doesn't strictly meet this interface requirement, wrap the
%%      third-party JSON module in a new module that implements the required interface,
%%      and pass in _that_ as JsonMod.
init(JsonMod, _) ->
    JsonMod.

names() ->
    [json, jsonb].

encode(ErlJson, json, JsonMod) when is_atom(JsonMod) ->
    JsonMod:encode(ErlJson);
encode(ErlJson, json, {JsonMod, EncodeOpts, _}) when is_atom(JsonMod) ->
    JsonMod:encode(ErlJson, EncodeOpts);
encode(ErlJson, jsonb, JsonMod) when is_atom(JsonMod) ->
    <<?JSONB_VERSION_1:8, (JsonMod:encode(ErlJson))/binary>>;
encode(ErlJson, jsonb, {JsonMod, EncodeOpts, _}) when is_atom(JsonMod) ->
    <<?JSONB_VERSION_1:8, (JsonMod:encode(ErlJson, EncodeOpts))/binary>>;
encode(Bin, json, _) ->
    Bin;
encode(Bin, jsonb, _) ->
    [<<?JSONB_VERSION_1:8>> | Bin].

decode(Bin, json, JsonMod) when is_atom(JsonMod) ->
    JsonMod:decode(Bin);
decode(Bin, json, {JsonMod, _, DecodeOpts}) when is_atom(JsonMod) ->
    JsonMod:decode(Bin, DecodeOpts);
decode(<<?JSONB_VERSION_1:8, Bin/binary>>, jsonb, JsonMod) when is_atom(JsonMod) ->
    JsonMod:decode(Bin);
decode(<<?JSONB_VERSION_1:8, Bin/binary>>, jsonb, {JsonMod, _, DecodeOpts}) when is_atom(JsonMod) ->
    JsonMod:decode(Bin, DecodeOpts);
decode(Bin, json, _) ->
    Bin;
decode(<<?JSONB_VERSION_1:8, Bin/binary>>, jsonb, _) ->
    Bin.

decode_text(V, _, _) -> V.
