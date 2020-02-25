%%% @doc
%%% Codec for `json', `jsonb'
%%%
%%% It is possible to instruct the codec to do JSON encoding/decoding to Erlang
%%% terms by providing callback module name, see {@link json_mod()}.
%%% <ul>
%%%   <li>[https://www.postgresql.org/docs/current/static/datatype-json.html]</li>
%%%   <li>$PG$/src/backend/utils/adt/json.c // `json'</li>
%%%   <li>$PG$/src/backend/utils/adt/jsonb.c // `jsonb'</li>
%%% </ul>
%%% @end
%%% Created : 12 Oct 2017 by Sergey Prokhorov <me@seriyps.ru>

-module(epgsql_codec_json).
-behaviour(epgsql_codec).

-export([init/2, names/0, encode/3, decode/3, decode_text/3]).

-export_type([data/0, json_mod/0]).

-type data() :: binary().
-type json_mod()::module() | {module(), EncodeOpts::any(), DecodeOpts::any()}.

-define(JSONB_VERSION_1, 1).

-optional_callbacks([encode/2, decode/2]).

%% Encode erlang-formatted JSON, e.g. a map or a proplist,
%% and return a valid JSON iolist or binary string.
-callback encode(ErlJson::term()) -> EncodedJson::iodata().
-callback encode(ErlJson::term(), EncodeOpts::term()) -> EncodedJson::iodata().

%% Decode JSON binary string into erlang-formatted JSON.
-callback decode(EncodedJson::binary()) -> ErlJson::term().
-callback decode(EncodedJson::binary(), DecodeOpts::term()) -> ErlJson::term().

%% JsonMod shall be a module that implements the callbacks defined by this module;
%% encode/1, decode/1, and optionally the option-accepting variants.
-spec init(JsonMod::json_mod(), epgsql_sock:pg_sock()) -> epgsql_codec:codec_state().
init(JsonMod, _) ->
    JsonMod.

names() ->
    [json, jsonb].

encode(ErlJson, json, JsonMod) when is_atom(JsonMod) ->
    JsonMod:encode(ErlJson);
encode(ErlJson, json, {JsonMod, EncodeOpts, _}) when is_atom(JsonMod) ->
    JsonMod:encode(ErlJson, EncodeOpts);
encode(ErlJson, jsonb, JsonMod) when is_atom(JsonMod) ->
    [<<?JSONB_VERSION_1:8>> | JsonMod:encode(ErlJson)];
encode(ErlJson, jsonb, {JsonMod, EncodeOpts, _}) when is_atom(JsonMod) ->
    [<<?JSONB_VERSION_1:8>> | JsonMod:encode(ErlJson, EncodeOpts)];
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
