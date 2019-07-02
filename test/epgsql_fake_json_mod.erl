-module(epgsql_fake_json_mod).

-export([encode/1]).
-export([decode/1]).

encode(ErlJson) ->
    encode(ErlJson, []).

encode({ErlJson}, _EncodeOpts) ->
    ErlJson.

decode(EncodedJson) ->
    decode(EncodedJson, []).

decode(EncodedJson, _DecodeOpts) ->
    {EncodedJson}.
