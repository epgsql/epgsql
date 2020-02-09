%%% @private
%%% @doc
%%% Dummy codec. Used internally
%%% @end
%%% Created : 12 Oct 2017 by Sergey Prokhorov <me@seriyps.ru>

-module(epgsql_codec_noop).
-behaviour(epgsql_codec).

-export([init/2, names/0, encode/3, decode/3, decode_text/3]).

-export_type([data/0]).

-type data() :: binary().

init(_, _) -> [].

names() -> [].

encode(Bin, _, _) when is_binary(Bin) -> Bin.

decode(Bin, _, _) -> Bin.

decode_text(Bin, _, _) -> Bin.
