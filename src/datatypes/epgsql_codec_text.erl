%%% @doc
%%% Codec for `text', `varchar', `bytea'.
%%% For 'char' see epgsql_codec_bpchar.erl.
%%% https://www.postgresql.org/docs/10/static/datatype-character.html
%%% $PG$/src/backend/utils/adt/varchar.c
%%% $PG$/src/backend/utils/adt/varlena.c
%%% @end
%%% Created : 12 Oct 2017 by Sergey Prokhorov <me@seriyps.ru>

-module(epgsql_codec_text).
-behaviour(epgsql_codec).

-export([init/2, names/0, encode/3, decode/3]).

-export_type([data/0]).

-type data() :: in_data() | out_data().
-type in_data() :: binary() | string().
-type out_data() :: binary().

init(_, _) -> [].

names() ->
    [text, varchar, bytea].

encode(String, Name, State) when is_list(String) ->
    encode(list_to_binary(String), Name, State);
encode(Bin, _, _) when is_binary(Bin) -> Bin.

decode(Bin, _, _) -> Bin.
