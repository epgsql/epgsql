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
encode(Bin, _, _) when is_binary(Bin) -> Bin;
encode(Other, _Name, _State) ->
    %% This is for backward compatibitlty! Maybe add warning?
    %% error_logger:warning_msg(
    %%   "epgsql_codec_text.erl: Deprecated attempt to encode '~p' as '~s'",
    %%   [Other, Name]),
    encode_compat(Other).

encode_compat(A) when is_atom(A)    -> atom_to_binary(A, utf8);
encode_compat(I) when is_integer(I) -> integer_to_binary(I);
encode_compat(F) when is_float(F)   -> float_to_binary(F).


decode(Bin, _, _) -> Bin.
