%%% @doc
%%% Codec for `text', `varchar', `bytea'.
%%%
%%% If input for `text' or `varchar' is provided as a list, not binary, and it
%%% contains not just `byte()', an attempt to perform unicode conversion will be made.
%%%
%%% Also, `integer()', `float()' and `atom()' are automatically converted to
%%% strings, but this kind of conversion might be eventualy removed.
%%% <ul>
%%%  <li>[https://www.postgresql.org/docs/10/static/datatype-character.html]</li>
%%%  <li>$PG$/src/backend/utils/adt/varchar.c</li>
%%%  <li>$PG$/src/backend/utils/adt/varlena.c</li>
%%% </ul>
%%% @end
%%% @see epgsql_codec_bpchar. epgsql_codec_bpchar - for 'char' and 'char(N)'
%%% @end
%%% Created : 12 Oct 2017 by Sergey Prokhorov <me@seriyps.ru>

-module(epgsql_codec_text).
-behaviour(epgsql_codec).

-export([init/2, names/0, encode/3, decode/3, decode_text/3]).

-export_type([data/0]).

-type data() :: in_data() | out_data().
-type in_data() :: binary() | iolist() | string().
-type out_data() :: binary().

init(_, _) -> [].

names() ->
    [text, varchar, bytea].

encode(String, Name, State) when is_list(String) ->
    %% Try to not convert iolists to binary; this way they will be written directly to socket
    %% But we are doing implicit check that iolist is well formed by calling
    %% relatively cheap iolist_size/1 on it
    try iolist_size(String) of
        _ -> String
    catch error:badarg when Name == varchar orelse Name == text ->
            %% Maybe it's a unicode string; try to convert it to bytes
            encode(unicode:characters_to_binary(String), Name, State)
    end;
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

decode_text(V, _, _) -> V.
