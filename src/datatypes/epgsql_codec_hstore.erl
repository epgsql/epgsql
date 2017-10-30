%%% @doc
%%% Codec for `hstore' type.
%%% https://www.postgresql.org/docs/current/static/hstore.html
%%% XXX: hstore not a part of postgresql builtin datatypes, it's in contrib.
%%% It should be enabled in postgresql by command
%%% `CREATE EXTENSION hstore`
%%% $PG$/contrib/hstore/
%%% @end
%%% Created : 14 Oct 2017 by Sergey Prokhorov <me@seriyps.ru>

-module(epgsql_codec_hstore).
-behaviour(epgsql_codec).

-export([init/2, names/0, encode/3, decode/3]).

-include("protocol.hrl").

-export_type([data/0]).

-type data() :: data_in() | data_out().

-type key_in() :: list() | binary() | atom() | integer() | float().
%% jiffy-style maps
-type data_in() :: { [{key_in(), binary()}] }.
-type data_out() :: { [{Key :: binary(), Value :: binary()}] }.

%% TODO: option for output format: proplist | jiffy-object | map
init(_, _) -> [].

names() ->
    [hstore].

-dialyzer([{nowarn_function, [encode/3]}, no_improper_lists]).
encode({Hstore}, hstore, _) when is_list(Hstore) ->
    Size = length(Hstore),
    Body = [[encode_key(K) | encode_value(V)]
           || {K, V} <- Hstore],
    [<<Size:?int32>> | Body].

decode(<<Size:?int32, Elements/binary>>, hstore, _) ->
    {do_decode(Size, Elements)}.


encode_key(K) ->
    encode_string(K).

encode_value(null) ->
    <<-1:?int32>>;
encode_value(undefined) ->
    <<-1:?int32>>;
encode_value(V) ->
    encode_string(V).

encode_string(Str) when is_binary(Str) ->
    <<(byte_size(Str)):?int32, Str/binary>>;
encode_string(Str) when is_list(Str) ->
    encode_string(list_to_binary(Str));
encode_string(Str) when is_atom(Str) ->
    encode_string(atom_to_binary(Str, utf8));
encode_string(Str) when is_integer(Str) ->
    encode_string(integer_to_binary(Str));
encode_string(Str) when is_float(Str) ->
    encode_string(io_lib:format("~w", [Str])).
    %% encode_string(erlang:float_to_binary(Str)).


do_decode(0, _) -> [];
do_decode(N, <<KeyLen:?int32, Key:KeyLen/binary, -1:?int32, Rest/binary>>) ->
    [{Key, null} | do_decode(N - 1, Rest)];
do_decode(N, <<KeyLen:?int32, Key:KeyLen/binary,
               ValLen:?int32, Value:ValLen/binary, Rest/binary>>) ->
    [{Key, Value} | do_decode(N - 1, Rest)].
