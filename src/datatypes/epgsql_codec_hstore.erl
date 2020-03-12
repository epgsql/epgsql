%%% @doc
%%% Codec for `hstore' type.
%%%
%%% Hstore codec can take a jiffy-style object or map() as input.
%%% Output format can be changed by providing `return' option. See {@link return_format()}.
%%% Values of hstore can be `NULL'. NULL representation can be changed by providing
%%% `nulls' option, semantics is similar to {@link epgsql:connect_opts()} `nulls' option.
%%%
%%% XXX: hstore is not a part of postgresql builtin datatypes, it's in contrib.
%%% It should be enabled in postgresql by command `CREATE EXTENSION hstore'.
%%% <ul>
%%%  <li>[https://www.postgresql.org/docs/current/static/hstore.html]</li>
%%%  <li>$PG$/contrib/hstore/</li>
%%% </ul>
%%% @end
%%% Created : 14 Oct 2017 by Sergey Prokhorov <me@seriyps.ru>

-module(epgsql_codec_hstore).
-behaviour(epgsql_codec).

-export([init/2, names/0, encode/3, decode/3, decode_text/3]).

-include("protocol.hrl").

-export_type([data/0, options/0, return_format/0]).

-type data() :: data_in() | data_out().

-type key_in() :: list() | binary() | atom() | integer() | float().
-type data_in() :: { [{key_in(), binary()}] } |
                   #{key_in() => binary() | atom()}.
-type data_out() :: { [{Key :: binary(), Value :: binary()}] } |      % jiffy
                    [{Key :: binary(), Value :: binary() | atom()}] | % proplist
                    #{binary() => binary() | atom()}.                 % map

-type return_format() :: map | jiffy | proplist.
-type options() :: #{return => return_format(),
                     nulls => [atom(), ...]}.

-record(st,
        {return :: return_format(),
         nulls :: [atom(), ...]}).

-dialyzer([{nowarn_function, [encode/3]}, no_improper_lists]).

init(Opts0, _) ->
    Opts = epgsql:to_map(Opts0),
    #st{return = maps:get(return, Opts, jiffy),
        nulls = maps:get(nulls, Opts, [null, undefined])}.

names() ->
    [hstore].

encode({KV}, hstore, #st{nulls = Nulls}) when is_list(KV) ->
    Size = length(KV),
    encode_kv(KV, Size, Nulls);
encode(Map, hstore, #st{nulls = Nulls}) when is_map(Map) ->
    Size = map_size(Map),
    encode_kv(maps:to_list(Map), Size, Nulls).

decode(<<Size:?int32, Elements/binary>>, hstore, #st{return = Return, nulls = Nulls}) ->
    KV = do_decode(Size, Elements, hd(Nulls)),
    case Return of
        jiffy ->
            {KV};
        map ->
            maps:from_list(KV);
        proplist ->
            KV
    end.

decode_text(V, _, _) -> V.

%% Internal

encode_kv(KV, Size, Nulls) ->
    %% TODO: construct improper list when support for Erl 17 will be dropped
    Body = [[encode_key(K), encode_value(V, Nulls)]
           || {K, V} <- KV],
    [<<Size:?int32>> | Body].

encode_key(K) ->
    encode_string(K).

encode_value(V, Nulls) ->
    case lists:member(V, Nulls) of
        true -> <<-1:?int32>>;
        false -> encode_string(V)
    end.

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


do_decode(0, _, _) -> [];
do_decode(N, <<KeyLen:?int32, Key:KeyLen/binary, -1:?int32, Rest/binary>>, Null) ->
    [{Key, Null} | do_decode(N - 1, Rest, Null)];
do_decode(N, <<KeyLen:?int32, Key:KeyLen/binary,
               ValLen:?int32, Value:ValLen/binary, Rest/binary>>, Null) ->
    [{Key, Value} | do_decode(N - 1, Rest, Null)].
