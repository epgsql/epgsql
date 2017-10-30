%%% @doc
%%% Codec for `inet', `cidr'
%%% https://www.postgresql.org/docs/10/static/datatype-net-types.html
%%% $PG$/src/backend/utils/adt/network.c
%%%
%%% TIP: use `inet:ntoa/1' to convert `ip()' to string.
%%% @end
%%% Created : 12 Oct 2017 by Sergey Prokhorov <me@seriyps.ru>
%% TODO: `macaddr', `macaddr8' `mac.c`
-module(epgsql_codec_net).
-behaviour(epgsql_codec).

-export([init/2, names/0, encode/3, decode/3]).

-export_type([data/0]).

-type data() :: ip() | ip_mask().

-type ip() :: inet:ip_address().
-type mask() :: 0..32.
-type ip_mask() :: {ip(), mask()}.

-define(INET, 2).
-define(INET6, 3).
-define(IP_SIZE, 4).
-define(IP6_SIZE, 16).
-define(MAX_IP_MASK, 32).
-define(MAX_IP6_MASK, 128).

init(_, _) -> [].

names() ->
    [inet, cidr].

encode(IpMask, _, _) ->
    encode_net(IpMask).

decode(Bin, _, _) ->
    decode_net(Bin).

-spec encode_net(data()) -> binary().
encode_net({{_, _, _, _} = IP, Mask}) ->
    Bin = list_to_binary(tuple_to_list(IP)),
    <<?INET, Mask:8, 1, ?IP_SIZE, Bin/binary>>;
encode_net({{_, _, _, _, _, _, _, _} = IP, Mask}) ->
    Bin = << <<X:16>> || X <- tuple_to_list(IP) >>,
    <<?INET6, Mask:8, 1, ?IP6_SIZE, Bin/binary>>;
encode_net({_, _, _, _} = IP) ->
    Bin = list_to_binary(tuple_to_list(IP)),
    <<?INET, ?MAX_IP_MASK, 0, ?IP_SIZE, Bin/binary>>;
encode_net({_, _, _, _, _, _, _, _} = IP) ->
    Bin = << <<X:16>> || X <- tuple_to_list(IP) >>,
    <<?INET6, ?MAX_IP6_MASK, 0, ?IP6_SIZE, Bin/binary>>.

-spec decode_net(binary()) -> data().
decode_net(<<?INET, Mask:8, 1, ?IP_SIZE, Bin/binary>>) ->
    {list_to_tuple(binary_to_list(Bin)), Mask};
decode_net(<<?INET6, Mask:8, 1, ?IP6_SIZE, Bin/binary>>) ->
    {list_to_tuple([X || <<X:16>> <= Bin]), Mask};
decode_net(<<?INET, ?MAX_IP_MASK, 0, ?IP_SIZE, Bin/binary>>) ->
    list_to_tuple(binary_to_list(Bin));
decode_net(<<?INET6, ?MAX_IP6_MASK, 0, ?IP6_SIZE, Bin/binary>>) ->
    list_to_tuple([X || <<X:16>> <= Bin]).
