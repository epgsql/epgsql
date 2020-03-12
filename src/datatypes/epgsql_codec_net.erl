%%% @doc
%%% Codec for `inet', `cidr'
%%%
%%% TIP: use {@link inet:ntoa/1} and {@link inet:parse_address/1} to convert
%%% between {@link ip()} and `string()'.
%%% <ul>
%%%  <li>[https://www.postgresql.org/docs/10/static/datatype-net-types.html]</li>
%%%  <li>$PG$/src/backend/utils/adt/network.c</li>
%%% </ul>
%%% @end
%%% Created : 12 Oct 2017 by Sergey Prokhorov <me@seriyps.ru>
-module(epgsql_codec_net).
-behaviour(epgsql_codec).

-export([init/2, names/0, encode/3, decode/3, decode_text/3]).

-export_type([data/0]).

-type data() :: ip() | ip_mask() | macaddr() | macaddr8().

-type ip() :: inet:ip_address().
-type mask() :: 0..32.
-type ip_mask() :: {ip(), mask()}.
-type macaddr() :: {byte(), byte(), byte(), byte(), byte(), byte()}.
-type macaddr8() :: {byte(), byte(), byte(), byte(), byte(), byte(), byte(), byte()}.

-define(INET, 2).
-define(INET6, 3).
-define(IP_SIZE, 4).
-define(IP6_SIZE, 16).
-define(MAX_IP_MASK, 32).
-define(MAX_IP6_MASK, 128).

init(_, _) -> [].

names() ->
    [inet, cidr, macaddr, macaddr8].

encode({B1, B2, B3, B4, B5, B6}, macaddr, _) ->
    <<B1, B2, B3, B4, B5, B6>>;
encode({B1, B2, B3, B4, B5, B6, B7, B8}, macaddr8, _) ->
    <<B1, B2, B3, B4, B5, B6, B7, B8>>;
encode(IpMask, _, _) ->
    encode_net(IpMask).

decode(<<B1, B2, B3, B4, B5, B6>>, macaddr, _) ->
    {B1, B2, B3, B4, B5, B6};
decode(<<B1, B2, B3, B4, B5, B6, B7, B8>>, macaddr8, _) ->
    {B1, B2, B3, B4, B5, B6, B7, B8};
decode(Bin, _, _) ->
    decode_net(Bin).

-spec encode_net(ip() | ip_mask()) -> binary().
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

-spec decode_net(binary()) -> ip() | ip_mask().
decode_net(<<?INET, Mask:8, 1, ?IP_SIZE, Bin/binary>>) ->
    {list_to_tuple(binary_to_list(Bin)), Mask};
decode_net(<<?INET6, Mask:8, 1, ?IP6_SIZE, Bin/binary>>) ->
    {list_to_tuple([X || <<X:16>> <= Bin]), Mask};
decode_net(<<?INET, ?MAX_IP_MASK, 0, ?IP_SIZE, Bin/binary>>) ->
    list_to_tuple(binary_to_list(Bin));
decode_net(<<?INET6, ?MAX_IP6_MASK, 0, ?IP6_SIZE, Bin/binary>>) ->
    list_to_tuple([X || <<X:16>> <= Bin]).

decode_text(V, _, _) -> V.
