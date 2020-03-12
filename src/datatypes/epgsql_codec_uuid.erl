%%% @doc
%%% Codec for `uuid' type.
%%%
%%% Input is expected to be in hex `string()' / `binary()', eg
%%% `<<"550e8400-e29b-41d4-a716-446655440000">>'.
%%% <ul>
%%%  <li>[https://www.postgresql.org/docs/current/static/datatype-uuid.html]</li>
%%%  <li>$PG$/src/backend/utils/adt/uuid.c</li>
%%% </ul>
%%% @end
%%% Created : 14 Oct 2017 by Sergey Prokhorov <me@seriyps.ru>

-module(epgsql_codec_uuid).
-behaviour(epgsql_codec).

-export([init/2, names/0, encode/3, decode/3, decode_text/3]).

-export_type([data/0]).

-type data() :: in_data() | out_data().
-type in_data() :: string() | binary().
-type out_data() :: binary().

init(_, _) -> [].

names() ->
    [uuid].

encode(Uuid, uuid, St) when is_list(Uuid) ->
    encode(list_to_binary(Uuid), uuid, St);
encode(Uuid, uuid, _) when is_binary(Uuid) ->
    Hex = binary:replace(Uuid, <<"-">>, <<>>, [global]),
    Int = erlang:binary_to_integer(Hex, 16),
    <<Int:128/big-unsigned-integer>>.

decode(<<U0:32, U1:16, U2:16, U3:16, U4:48>>, uuid, _) ->
    Format = "~8.16.0b-~4.16.0b-~4.16.0b-~4.16.0b-~12.16.0b",
    iolist_to_binary(io_lib:format(Format, [U0, U1, U2, U3, U4])).

decode_text(V, _, _) -> V.
