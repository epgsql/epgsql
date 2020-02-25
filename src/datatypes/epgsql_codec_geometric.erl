%%% @doc
%%% Codec for `point'.
%%%
%%% Codecs for other geometric datatypes (line, box, path, polygon, circle) can
%%% be added later.
%%%
%%% XXX: it's not PostGIS! For PostGIS see {@link epgsql_codec_postgis}.
%%% <ul>
%%%  <li>[https://www.postgresql.org/docs/current/static/datatype-geometric.html]</li>
%%%  <li>$PG$/src/backend/utils/adt/geo_ops.c</li>
%%% </ul>
%%% @end
%%% Created : 14 Oct 2017 by Sergey Prokhorov <me@seriyps.ru>
%%% TODO: line, lseg, box, path, polygon, circle

-module(epgsql_codec_geometric).
-behaviour(epgsql_codec).

-export([init/2, names/0, encode/3, decode/3, decode_text/3]).

-export_type([data/0]).

-type data() :: point().
-type point() :: {float(), float()}.

init(_, _) -> [].

names() ->
    [point].

encode({X, Y}, point, _) when is_number(X), is_number(Y) ->
    %% XXX: looks like it doesn't have size prefix?!
    <<X:1/big-float-unit:64, Y:1/big-float-unit:64>>.

decode(<<X:1/big-float-unit:64, Y:1/big-float-unit:64>>, point, _) ->
    {X, Y}.

decode_text(V, _, _) -> V.
