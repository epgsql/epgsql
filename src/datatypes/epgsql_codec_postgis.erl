%%% @doc
%%% Codec for `geometry' PostGIS umbrella datatype.
%%%
%%% XXX: PostGIS is not a Postgres's built-in datatype! It should be instaled
%%% separately and enabled via `CREATE EXTENSION postgis'.
%%% <ul>
%%%  <li>[http://postgis.net/docs/manual-2.4/geometry.html]</li>
%%%  <li>$POSTGIS$/postgis/lwgeom_inout.c</li>
%%% </ul>
%%% @end
%%% @see ewkb
%%% @end
%%% Created : 14 Oct 2017 by Sergey Prokhorov <me@seriyps.ru>

-module(epgsql_codec_postgis).
-behaviour(epgsql_codec).

-export([init/2, names/0, encode/3, decode/3, decode_text/3]).

-export_type([data/0]).

-type data() :: ewkb:geometry().

init(_, _) -> [].

names() ->
    [geometry].

encode(Geo, geometry, _) ->
    ewkb:encode_geometry(Geo).

decode(Bin, geometry, _) ->
    ewkb:decode_geometry(Bin).

decode_text(V, _, _) -> V.
