%%% @doc
%%% Codec for `geometry' PostGIS umbrella datatype.
%%% http://postgis.net/docs/manual-2.4/geometry.html
%%% $POSTGIS$/postgis/lwgeom_inout.c
%%% @end
%%% Created : 14 Oct 2017 by Sergey Prokhorov <me@seriyps.ru>

-module(epgsql_codec_postgis).
-behaviour(epgsql_codec).

-export([init/2, names/0, encode/3, decode/3]).

-export_type([data/0]).

-type data() :: point().
-type point() :: {}.

init(_, _) -> [].

names() ->
    [geometry].

encode(Geo, geometry, _) ->
    ewkb:encode_geometry(Geo).

decode(Bin, geometry, _) ->
    ewkb:decode_geometry(Bin).
