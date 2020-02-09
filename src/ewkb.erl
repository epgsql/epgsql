%% @doc
%% Encoder/decoder for PostGIS binary data representation.
%%
%% <ul>
%%  <li>[https://en.wikipedia.org/wiki/Well-known_text]</li>
%%  <li>[http://postgis.net/docs/manual-2.4/using_postgis_dbmanagement.html#EWKB_EWKT]</li>
%% </ul>
-module(ewkb).
-export([decode_geometry/1, encode_geometry/1]).
-export_type([point_type/0, point/1, multi_point/1, line_string/1,
              multi_line_string/1, basic_string/1, curve/1, multi_curve/1,
              polygon/1, multi_polygon/1, triangle/1, curve_polygon/1,
              polyhedral_surface/1, surface/1, multi_surface/1, tin/1,
              geometry/1, geometry/0, geometry_collection/1, geom_type/0]).

-include("epgsql_geometry.hrl").

-type point_type() :: '2d' | '3d' | '2dm' | '3dm'.
-type point(PointType) :: #point{ point_type :: PointType }.
-type multi_point(PointType) :: #multi_point{ point_type :: PointType }.
-type line_string(PointType) :: #line_string{ point_type :: PointType }.
-type multi_line_string(PointType) :: #multi_line_string{ point_type :: PointType }.
-type basic_string(PointType) :: #circular_string{ point_type :: PointType }
                               | #line_string{ point_type :: PointType }.
-type curve(PointType) :: #circular_string{ point_type :: PointType }
                        | #line_string{ point_type :: PointType }
                        | #compound_curve{ point_type :: PointType }.
-type multi_curve(PointType) :: #multi_curve{ point_type :: PointType }.
-type polygon(PointType) :: #polygon{ point_type :: PointType }.
-type multi_polygon(PointType) :: #multi_polygon{ point_type :: PointType }.
-type triangle(PointType) :: #triangle{ point_type :: PointType }.
-type curve_polygon(PointType) :: #curve_polygon{ point_type :: PointType }.
-type polyhedral_surface(PointType) :: #polyhedral_surface{ point_type :: PointType }.
-type surface(PointType) :: polygon(PointType)
                          | curve_polygon(PointType)
                          | polyhedral_surface(PointType).
-type multi_surface(PointType) :: #multi_surface{ point_type :: PointType }.
-type tin(PointType) :: #tin{ point_type :: PointType }.
-type geometry(PointType) :: point(PointType) |
                             line_string(PointType) |
                             triangle(PointType) |
                             tin(PointType) |
                             curve(PointType) |
                             surface(PointType) |
                             multi_point(PointType) |
                             multi_line_string(PointType) |
                             multi_polygon(PointType) |
                             multi_curve(PointType) |
                             multi_surface(PointType) |
                             geometry_collection(PointType).
-type geometry() :: geometry(point_type()).
-type geometry_collection(PointType) :: [geometry(PointType)].

-type geom_type() :: geometry
                   | point       %
                   | line_string%
                   | polygon%
                   | multi_point%
                   | multi_line_string%
                   | multi_polygon%
                   | geometry_collection%
                   | circular_string%
                   | compound_curve%
                   | curve_polygon%
                   | multi_curve%
                   | multi_surface%
                   | curve%
                   | surface%
                   | polyhedral_surface%
                   | tin%
                   | triangle.%

-spec decode_geometry(binary()) -> geometry().
decode_geometry(Binary) ->
  {Geometry, <<>>} = decode_geometry_data(Binary),
  Geometry.

-spec encode_geometry(geometry()) -> binary().
encode_geometry(Geometry) ->
  Type = encode_type(Geometry),
  PointType = encode_point_type(Geometry),
  Data = encode_geometry_data(Geometry),
  <<1, Type/binary, PointType/binary, Data/binary>>.

encode_geometry_data(#point{ point_type = '2d', x = X, y = Y }) ->
  Xbin = encode_float64(X),
  Ybin = encode_float64(Y),
  <<Xbin/binary, Ybin/binary>>;
encode_geometry_data(#point{ point_type = '2dm', x = X, y = Y, m = M }) ->
  Xbin = encode_float64(X),
  Ybin = encode_float64(Y),
  Mbin = encode_float64(M),
  <<Xbin/binary, Ybin/binary, Mbin/binary>>;
encode_geometry_data(#point{ point_type = '3d', x = X, y = Y, z = Z }) ->
  Xbin = encode_float64(X),
  Ybin = encode_float64(Y),
  Zbin = encode_float64(Z),
  <<Xbin/binary, Ybin/binary, Zbin/binary>>;
encode_geometry_data(#point{ point_type = '3dm', x = X, y = Y, z = Z, m = M }) ->
  Xbin = encode_float64(X),
  Ybin = encode_float64(Y),
  Zbin = encode_float64(Z),
  Mbin = encode_float64(M),
  <<Xbin/binary, Ybin/binary, Zbin/binary, Mbin/binary>>;
encode_geometry_data({SimpleCollection, _, Data})
    when SimpleCollection == line_string;
         SimpleCollection == circular_string;
         SimpleCollection == polygon;
         SimpleCollection == triangle ->
  encode_collection(Data);
encode_geometry_data({TypedCollection, _, Data})
    when
        TypedCollection == multi_point;
        TypedCollection == multi_line_string;
        TypedCollection == multi_curve;
        TypedCollection == multi_polygon;
        TypedCollection == multi_surface;
        TypedCollection == compound_curve;
        TypedCollection == curve_polygon;
        TypedCollection == geometry_collection;
        TypedCollection == polyhedral_surface;
        TypedCollection == tin ->
  encode_typed_collection(Data).

encode_collection(Collection) when is_list(Collection) ->
  Length = length(Collection),
  LengthBin = encode_int32(Length),
  CollectionBin = lists:foldl(
    fun(Element, Acc) ->
      ElementBin = encode_geometry_data(Element),
      <<Acc/binary, ElementBin/binary>>
    end,
    <<>>,
    Collection),
  <<LengthBin/binary, CollectionBin/binary>>.

encode_typed_collection(Collection) when is_list(Collection) ->
  Length = length(Collection),
  LengthBin = encode_int32(Length),
  CollectionBin = lists:foldl(
    fun(Element, Acc) ->
      ElementBin = encode_geometry(Element),
      <<Acc/binary, ElementBin/binary>>
    end,
    <<>>,
    Collection),
  <<LengthBin/binary, CollectionBin/binary>>.


encode_int32(Int) when is_integer(Int) ->
  <<Int:1/little-integer-unit:32>>.

encode_float64(Int) when is_number(Int) ->
  <<Int:1/little-float-unit:64>>.

-spec decode_geometry_data(binary()) -> {geometry(), binary()}.
decode_geometry_data(Binary) ->
  <<1, TypeCode:2/binary, SubtypeCode:2/binary, Data/binary>> = Binary,
  Type = decode_type(TypeCode),
  Subtype = decode_point_type(SubtypeCode),
  decode_geometry_data(Type, Subtype, Data).

-spec decode_geometry_data(geom_type(), point_type(), binary()) -> {geometry(), binary()}.
decode_geometry_data(curve, _, _) -> error({curve, not_supported});
decode_geometry_data(surface, _, _) -> error({surface, not_supported});
decode_geometry_data(geometry, _, _) -> error({geometry, not_supported});
decode_geometry_data(point, PointType, Data) ->
  decode_point(PointType, Data);
decode_geometry_data(LineType, PointType, Data)
    when LineType == line_string;
         LineType == circular_string ->
  {Points, Rest} = decode_collection(point, PointType, Data),
  {{LineType, PointType, Points}, Rest};
decode_geometry_data(polygon, PointType, Data) ->
  {Lines, Rest} = decode_collection(line_string, PointType, Data),
  {#polygon{ point_type = PointType, rings = Lines }, Rest};
decode_geometry_data(triangle, PointType, Data) ->
  {#polygon{ rings = Rings }, Rest} = decode_geometry_data(polygon, PointType, Data),
  {#triangle{ point_type = PointType, rings = Rings }, Rest};
decode_geometry_data(Collection, PointType, Data)
    when
        Collection == multi_point;
        Collection == multi_line_string;
        Collection == multi_curve;
        Collection == multi_polygon;
        Collection == multi_surface;
        Collection == compound_curve;
        Collection == curve_polygon;
        Collection == geometry_collection;
        Collection == polyhedral_surface;
        Collection == tin  ->
    {Lines, Rest} = decode_typed_collection(Data),
    {{Collection, PointType, Lines}, Rest}.

-spec decode_collection(geom_type(), point_type(), binary()) -> {[geometry()], binary()}.
decode_collection(Type, PointType, Data) ->
  {Length, CountRest} = decode_int32(Data),
  lists:foldl(
    fun(_, {Geoms, Rest}) ->
      {Geom, R} = decode_geometry_data(Type, PointType, Rest),
      {Geoms ++ [Geom], R}
    end,
    {[], CountRest},
    lists:seq(1, Length)).

-spec decode_typed_collection(binary()) -> {[geometry()], binary()}.
decode_typed_collection(Data) ->
  {Length, CountRest} = decode_int32(Data),
  lists:foldl(
    fun(_, {Geoms, Rest}) ->
      {Geom, R} = decode_geometry_data(Rest),
      {Geoms ++ [Geom], R}
    end,
    {[], CountRest},
    lists:seq(1, Length)).


-spec decode_int32(binary()) -> {integer(), binary()}.
decode_int32(<<Hex:4/binary, Rest/binary>>) ->
  <<Int:1/little-integer-unit:32>> = Hex,
  {Int, Rest}.

-spec decode_float64(binary()) -> {float(), binary()}.
decode_float64(<<Hex:8/binary, Rest/binary>>) ->
  <<Float:1/little-float-unit:64>> = Hex,
  {Float, Rest}.

decode_point(PointType, Data) ->
  {Values, Rest} = lists:foldl(
    fun(_, {Values, Rest}) ->
      {Value, R} = decode_float64(Rest),
      {Values ++ [Value], R}
    end,
    {[], Data},
    lists:seq(1, point_size(PointType))),
  Point = case {PointType, Values} of
    {'2d', [X, Y]} ->
      #point{ point_type = PointType, x = X, y = Y };
    {'2dm', [X, Y, M]} ->
      #point{ point_type = PointType, x = X, y = Y, m = M };
    {'3d', [X, Y, Z]} ->
      #point{ point_type = PointType, x = X, y = Y, z = Z };
    {'3dm', [X, Y, Z, M]} ->
      #point{ point_type = PointType, x = X, y = Y, z = Z, m = M }
  end,
  {Point, Rest}.

-spec point_size(point_type()) -> 2..4.
point_size('2d')  -> 2;
point_size('2dm') -> 3;
point_size('3d')  -> 3;
point_size('3dm') -> 4.

-spec decode_type(binary()) -> geom_type().
decode_type(<<0, 0>>) -> geometry;
decode_type(<<1, 0>>) -> point;
decode_type(<<2, 0>>) -> line_string;
decode_type(<<3, 0>>) -> polygon;
decode_type(<<4, 0>>) -> multi_point;
decode_type(<<5, 0>>) -> multi_line_string;
decode_type(<<6, 0>>) -> multi_polygon;
decode_type(<<7, 0>>) -> geometry_collection;
decode_type(<<8, 0>>) -> circular_string;
decode_type(<<9, 0>>) -> compound_curve;
decode_type(<<10, 0>>) -> curve_polygon;
decode_type(<<11, 0>>) -> multi_curve;
decode_type(<<12, 0>>) -> multi_surface;
decode_type(<<13, 0>>) -> curve;
decode_type(<<14, 0>>) -> surface;
decode_type(<<15, 0>>) -> polyhedral_surface;
decode_type(<<16, 0>>) -> tin;
decode_type(<<17, 0>>) -> triangle.

-spec encode_type(geometry() | geom_type()) -> binary().
encode_type(Geometry) when is_tuple(Geometry) ->
  encode_type(element(1, Geometry));
encode_type(geometry)            -> <<00, 0>>;
encode_type(point)               -> <<01, 0>>;
encode_type(line_string)         -> <<02, 0>>;
encode_type(polygon)             -> <<03, 0>>;
encode_type(multi_point)         -> <<04, 0>>;
encode_type(multi_line_string)   -> <<05, 0>>;
encode_type(multi_polygon)       -> <<06, 0>>;
encode_type(geometry_collection) -> <<07, 0>>;
encode_type(circular_string)     -> <<08, 0>>;
encode_type(compound_curve)      -> <<09, 0>>;
encode_type(curve_polygon)       -> <<10, 0>>;
encode_type(multi_curve)         -> <<11, 0>>;
encode_type(multi_surface)       -> <<12, 0>>;
encode_type(curve)               -> <<13, 0>>;
encode_type(surface)             -> <<14, 0>>;
encode_type(polyhedral_surface)  -> <<15, 0>>;
encode_type(tin)                 -> <<16, 0>>;
encode_type(triangle)            -> <<17, 0>>.


-spec decode_point_type(binary()) -> point_type().
decode_point_type(<<0, 0>>) -> '2d';
decode_point_type(<<0, 64>>) -> '2dm';
decode_point_type(<<0, 128>>) -> '3d';
decode_point_type(<<0, 192>>) -> '3dm'.

-spec encode_point_type(geometry() | point_type()) -> binary().
encode_point_type(Geometry) when is_tuple(Geometry) ->
  encode_point_type(element(2, Geometry));
encode_point_type('2d') -> <<0, 0>>;
encode_point_type('2dm') -> <<0, 64>>;
encode_point_type('3d') -> <<0, 128>>;
encode_point_type('3dm') -> <<0, 192>>.
