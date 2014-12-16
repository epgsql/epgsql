-module(ewkb).

-export([parse_geometry/1, format_geometry/1]).

-type point_type() :: '2d' | '3d' | '2dm' | '3dm'.

-record(point,{
  point_type :: point_type(),
  x :: float(),
  y :: float(),
  z :: float(),
  m :: float()
  }).

-type point(PointType) :: #point{ point_type :: PointType }.

-record(multi_point,{
  point_type :: PointType,
  points :: [point(PointType)]
  }).

-type multi_point(PointType) :: #multi_point{ point_type :: PointType }.

-record(line_string,{
  point_type :: PointType,
  points :: [point(PointType)]
  }).

-type line_string(PointType) :: #line_string{ point_type :: PointType }.

-record(multi_line_string,{
  point_type :: PointType,
  line_strings :: [line_string(PointType)]
  }).

-type multi_line_string(PointType) :: #multi_line_string{ point_type :: PointType }.

-record(circular_string,{
  point_type :: PointType,
  points :: [point(PointType)]
  }).

-type basic_string(PointType) :: #circular_string{ point_type :: PointType } | #line_string{ point_type :: PointType }.

-record(compound_curve,{
  point_type :: PointType,
  lines :: [basic_string(PointType)]
  }).

-type curve(PointType) :: #circular_string{ point_type :: PointType } | #line_string{ point_type :: PointType } | #compound_curve{ point_type :: PointType }.

-record(multi_curve,{
  point_type :: PointType,
  curves :: [curve(PointType)]
  }).

-type multi_curve(PointType) :: #multi_curve{ point_type :: PointType }.

-record(polygon,{
  point_type :: PointType,
  rings :: [line_string(PointType)]
  }).

-type polygon(PointType) :: #polygon{ point_type :: PointType }.

-record(multi_polygon,{
  point_type :: PointType,
  polygons :: [polygon(PointType)]
  }).

-type multi_polygon(PointType) :: #multi_polygon{ point_type :: PointType }.

-record(triangle,{
  point_type :: PointType,
  rings :: [line_string(PointType)]
  }).

-type triangle(PointType) :: #triangle{ point_type :: PointType }.

-record(curve_polygon,{
  point_type :: PointType,
  rings :: [curve(PointType)] 
  }).

-type curve_polygon(PointType) :: #curve_polygon{ point_type :: PointType }.

-record(polyhedral_surface,{
  point_type :: PointType,
  polygons :: [polygon(PointType)]
  }).

-type polyhedral_surface(PointType) :: #polyhedral_surface{ point_type :: PointType }.

-type surface(PointType) :: polygon(PointType) | curve_polygon(PointType) | polyhedral_surface(PointType).

-record(multi_surface,{
  point_type :: PointType,
  surfaces :: [surface(PointType)]
  }).

-type multi_surface(PointType) :: #multi_surface{ point_type :: PointType }.

-record(tin,{
  point_type :: PointType,
  triangles :: [triangle(PointType)]
  }).

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


parse_geometry(Binary) ->
  {Geometry, <<>>} = parse_geometry_data(Binary),
  Geometry.

format_geometry(Geometry) ->
  Type = format_type(Geometry),
  PointType = format_point_type(Geometry),
  Data = format_geometry_data(Geometry),
  <<"01", Type/binary, PointType/binary, Data/binary>>.

format_geometry_data(#point{ point_type = '2d', x = X, y = Y }) ->
  Xbin = format_float64(X),
  Ybin = format_float64(Y),
  <<Xbin/binary, Ybin/binary>>;
format_geometry_data(#point{ point_type = '2dm', x = X, y = Y, m = M }) ->
  Xbin = format_float64(X),
  Ybin = format_float64(Y),
  Mbin = format_float64(M),
  <<Xbin/binary, Ybin/binary, Mbin/binary>>;
format_geometry_data(#point{ point_type = '3d', x = X, y = Y, z = Z }) ->
  Xbin = format_float64(X),
  Ybin = format_float64(Y),
  Zbin = format_float64(Z),
  <<Xbin/binary, Ybin/binary, Zbin/binary>>;
format_geometry_data(#point{ point_type = '3dm', x = X, y = Y, z = Z, m = M }) ->
  Xbin = format_float64(X),
  Ybin = format_float64(Y),
  Zbin = format_float64(Z),
  Mbin = format_float64(M),
  <<Xbin/binary, Ybin/binary, Zbin/binary, Mbin/binary>>;
format_geometry_data({SimpleCollection, _, Data})
    when SimpleCollection == line_string;
         SimpleCollection == circular_string;
         SimpleCollection == polygon;
         SimpleCollection == triangle ->
  format_collection(Data);
format_geometry_data({TypedCollection, _, Data})
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
  format_typed_collection(Data).

format_collection(Collection) when is_list(Collection) ->
  Length = length(Collection),
  LengthBin = format_int32(Length),
  CollectionBin = lists:foldl(
    fun(Element, Acc) ->
      ElementBin = format_geometry_data(Element),
      <<Acc/binary, ElementBin/binary>>
    end,
    <<>>,
    Collection),
  <<LengthBin/binary, CollectionBin/binary>>.

format_typed_collection(Collection) when is_list(Collection) ->
  Length = length(Collection),
  LengthBin = format_int32(Length),
  CollectionBin = lists:foldl(
    fun(Element, Acc) ->
      ElementBin = format_geometry(Element),
      <<Acc/binary, ElementBin/binary>>
    end,
    <<>>,
    Collection),
  <<LengthBin/binary, CollectionBin/binary>>.  
  

format_int32(Int) when is_integer(Int) ->
  Bin = <<Int:1/little-integer-unit:32>>,
  bin_to_hex(Bin).

format_float64(Int) when is_number(Int) ->
  Bin = <<Int:1/little-float-unit:64>>,
  bin_to_hex(Bin).

-spec parse_geometry_data(binary()) -> {geometry(), binary()}.
parse_geometry_data(Binary) ->
  <<"01", TypeCode:4/binary, SubtypeCode:4/binary, Data/binary>> = Binary,
  Type = parse_type(TypeCode),
  Subtype = parse_point_type(SubtypeCode),
  parse_geometry_data(Type, Subtype, Data).

-spec parse_geometry_data(geom_type(), point_type(), binary()) -> {geometry(), binary()}.
parse_geometry_data(curve, _, _) -> error({curve, not_supported});
parse_geometry_data(surface, _, _) -> error({surface, not_supported});
parse_geometry_data(geometry, _, _) -> error({geometry, not_supported});
parse_geometry_data(point, PointType, Data) ->
  parse_point(PointType, Data);
parse_geometry_data(LineType, PointType, Data) 
    when LineType == line_string; 
         LineType == circular_string ->
  {Points, Rest} = parse_collection(point, PointType, Data),
  {{LineType, PointType, Points}, Rest};
parse_geometry_data(polygon, PointType, Data) ->
  {Lines, Rest} = parse_collection(line_string, PointType, Data),
  {#polygon{ point_type = PointType, rings = Lines }, Rest};
parse_geometry_data(triangle, PointType, Data) ->
  {#polygon{ rings = Rings }, Rest} = parse_geometry_data(polygon, PointType, Data),
  {#triangle{ point_type = PointType, rings = Rings }, Rest};
parse_geometry_data(Collection, PointType, Data)
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
    {Lines, Rest} = parse_typed_collection(Data),
    {{Collection, PointType, Lines}, Rest}.

-spec parse_collection(geom_type(), point_type(), binary()) -> {[geometry()], binary()}.
parse_collection(Type, PointType, Data) ->
  {Length, CountRest} = parse_int32(Data),
  lists:foldl(
    fun(_, {Geoms, Rest}) ->
      {Geom, R} = parse_geometry_data(Type, PointType, Rest),
      {Geoms ++ [Geom], R}
    end,
    {[], CountRest},
    lists:seq(1, Length)).

-spec parse_typed_collection(binary()) -> {[geometry()], binary()}.
parse_typed_collection(Data) ->
  {Length, CountRest} = parse_int32(Data),
  lists:foldl(
    fun(_, {Geoms, Rest}) ->
      {Geom, R} = parse_geometry_data(Rest),
      {Geoms ++ [Geom], R}
    end,
    {[], CountRest},
    lists:seq(1, Length)).


-spec parse_int32(binary()) -> {integer(), binary()}.
parse_int32(<<Hex:8/binary, Rest/binary>>) ->
  <<Int:1/little-integer-unit:32>> = hex_to_bin(Hex),
  {Int, Rest}.

-spec parse_float64(binary()) -> {float(), binary()}.
parse_float64(<<Hex:16/binary, Rest/binary>>) ->
  <<Float:1/little-float-unit:64>> = hex_to_bin(Hex),
  {Float, Rest}.

parse_point(PointType, Data) ->
  {Values, Rest} = lists:foldl(
    fun(_, {Values, Rest}) ->
      {Value, R} = parse_float64(Rest),
      {Values ++ [Value], R}
    end,
    {[], Data},
    lists:seq(1, point_size(PointType))),
  Point = case {PointType, Values} of
    {'2d', [X,Y]} ->
      #point{ point_type = PointType, x = X, y = Y };
    {'2dm', [X,Y,M]} ->
      #point{ point_type = PointType, x = X, y = Y, m = M };
    {'3d', [X,Y,Z]} ->
      #point{ point_type = PointType, x = X, y = Y, z = Z };
    {'3dm', [X,Y,Z,M]} ->
      #point{ point_type = PointType, x = X, y = Y, z = Z, m = M }
  end,
  {Point, Rest}.

-spec point_size(point_type()) -> 2..4.
point_size('2d')  -> 2;
point_size('2dm') -> 3;
point_size('3d')  -> 3;
point_size('3dm') -> 4.

-spec parse_type(binary()) -> geom_type().
parse_type(<<"0000">>) -> geometry;
parse_type(<<"0100">>) -> point;
parse_type(<<"0200">>) -> line_string;
parse_type(<<"0300">>) -> polygon;
parse_type(<<"0400">>) -> multi_point;
parse_type(<<"0500">>) -> multi_line_string;
parse_type(<<"0600">>) -> multi_polygon;
parse_type(<<"0700">>) -> geometry_collection;
parse_type(<<"0800">>) -> circular_string;
parse_type(<<"0900">>) -> compound_curve;
parse_type(<<"0A00">>) -> curve_polygon;
parse_type(<<"0B00">>) -> multi_curve;
parse_type(<<"0C00">>) -> multi_surface;
parse_type(<<"0D00">>) -> curve;
parse_type(<<"0E00">>) -> surface;
parse_type(<<"0F00">>) -> polyhedral_surface;
parse_type(<<"1000">>) -> tin;
parse_type(<<"1100">>) -> triangle.

-spec format_type(geometry() | geom_type()) -> binary().
format_type(Geometry) when is_tuple(Geometry) ->
  format_type(element(1, Geometry));
format_type(geometry)            -> <<"0000">>;
format_type(point)               -> <<"0100">>;
format_type(line_string)         -> <<"0200">>;
format_type(polygon)             -> <<"0300">>;
format_type(multi_point)         -> <<"0400">>;
format_type(multi_line_string)   -> <<"0500">>;
format_type(multi_polygon)       -> <<"0600">>;
format_type(geometry_collection) -> <<"0700">>;
format_type(circular_string)     -> <<"0800">>;
format_type(compound_curve)      -> <<"0900">>;
format_type(curve_polygon)       -> <<"0A00">>;
format_type(multi_curve)         -> <<"0B00">>;
format_type(multi_surface)       -> <<"0C00">>;
format_type(curve)               -> <<"0D00">>;
format_type(surface)             -> <<"0E00">>;
format_type(polyhedral_surface)  -> <<"0F00">>;
format_type(tin)                 -> <<"1000">>;
format_type(triangle)            -> <<"1100">>.


-spec parse_point_type(binary()) -> point_type().
parse_point_type(<<"0000">>) -> '2d';
parse_point_type(<<"0080">>) -> '3d';
parse_point_type(<<"0040">>) -> '2dm';
parse_point_type(<<"00c0">>) -> '3dm'.

-spec format_point_type(geometry() | point_type()) -> binary().
format_point_type(Geometry) when is_tuple(Geometry) ->
  format_point_type(element(2, Geometry));
format_point_type('2d') -> <<"0000">>;
format_point_type('3d') -> <<"0080">>;
format_point_type('2dm') -> <<"0040">>;
format_point_type('3dm') -> <<"00c0">>.

hex_to_bin(<<C:2/binary>>) ->
  Int = binary_to_integer(C, 16),
  << Int >>;
hex_to_bin(<<C:2/binary, Rest/binary>>) ->
  Int = binary_to_integer(C, 16),
  RestBin = hex_to_bin(Rest),
  << Int, RestBin/binary >>.

bin_to_hex(<<C>>) ->
  Int = int_to_hex(C),
  << Int/binary >>;
bin_to_hex(<<C, Rest/binary>>) ->
  Int = int_to_hex(C),
  RestBin = bin_to_hex(Rest),
  << Int/binary, RestBin/binary >>.

int_to_hex(C) ->
  case integer_to_binary(C, 16) of
    <<_,_>> = Val -> Val;
    <<Byte>> -> <<"0", Byte>>
  end.
