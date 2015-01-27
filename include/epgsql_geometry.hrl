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
  point_type :: point_type() ,
  points :: [point(point_type())]
  }).

-type multi_point(PointType) :: #multi_point{ point_type :: PointType }.

-record(line_string,{
  point_type :: point_type(),
  points :: [point(point_type())]
  }).

-type line_string(PointType) :: #line_string{ point_type :: PointType }.

-record(multi_line_string,{
  point_type :: point_type(),
  line_strings :: [line_string(point_type())]
  }).

-type multi_line_string(PointType) :: #multi_line_string{ point_type :: PointType }.

-record(circular_string,{
  point_type :: point_type(),
  points :: [point(point_type())]
  }).

-type basic_string(PointType) :: #circular_string{ point_type :: PointType } | #line_string{ point_type :: PointType }.

-record(compound_curve,{
  point_type :: point_type(),
  lines :: [basic_string(point_type())]
  }).

-type curve(PointType) :: #circular_string{ point_type :: PointType } | #line_string{ point_type :: PointType } | #compound_curve{ point_type :: PointType }.

-record(multi_curve,{
  point_type :: point_type(),
  curves :: [curve(point_type())]
  }).

-type multi_curve(PointType) :: #multi_curve{ point_type :: PointType }.

-record(polygon,{
  point_type :: point_type(),
  rings :: [line_string(point_type())]
  }).

-type polygon(PointType) :: #polygon{ point_type :: PointType }.

-record(multi_polygon,{
  point_type :: point_type(),
  polygons :: [polygon(point_type())]
  }).

-type multi_polygon(PointType) :: #multi_polygon{ point_type :: PointType }.

-record(triangle,{
  point_type :: point_type(),
  rings :: [line_string(point_type())]
  }).

-type triangle(PointType) :: #triangle{ point_type :: PointType }.

-record(curve_polygon,{
  point_type :: point_type(),
  rings :: [curve(point_type())]
  }).

-type curve_polygon(PointType) :: #curve_polygon{ point_type :: PointType }.

-record(polyhedral_surface,{
  point_type :: point_type(),
  polygons :: [polygon(point_type())]
  }).

-type polyhedral_surface(PointType) :: #polyhedral_surface{ point_type :: PointType }.

-type surface(PointType) :: polygon(PointType) | curve_polygon(PointType) | polyhedral_surface(PointType).

-record(multi_surface,{
  point_type :: point_type(),
  surfaces :: [surface(point_type())]
  }).

-type multi_surface(PointType) :: #multi_surface{ point_type :: PointType }.

-record(tin,{
  point_type :: point_type(),
  triangles :: [triangle(point_type())]
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
