
-record(point,{
  point_type :: any(),
  x :: float(),
  y :: float(),
  z :: float() | undefined,
  m :: float() | undefined
  }).

-record(multi_point,{
  point_type :: any(),
  points :: [ewkb:point(ewkb:point_type())]
  }).

-record(line_string,{
  point_type :: any(),
  points :: [ewkb:point(ewkb:point_type())]
  }).

-record(multi_line_string,{
  point_type :: any(),
  line_strings :: [ewkb:line_string(ewkb:point_type())]
  }).

-record(circular_string,{
  point_type :: any(),
  points :: [ewkb:point(ewkb:point_type())]
  }).

-record(compound_curve,{
  point_type :: any(),
  lines :: [ewkb:basic_string(ewkb:point_type())]
  }).

-record(multi_curve,{
  point_type :: any(),
  curves :: [ewkb:curve(ewkb:point_type())]
  }).

-record(polygon,{
  point_type :: any(),
  rings :: [ewkb:line_string(ewkb:point_type())]
  }).

-record(multi_polygon,{
  point_type :: any(),
  polygons :: [ewkb:polygon(ewkb:point_type())]
  }).

-record(triangle,{
  point_type :: any(),
  rings :: [ewkb:line_string(ewkb:point_type())]
  }).

-record(curve_polygon,{
  point_type :: any(),
  rings :: [ewkb:curve(ewkb:point_type())]
  }).

-record(polyhedral_surface,{
  point_type :: any(),
  polygons :: [ewkb:polygon(ewkb:point_type())]
  }).

-record(multi_surface,{
  point_type :: any(),
  surfaces :: [ewkb:surface(ewkb:point_type())]
  }).

-record(tin,{
  point_type :: any(),
  triangles :: [ewkb:triangle(ewkb:point_type())]
  }).
