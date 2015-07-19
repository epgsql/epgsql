-type point_type() :: '2d' | '3d' | '2dm' | '3dm'.

-record(point,{
  point_type :: point_type(),
  x :: float(),
  y :: float(),
  z :: float(),
  m :: float()
  }).

-type point() :: #point{}.

-record(multi_point,{
  point_type :: point_type() ,
  points :: [point()]
  }).

-type multi_point() :: #multi_point{}.

-record(line_string,{
  point_type :: point_type(),
  points :: [point()]
  }).

-type line_string() :: #line_string{}.

-record(multi_line_string,{
  point_type :: point_type(),
  line_strings :: [line_string()]
  }).

-type multi_line_string() :: #multi_line_string{}.

-record(circular_string,{
  point_type :: point_type(),
  points :: [point()]
  }).

-type basic_string() :: #circular_string{} | #line_string{}.

-record(compound_curve,{
  point_type :: point_type(),
  lines :: [basic_string()]
  }).

-type curve() :: #circular_string{} | #line_string{} | #compound_curve{}.

-record(multi_curve,{
  point_type :: point_type(),
  curves :: [curve()]
  }).

-type multi_curve() :: #multi_curve{}.

-record(polygon,{
  point_type :: point_type(),
  rings :: [line_string()]
  }).

-type polygon() :: #polygon{}.

-record(multi_polygon,{
  point_type :: point_type(),
  polygons :: [polygon()]
  }).

-type multi_polygon() :: #multi_polygon{}.

-record(triangle,{
  point_type :: point_type(),
  rings :: [line_string()]
  }).

-type triangle() :: #triangle{}.

-record(curve_polygon,{
  point_type :: point_type(),
  rings :: [curve()]
  }).

-type curve_polygon() :: #curve_polygon{}.

-record(polyhedral_surface,{
  point_type :: point_type(),
  polygons :: [polygon()]
  }).

-type polyhedral_surface() :: #polyhedral_surface{}.

-type surface() :: polygon() | curve_polygon() | polyhedral_surface().

-record(multi_surface,{
  point_type :: point_type(),
  surfaces :: [surface()]
  }).

-type multi_surface() :: #multi_surface{}.

-record(tin,{
  point_type :: point_type(),
  triangles :: [triangle()]
  }).

-type tin() :: #tin{}.

-type geometry() :: point() |
                    line_string() |
                    triangle() |
                    tin() |
                    curve() |
                    surface() |
                    multi_point() |
                    multi_line_string() |
                    multi_polygon() |
                    multi_curve() |
                    multi_surface() |
                    geometry_collection().

-type geometry_collection() :: [geometry()].

