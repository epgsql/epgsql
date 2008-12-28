{application, epgsql,
 [{description, "PostgreSQL Database Client"},
  {vsn, "0.1"},
  {modules, [pgsql, pgsql_binary, pgsql_connection, pgsql_types]},
  {registered, []},
  {mod, {epgsql, []}},
  {applications, [kernel, stdlib, crypto, sasl},
  {included_applications, []}]}.
