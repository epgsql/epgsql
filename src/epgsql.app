{application, epgsql,
 [{description, "PostgreSQL Client"},
  {vsn, "0.2"},
  {modules, [pgsql, pgsql_binary, pgsql_connection, pgsql_types]},
  {registered, []},
  {applications, [kernel, stdlib]},
  {included_applications, []}]}.
