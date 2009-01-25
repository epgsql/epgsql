{application, epgsql,
 [{description, "PostgreSQL Client"},
  {vsn, "1.0"},
  {modules, [pgsql, pgsql_binary, pgsql_connection, pgsql_datetime, pgsql_types]},
  {registered, []},
  {applications, [kernel, stdlib]},
  {included_applications, []}]}.
