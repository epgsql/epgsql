-record(column, {
    name :: binary(),
    type :: epgsql:epgsql_type(),
    oid :: integer(),
    size :: -1 | pos_integer(),
    modifier :: -1 | pos_integer(),
    format :: integer()
}).

-record(statement, {
    name :: string(),
    columns :: [#column{}],
    types :: [epgsql:epgsql_type()],
    parameter_info :: [epgsql_oid_db:oid_entry()]
}).

-record(error, {
    % see client_min_messages config option
    severity :: debug | log | info | notice | warning | error | fatal | panic,
    code :: binary(),
    codename :: atom(),
    message :: binary(),
    extra :: [{severity | detail | hint | position | internal_position | internal_query
               | where | schema_name | table_name | column_name | data_type_name
               | constraint_name | file | line | routine,
               binary()}]
}).
