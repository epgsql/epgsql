-type epgsql_type() :: atom() | {array, atom()} | {unknown_oid, integer()}.

-record(column, {
    name :: binary(),
    type :: epgsql_type(),
    size :: -1 | pos_integer(),
    modifier :: -1 | pos_integer(),
    format :: integer()
}).

-record(statement, {
    name :: string(),
    columns :: [#column{}],
    types :: [epgsql_type()]
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
