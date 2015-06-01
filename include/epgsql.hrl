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
    severity :: fatal | error | atom(), %TODO: concretize
    code :: binary(),
    codename :: atom(),
    message :: binary(),
    extra :: [{detail, binary()} | {hint, binary()} | {position, binary()}]
}).
