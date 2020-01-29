%% See https://www.postgresql.org/docs/current/protocol-message-formats.html
%% Description of `RowDescription' packet
-record(column, {
    %% field name
    name :: binary(),
    %% name of the field data type
    type :: epgsql:epgsql_type(),
    %% OID of the field's data type
    oid :: non_neg_integer(),
    %% data type size (see pg_type.typlen). negative values denote variable-width types
    size :: -1 | pos_integer(),
    %% type modifier (see pg_attribute.atttypmod). meaning of the modifier is type-specific
    modifier :: -1 | pos_integer(),
    %% format code being used for the field during server->client transmission.
    %% Currently will be zero (text) or one (binary).
    format :: integer(),
    %% If the field can be identified as a column of a specific table, the OID of the table; otherwise zero.
    %% SELECT relname FROM pg_catalog.pg_class WHERE oid=<table_oid>
    table_oid :: non_neg_integer(),
    %% If table_oid is not zero, the attribute number of the column; otherwise zero.
    %% SELECT attname FROM pg_catalog.pg_attribute
    %% WHERE attrelid=<table_oid> AND attnum=<table_attr_number>
    table_attr_number :: pos_integer()
}).

-record(statement, {
    name :: string(),
    columns :: [#column{}],
    types :: [epgsql:epgsql_type()],
    parameter_info :: [epgsql_oid_db:oid_entry()]
}).


%% See https://www.postgresql.org/docs/current/protocol-error-fields.html
-record(error, {
    % see client_min_messages config option
    severity :: debug | log | info | notice | warning | error | fatal | panic,
    code :: binary(), % See https://www.postgresql.org/docs/current/errcodes-appendix.html
    codename :: atom(),
    message :: binary(),
    extra :: [{severity | detail | hint | position | internal_position | internal_query
               | where | schema_name | table_name | column_name | data_type_name
               | constraint_name | file | line | routine,
               binary()}]
}).
