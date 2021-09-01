# Erlang PostgreSQL Database Client

Asynchronous fork of [wg/epgsql](https://github.com/wg/epgsql) originally here:
[mabrek/epgsql](https://github.com/mabrek/epgsql) and subsequently forked in order to
provide a common fork for community development.

## pgapp

If you want to get up to speed quickly with code that lets you run
Postgres queries, you might consider trying
[epgsql/pgapp](https://github.com/epgsql/pgapp), which adds the
following, on top of the epgsql driver:

- A 'resource pool' (currently poolboy), which lets you decide how
  many Postgres workers you want to utilize.
- Resilience against the database going down or other problems.  The
  pgapp code will keep trying to reconnect to the database, but will
  not propagate the crash up the supervisor tree, so that, for
  instance, your web site will stay up even if the database is down
  for some reason.  Erlang's "let it crash" is a good idea, but
  external resources going away might not be a good reason to crash
  your entire system.

## Motivation

When you need to execute several queries, it involves a number network
round-trips between the application and the database.
The PostgreSQL frontend/backend protocol supports request pipelining.
This means that you don't need to wait for the previous command to finish
before sending the next command. This version of the driver makes full use
of the protocol feature that allows faster execution.


## Difference highlights

- 3 API sets:
  - **epgsql** maintains backwards compatibility with the original driver API
  - **epgsqla** delivers complete results as regular erlang messages
  - **epgsqli** delivers results as messages incrementally (row by row)
  All API interfaces can be used with the same connection: eg, connection opened with `epgsql`
  can be queried with `epgsql` / `epgsqla` / `epgsqli` in any combinations.
- internal queue of client requests, so you don't need to wait for the response
  to send the next request (pipelining)
- single process to hold driver state and receive socket data
- execution of several parsed statements as a batch
- binding timestamps in `erlang:now()` format

see `CHANGES` for full list.

### Differences between current epgsql and mabrek's original async fork:

- Unnamed statements are used unless specified otherwise. This may
  cause problems for people attempting to use the same connection
  concurrently, which will no longer work.

## Known problems

- SSL performance can degrade if the driver process has a large inbox
  (thousands of messages).

## Usage

### Connect

```erlang
connect(Opts) -> {ok, Connection :: epgsql:connection()} | {error, Reason :: epgsql:connect_error()}
    when
  Opts ::
    #{host :=     inet:ip_address() | inet:hostname(),
      username := iodata(),
      password => iodata() | fun( () -> iodata() ),
      database => iodata(),
      port =>     inet:port_number(),
      ssl =>      boolean() | required,
      ssl_opts => [ssl:tls_client_option()], % @see OTP ssl documentation
      tcp_opts => [gen_tcp:option()],    % @see OTP gen_tcp module documentation
      timeout =>  timeout(),             % socket connect timeout, default: 5000 ms
      async =>    pid() | atom(),        % process to receive LISTEN/NOTIFY msgs
      codecs =>   [{epgsql_codec:codec_mod(), any()}]}
      nulls =>    [any(), ...],          % NULL terms
      replication => Replication :: string()} % Pass "database" to connect in replication mode
    | list().

connect(Host, Username, Password, Opts) -> {ok, C} | {error, Reason}.
```

example:

```erlang
{ok, C} = epgsql:connect(#{
    host => "localhost",
    username => "username",
    password => "psss",
    database => "test_db",
    timeout => 4000
}),
...
ok = epgsql:close(C).
```

Only `host` and `username` are mandatory, but most likely you would need `database` and `password`.

- `password` - DB user password. It might be provided as string / binary or as a fun that returns
   string / binary. Internally, plain password is wrapped to anonymous fun before it is sent to connection
   process, so, if `connect` command crashes, plain password will not appear in crash logs.
- `timeout` parameter will trigger an `{error, timeout}` result when the
   socket fails to connect within provided milliseconds.
- `ssl` if set to `true`, perform an attempt to connect in ssl mode, but continue unencrypted
  if encryption isn't supported by server. if set to `required` connection will fail if encryption
  is not available.
- `ssl_opts` will be passed as is to `ssl:connect/3`
- `tcp_opts` will be passed as is to `gen_tcp:connect/3`. Some options are forbidden, such as
  `mode`, `packet`, `header`, `active`. When `tcp_opts` is not provided, epgsql does some tuning
  (eg, sets TCP `keepalive` and auto-tunes `buffer`), but when `tcp_opts` is provided, no
  additional tweaks are added by epgsql itself, other than necessary ones (`active`, `packet` and `mode`).
- `async` see [Server notifications](#server-notifications)
- `codecs` see [Pluggable datatype codecs](#pluggable-datatype-codecs)
- `nulls` terms which will be used to represent SQL `NULL`. If any of those has been encountered in
   placeholder parameters (`$1`, `$2` etc values), it will be interpreted as `NULL`.
   1st element of the list will be used to represent NULLs received from the server. It's not recommended
   to use `"string"`s or lists. Try to keep this list short for performance!
   Default is `[null, undefined]`, i.e. encode `null` or `undefined` in parameters as `NULL`
   and decode `NULL`s as atom `null`.
- `replication` see [Streaming replication protocol](#streaming-replication-protocol)
- `application_name` is an optional string parameter. It is usually set by an application upon
   connection to the server. The name will be displayed in the `pg_stat_activity`
   view and included in CSV log entries.

Options may be passed as proplist or as map with the same key names.

Asynchronous connect example (applies to **epgsqli** too):

```erlang
  {ok, C} = epgsqla:start_link(),
  Ref = epgsqla:connect(C, "localhost", "username", "psss", #{database => "test_db"}),
  receive
    {C, Ref, connected} ->
        {ok, C};
    {C, Ref, Error = {error, _}} ->
        Error;
    {'EXIT', C, _Reason} ->
        {error, closed}
  end.
```

### Simple Query

```erlang
-include_lib("epgsql/include/epgsql.hrl").

-type query() :: string() | iodata().
-type squery_row() :: tuple() % tuple of binary().

-type ok_reply(RowType) ::
    {ok, ColumnsDescription :: [epgsql:column()], RowsValues :: [RowType]} |                            % select
    {ok, Count :: non_neg_integer()} |                                                            % update/insert/delete
    {ok, Count :: non_neg_integer(), ColumnsDescription :: [epgsql:column()], RowsValues :: [RowType]}. % update/insert/delete + returning
-type error_reply() :: {error, query_error()}.
-type reply(RowType) :: ok_reply() | error_reply().

-spec squery(connection(), query()) -> reply(squery_row()) | [reply(squery_row())].
%% @doc runs simple `SqlQuery' via given `Connection'
squery(Connection, SqlQuery) -> ...
```

examples:

```erlang
epgsql:squery(C, "insert into account (name) values  ('alice'), ('bob')").
> {ok,2}
```

```erlang
epgsql:squery(C, "select * from account").
> {ok,
    [#column{name = <<"id">>, type = int4, …},#column{name = <<"name">>, type = text, …}],
    [{<<"1">>,<<"alice">>},{<<"2">>,<<"bob">>}]
}
```

```erlang
epgsql:squery(C,
    "insert into account(name)"
    "    values ('joe'), (null)"
    "    returning *").
> {ok,2,
    [#column{name = <<"id">>, type = int4, …}, #column{name = <<"name">>, type = text, …}],
    [{<<"3">>,<<"joe">>},{<<"4">>,null}]
}
```

```erlang
epgsql:squery(C, "SELECT * FROM _nowhere_").
> {error,
   #error{severity = error,code = <<"42P01">>,
          codename = undefined_table,
          message = <<"relation \"_nowhere_\" does not exist">>,
          extra = [{file,<<"parse_relation.c">>},
                   {line,<<"1160">>},
                   {position,<<"15">>},
                   {routine,<<"parserOpenTable">>}]}}
```

The simple query protocol returns all columns as binary strings
and does not support parameters binding.

Several queries separated by semicolon can be executed by squery.

```erlang
[{ok, _, [{<<"1">>}]}, {ok, _, [{<<"2">>}]}] = epgsql:squery(C, "select 1; select 2").
```

`epgsqla:squery/2` returns result as a single message:

```erlang
  Ref = epgsqla:squery(C, Sql),
  receive
    {C, Ref, Result} -> Result
  end.
```

Result has the same format as return value of `epgsql:squery/2`.

`epgsqli:squery/2` returns results incrementally for each query inside Sql and for each row:

```erlang
Ref = epgsqli:squery(C, Sql),
receive
  {C, Ref, {columns, Columns}} ->
      %% columns description
      Columns;
  {C, Ref, {data, Row}} ->
      %% single data row
      Row;
  {C, Ref, {error, _E} = Error} ->
      Error;
  {C, Ref, {complete, {_Type, Count}}} ->
      %% execution of one insert/update/delete has finished
      {ok, Count}; % affected rows count
  {C, Ref, {complete, _Type}} ->
      %% execution of one select has finished
      ok;
  {C, Ref, done} ->
      %% execution of all queries from Sql has been finished
      done;
end.
```

### Extended Query

```erlang
{ok, Columns, Rows}        = epgsql:equery(C, "select ...", [Parameters]).
{ok, Count}                = epgsql:equery(C, "update ...", [Parameters]).
{ok, Count, Columns, Rows} = epgsql:equery(C, "insert ... returning ...", [Parameters]).
{error, Error}             = epgsql:equery(C, "invalid SQL", [Parameters]).
```

`Parameters` - optional list of values to be bound to `$1`, `$2`, `$3`, etc.

The extended query protocol combines parse, bind, and execute using
the unnamed prepared statement and portal. A `select` statement returns
`{ok, Columns, Rows}`, `insert/update/delete` returns `{ok, Count}` or
`{ok, Count, Columns, Rows}` when a `returning` clause is present. When
an error occurs, all statements result in `{error, #error{}}`.

```erlang
epgsql:equery(C, "select id from account where name = $1", ["alice"]),
> {ok,
    [#column{name = <<"id">>, type = int4, …}],
    [{1}]
}
```

PostgreSQL's binary format is used to return integers as Erlang
integers, floats as floats, bytes/text/varchar columns as binaries,
bools as true/false, etc. For details see `pgsql_binary.erl` and the
Data Representation section below.

Asynchronous API `epgsqla:equery/3` requires you to parse statement beforehand

```erlang
#statement{types = Types} = Statement,
TypedParameters = lists:zip(Types, Parameters),
Ref = epgsqla:equery(C, Statement, [TypedParameters]),
receive
  {C, Ref, Res} -> Res
end.
```

- `Statement` - parsed statement (see parse below)
- `Res` has same format as return value of `epgsql:equery/3`.

`epgsqli:equery(C, Statement, [TypedParameters])` sends same set of messages as
squery including final `{C, Ref, done}`.

### Prepared Query

```erlang
{ok, Columns, Rows}        = epgsql:prepared_query(C, Statement :: #statement{} | string(), [Parameters]).
{ok, Count}                = epgsql:prepared_query(C, Statement, [Parameters]).
{ok, Count, Columns, Rows} = epgsql:prepared_query(C, Statement, [Parameters]).
{error, Error}             = epgsql:prepared_query(C, "non_existent_query", [Parameters]).
```

- `Parameters` - optional list of values to be bound to `$1`, `$2`, `$3`, etc.
- `Statement` - name of query given with ```erlang epgsql:parse(C, StatementName, "select ...", []).```
               (can be empty string) or `#statement{}` record returned by `epgsql:parse`.

With prepared query one can parse a query giving it a name with `epgsql:parse` on start and reuse the name
for all further queries with different parameters.

```erlang
{ok, Stmt} = epgsql:parse(C, "inc", "select $1+1", []).
epgsql:prepared_query(C, Stmt, [4]).
epgsql:prepared_query(C, Stmt, [1]).
```

Asynchronous API `epgsqla:prepared_query/3` requires you to always parse statement beforehand

```erlang
#statement{types = Types} = Statement,
TypedParameters = lists:zip(Types, Parameters),
Ref = epgsqla:prepared_query(C, Statement, [TypedParameters]),
receive
  {C, Ref, Res} -> Res
end.
```

- `Statement` - parsed statement (see parse below)
- `Res` has same format as return value of `epgsql:prepared_query/3`.

`epgsqli:prepared_query(C, Statement, [TypedParameters])` sends same set of messages as
squery including final `{C, Ref, done}`.

### Parse/Bind/Execute

```erlang
{ok, Statement} = epgsql:parse(C, [StatementName], Sql, [ParameterTypes]).
```

- `StatementName`   - optional, reusable, name for the prepared statement.
- `ParameterTypes`  - optional list of PostgreSQL types for each parameter.

For valid type names see `pgsql_types.erl`.

`epgsqla:parse/2` sends `{C, Ref, {ok, Statement} | {error, Reason}}`.

`epgsqli:parse/2` sends:

- `{C, Ref, {types, Types}}`
- `{C, Ref, {columns, Columns}}`
- `{C, Ref, no_data}` if statement will not return rows
- `{C, Ref, {error, Reason}}`

```erlang
ok = epgsql:bind(C, Statement, [PortalName], ParameterValues).
```

- `PortalName`      - optional name for the result portal.

both `epgsqla:bind/3` and `epgsqli:bind/3` send `{C, Ref, ok | {error, Reason}}`

```erlang
{ok | partial, Rows} = epgsql:execute(C, Statement, [PortalName], [MaxRows]).
{ok, Count}          = epgsql:execute(C, Statement, [PortalName]).
{ok, Count, Rows}    = epgsql:execute(C, Statement, [PortalName]).
```

- `PortalName`      - optional portal name used in `epgsql:bind/4`.
- `MaxRows`         - maximum number of rows to return (0 for all rows).

`epgsql:execute/3` returns `{partial, Rows}` when more rows are available.

`epgsqla:execute/3` sends `{C, Ref, Result}` where `Result` has same format as
return value of `epgsql:execute/3`.

`epgsqli:execute/3` sends

- `{C, Ref, {data, Row}}`
- `{C, Ref, {error, Reason}}`
- `{C, Ref, suspended}` partial result was sent, more rows are available
- `{C, Ref, {complete, {_Type, Count}}}`
- `{C, Ref, {complete, _Type}}`

```erlang
ok = epgsql:close(C, Statement).
ok = epgsql:close(C, statement | portal, Name).
ok = epgsql:sync(C).
```

All epgsql functions return `{error, Error}` when an error occurs.

`epgsqla`/`epgsqli` modules' `close` and `sync` functions send `{C, Ref, ok}`.

### Batch execution

Batch execution is `bind` + `execute` for several prepared statements.
It uses unnamed portals and `MaxRows = 0`.

```erlang
Results = epgsql:execute_batch(C, BatchStmt :: [{statement(), [bind_param()]}]).
{Columns, Results} = epgsql:execute_batch(C, statement() | sql_query(), Batch :: [ [bind_param()] ]).
```

- `BatchStmt` - list of `{Statement, ParameterValues}`, each item has it's own `#statement{}`
- `Batch` - list of `ParameterValues`, each item executes the same common `#statement{}` or SQL query
- `Columns` - list of `#column{}` descriptions of `Results` columns
- `Results` - list of `{ok, Count}` or `{ok, Count, Rows}`

There are 2 versions:

`execute_batch/2` - each item in a batch has it's own named statement (but it's allowed to have duplicates)

example:

```erlang
{ok, S1} = epgsql:parse(C, "one", "select $1::integer", []),
{ok, S2} = epgsql:parse(C, "two", "select $1::integer + $2::integer", []),
[{ok, [{1}]}, {ok, [{3}]}] = epgsql:execute_batch(C, [{S1, [1]}, {S2, [1, 2]}]).
ok = epgsql:close(C, "one").
ok = epgsql:close(C, "two").
```

`execute_batch/3` - each item in a batch executed with the same common SQL query or `#statement{}`.
It's allowed to use unnamed statement.

example (the most efficient way to make batch inserts with epgsql):

```erlang
{ok, Stmt} = epgsql:parse(C, "my_insert", "INSERT INTO account (name, age) VALUES ($1, $2) RETURNING id", []).
{[#column{name = <<"id">>}], [{ok, [{1}]}, {ok, [{2}]}, {ok, [{3}]}]} =
    epgsql:execute_batch(C, Stmt, [ ["Joe", 35], ["Paul", 26], ["Mary", 24] ]).
ok = epgsql:close(C, "my_insert").
```

equivalent:

```erlang
epgsql:execute_batch(C, "INSERT INTO account (name, age) VALUES ($1, $2) RETURNING id",
                     [ ["Joe", 35], ["Paul", 26], ["Mary", 24] ]).
```

In case one of the batch items causes an error, all the remaining queries of
that batch will be ignored. So, last element of the result list will be 
`{error, #error{}}` and the length of the result list might be shorter that 
the length of the batch. For a better illustration of such scenario please 
refer to `epgsql_SUITE:batch_error/1`

`epgsqla:execute_batch/{2,3}` sends `{C, Ref, Results}`

`epgsqli:execute_batch/{2,3}` sends

- `{C, Ref, {data, Row}}`
- `{C, Ref, {error, Reason}}`
- `{C, Ref, {complete, {_Type, Count}}}`
- `{C, Ref, {complete, _Type}}`
- `{C, Ref, done}` - execution of all queries from Batch has finished

### Query cancellation

```erlang
epgsql:cancel(connection()) -> ok.
```

PostgreSQL protocol supports [cancellation](https://www.postgresql.org/docs/current/protocol-flow.html#id-1.10.5.7.9)
of currently executing command. `cancel/1` sends a cancellation request via the
new temporary TCP/TLS_over_TCP connection asynchronously, it doesn't await for the command to
be cancelled. Instead, client should expect to get
`{error, #error{code = <<"57014">>, codename = query_canceled}}` back from
the command that was cancelled. However, normal response can still be received as well.
While it's not so straightforward to use with synchronous `epgsql` API, it plays
quite nicely with asynchronous `epgsqla` API. For example, that's how a query with
soft timeout can be implemented:

```erlang
squery(C, SQL, Timeout) ->
    Ref = epgsqla:squery(C, SQL),
    receive
       {C, Ref, Result} -> Result
    after Timeout ->
        ok = epgsql:cancel(C),
        % We can still receive {ok, …} as well as
        % {error, #error{codename = query_canceled}} or any other error
        receive
            {C, Ref, Result} -> Result
        end
    end.
```

This API should be used with extreme care when pipelining is in use: it only cancels
currently executing command, all the subsequent pipelined commands will continue
their normal execution. And it's not always easy to see which command exactly is
executing when we are issuing the cancellation request.

## Data Representation

Data representation may be configured using [pluggable datatype codecs](doc/pluggable_types.md),
so following is just default mapping:

PG type       | Representation
--------------|-------------------------------------
  null        | `null`
  bool        | `true` | `false`
  char        | `$A` | `binary`
  intX        | `1`
  floatX      | `1.0` | `nan` | `minus_infinity` | `plus_infinity`
  date        | `{Year, Month, Day}`
  time        | `{Hour, Minute, Second.Microsecond}`
  timetz      | `{time, Timezone}`
  timestamp   | `{date, time}`
  timestamptz | `{date, time}`
  interval    | `{time, Days, Months}`
  text        | `<<"a">>`
  varchar     | `<<"a">>`
  bytea       | `<<1, 2>>`
  array       | `[1, 2, 3]`
  record      | `{int2, time, text, ...}` (decode only)
  point       |  `{10.2, 100.12}`
  int4range   | `[1,5)`
  hstore      | `{[ {binary(), binary() \| null} ]}` (configurable)
  json/jsonb  | `<<"{ \"key\": [ 1, 1.0, true, \"string\" ] }">>` (configurable)
  uuid        | `<<"123e4567-e89b-12d3-a456-426655440000">>`
  inet        | `inet:ip_address()`
  cidr        | `{ip_address(), Mask :: 0..32}`
  macaddr(8)  | tuple of 6 or 8 `byte()`
  geometry    | `ewkb:geometry()`
  tsrange     | `{{Hour, Minute, Second.Microsecond}, {Hour, Minute, Second.Microsecond}}`
  tstzrange   | `{{Hour, Minute, Second.Microsecond}, {Hour, Minute, Second.Microsecond}}`
  daterange   | `{{Year, Month, Day}, {Year, Month, Day}}`

`null` can be configured. See `nulls` `connect/1` option.

`timestamp` and `timestamptz` parameters can take `erlang:now()` format: `{MegaSeconds, Seconds, MicroSeconds}`

`int4range` is a range type for ints that obeys inclusive/exclusive semantics,
bracket and parentheses respectively. Additionally, infinities are represented by the atoms `minus_infinity`
and `plus_infinity`

`tsrange`, `tstzrange`, `daterange` are range types for `timestamp`, `timestamptz` and `date`
respectively. They can return `empty` atom as the result from a database if bounds are equal

`hstore` type can take map or jiffy-style objects as input. Output can be tuned by
providing `return :: map | jiffy | proplist` option to choose the format to which
hstore should be decoded. `nulls :: [atom(), ...]` option can be used to select the
terms which should be interpreted as SQL `NULL` - semantics is the same as
for `connect/1` `nulls` option.

`json` and `jsonb` types can optionally use a custom JSON encoding/decoding module to accept
and return erlang-formatted JSON. The module must implement the callbacks in `epgsql_codec_json`,
which most popular open source JSON parsers will already, and you can specify it in the codec
configuration like this:

```erlang
{epgsql_codec_json, JsonMod}

% With options
{epgsql_codec_json, JsonMod, EncodeOpts, DecodeOpts}

% Real world example using jiffy to return a map on decode
{epgsql_codec_json, {jiffy, [], [return_maps]}}
```

Note that the decoded terms will be message-passed to the receiving process (i.e. copied), which may exhibit a performance hit if decoding large terms very frequently.

## Errors

Errors originating from the PostgreSQL backend are returned as `{error, #error{}}`,
see `epgsql.hrl` for the record definition. `epgsql` functions may also return
`{error, What}` where `What` is one of the following:

- `{unsupported_auth_method, Method}`     - required auth method is unsupported
- `timeout`                               - request timed out
- `closed`                                - connection was closed
- `sync_required`                         - error occured and epgsql:sync must be called

## Server Notifications

PostgreSQL may deliver two types of asynchronous message: "notices" in response
to [notice and warning](https://www.postgresql.org/docs/current/static/plpgsql-errors-and-messages.html)
messages generated by the server, and [notifications](https://www.postgresql.org/docs/current/static/sql-notify.html)
which are generated by the `LISTEN/NOTIFY` mechanism.

Passing the `{async, PidOrName}` option to `epgsql:connect/3` will result in these async
messages being sent to the specified pid or registered process, otherwise they will be dropped.

Another way to set notification receiver is to use `set_notice_receiver/2` function.
It returns previous `async` value. Use `undefined` to disable notifications.

```erlang
% receiver is pid()
{ok, Previous} = epgsql:set_notice_receiver(C, self()).

% receiver is registered process
register(notify_receiver, self()).
{ok, Previous1} = epgsqla:set_notice_receiver(C, notify_receiver).

% disable notifications
{ok, Previous2} = epgsqli:set_notice_receiver(C, undefined).
```

Message formats:

```erlang
{epgsql, Connection, {notification, Channel, Pid, Payload}}
```

- `Connection`  - connection the notification occurred on
- `Channel`  - channel the notification occurred on
- `Pid`  - database session pid that sent notification
- `Payload`  - optional payload, only available from PostgreSQL >= 9.0

```erlang
{epgsql, Connection, {notice, Error}}
```

- `Connection`  - connection the notice occurred on
- `Error`       - an `#error{}` record, see `epgsql.hrl`


## Utility functions

### Transaction helpers

```erlang
with_transaction(connection(), fun((connection()) -> Result :: any()), Opts) ->
    Result | {rollback, Reason :: any()} when
Opts :: [{reraise, boolean()},
         {ensure_committed, boolean()},
         {begin_opts, iodata()}] | map().
```

Executes a function in a PostgreSQL transaction. It executes `BEGIN` prior to executing the function,
`ROLLBACK` if the function raises an exception and `COMMIT` if the function returns without an error.
If it is successful, it returns the result of the function. The failure case may differ, depending on
the options passed.

Options (proplist or map):

- `reraise` (default `true`): when set to true, the original exception will be re-thrown after rollback,
  otherwise `{rollback, ErrorReason}` will be returned
- `ensure_committed` (default `false`): even when the callback returns without exception,
  check that transaction was committed by checking the `CommandComplete` status
  of the `COMMIT` command. If the transaction was rolled back, the status will be
  `rollback` instead of `commit` and an `ensure_committed_failed` error will be generated.
- `begin_opts` (default `""`): append extra options to `BEGIN` command (see
  https://www.postgresql.org/docs/current/static/sql-begin.html) as a string by just
  appending them to `"BEGIN "` string. Eg `{begin_opts, "ISOLATION LEVEL SERIALIZABLE"}`.
  Beware of SQL injection! The value of `begin_opts` is not escaped!

### Command status

`epgsql{a,i}:get_cmd_status(C) -> undefined | atom() | {atom(), integer()}`

This function returns the last executed command's status information. It's usualy
the name of SQL command and, for some of them (like UPDATE or INSERT) the
number of affected rows. See [libpq PQcmdStatus](https://www.postgresql.org/docs/current/static/libpq-exec.html#LIBPQ-PQCMDSTATUS).
But there is one interesting case: if you execute `COMMIT` on a failed transaction,
status will be `rollback`, not `commit`.
This is how you can detect failed transactions:

```erlang
{ok, _, _} = epgsql:squery(C, "BEGIN").
{error, _} = epgsql:equery(C, "SELECT 1 / $1::integer", [0]).
{ok, _, _} = epgsql:squery(C, "COMMIT").
{ok, rollback} = epgsql:get_cmd_status(C).
```

### Server parameters

`epgsql{a,i}:get_parameter(C, Name) -> binary() | undefined`

Retrieve actual value of server-side parameters, such as character endoding,
date/time format and timezone, server version and so on. See [libpq PQparameterStatus](https://www.postgresql.org/docs/current/static/libpq-status.html#LIBPQ-PQPARAMETERSTATUS).
Parameter's value may change during connection's lifetime.


## Streaming replication protocol

See [streaming.md](doc/streaming.md).

## Pluggable commands

See [pluggable_commands.md](doc/pluggable_commands.md)

## Pluggable datatype codecs

See [pluggable_types.md](doc/pluggable_types.md)

## Mailing list

  [Google groups](https://groups.google.com/forum/#!forum/epgsql)

## Contributing

epgsql is a community driven effort - we welcome contributions!
Here's how to create a patch that's easy to integrate:

- Create a new branch for the proposed fix.
- Make sure it includes a test and documentation, if appropriate.
- Open a pull request against the `devel` branch of epgsql.
- Passing build in travis

## Test Setup

In order to run the epgsql tests, you will need to install local
Postgres database.

NOTE: you will need the postgis and hstore extensions to run these
tests! On Ubuntu, you can install them with a command like this:

1. `apt-get install postgresql-9.3-postgis-2.1 postgresql-contrib`
1. `make test` # Runs the tests

NOTE 2: It's possible to run tests on exact postgres version by changing $PATH like

   `PATH=$PATH:/usr/lib/postgresql/9.5/bin/ make test`

[![CI](https://github.com/epgsql/epgsql/actions/workflows/ci.yml/badge.svg)](https://github.com/epgsql/epgsql/actions/workflows/ci.yml)
