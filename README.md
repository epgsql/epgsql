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
- internal queue of client requests, so you don't need to wait for the response
  to send the next request
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
-type host() :: inet:ip_address() | inet:hostname().

-type connect_option() ::
    {database, DBName     :: string()}             |
    {port,     PortNum    :: inet:port_number()}   |
    {ssl,      IsEnabled  :: boolean() | required} |
    {ssl_opts, SslOptions :: [ssl:ssl_option()]}   | % @see OTP ssl app, ssl_api.hrl
    {timeout,  TimeoutMs  :: timeout()}            | % default: 5000 ms
    {async,    Receiver   :: pid()}. % process to receive LISTEN/NOTIFY msgs
    
-spec connect(host(), string(), string(), [connect_option()])
        -> {ok, Connection :: connection()} | {error, Reason :: connect_error()}.    
%% @doc connects to Postgres
%% where
%% `Host'     - host to connect to
%% `Username' - username to connect as, defaults to `$USER'
%% `Password' - optional password to authenticate with
%% `Opts'     - proplist of extra options
%% returns `{ok, Connection}' otherwise `{error, Reason}'
connect(Host, Username, Password, Opts) -> ...
```
example:
```erlang
{ok, C} = epgsql:connect("localhost", "username", "psss", [
    {database, "test_db"},
    {timeout, 4000}
]),
...
ok = epgsql:close(C).
```

The `{timeout, TimeoutMs}` parameter will trigger an `{error, timeout}` result when the
socket fails to connect within `TimeoutMs` milliseconds.

Asynchronous connect example (applies to **epgsqli** too):

```erlang
  {ok, C} = epgsqla:start_link(),
  Ref = epgsqla:connect(C, "localhost", "username", "psss", [{database, "test_db"}]),
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
-type query() :: string() | iodata().
-type squery_row() :: {binary()}.

-record(column, {
    name :: binary(),
    type :: epgsql_type(),
    size :: -1 | pos_integer(),
    modifier :: -1 | pos_integer(),
    format :: integer()
}).

-type ok_reply(RowType) ::
    {ok, Count :: non_neg_integer()} |                                                            % select
    {ok, ColumnsDescription :: [#column{}], RowsValues :: [RowType]} |                            % update/insert
    {ok, Count :: non_neg_integer(), ColumnsDescription :: [#column{}], RowsValues :: [RowType]}. % update/insert + returning
-type error_reply() :: {error, query_error()}.
-type reply(RowType) :: ok_reply() | error_reply().

-spec squery(connection(), query()) -> reply(squery_row()) | [reply(squery_row())].
%% @doc runs simple `SqlQuery' via given `Connection'
squery(Connection, SqlQuery) -> ...
```
examples:
```erlang
InsertRes = epgsql:squery(C, "insert into account (name) values  ('alice'), ('bob')"),
io:format("~p~n", [InsertRes]),
```
> ```
{ok,2}
```

```erlang
SelectRes = epgsql:squery(C, "select * from account"),
io:format("~p~n", [SelectRes]).
```
> ```
{ok,
    [{column,<<"id">>,int4,4,-1,0},{column,<<"name">>,text,-1,-1,0}],
    [{<<"1">>,<<"alice">>},{<<"2">>,<<"bob">>}]
}
```

```erlang
InsertReturningRes = epgsql:squery(C, 
    "insert into account(name)"
    "    values ('joe'), (null)"
    "    returning *"),
io:format("~p~n", [InsertReturningRes]).
```
> ```
{ok,2,
    [{column,<<"id">>,int4,4,-1,0}, {column,<<"name">>,text,-1,-1,0}],
    [{<<"3">>,<<"joe">>},{<<"4">>,null}]
}
```

```erlang
{error, Reason} = epgsql:squery(C, "insert into account values (1, 'bad_pkey')"),
io:format("~p~n", [Reason]).
```
> ```
{error,
    error,
    <<"23505">>,
    <<"duplicate key value violates unique constraint \"account_pkey\"">>,
    [{detail,<<"Key (id)=(1) already exists.">>}]
}
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

## Extended Query

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
SelectRes = epgsql:equery(C, "select id from account where name = $1", ["alice"]),
io:format("~p~n", [SelectRes]).
```
> ```
{ok,
    [{column,<<"id">>,int4,4,-1,1}],
    [{1}]
}
```

PostgreSQL's binary format is used to return integers as Erlang
integers, floats as floats, bytes/text/varchar columns as binaries,
bools as true/false, etc. For details see `pgsql_binary.erl` and the
Data Representation section below.

Asynchronous API `epgsqla:equery/3` requires you to parse statement beforehand

```erlang
Ref = epgsqla:equery(C, Statement, [Parameters]),
receive
  {C, Ref, Res} -> Res
end.
```

- `Statement` - parsed statement (see parse below)
- `Res` has same format as return value of `epgsql:equery/3`.

`epgsqli:equery(C, Statement, [Parameters])` sends same set of messages as
squery including final `{C, Ref, done}`.


## Parse/Bind/Execute

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


## Batch execution

Batch execution is `bind` + `execute` for several prepared statements.
It uses unnamed portals and `MaxRows = 0`.

```erlang
Results = epgsql:execute_batch(C, Batch).
```

- `Batch`   - list of {Statement, ParameterValues}
- `Results` - list of {ok, Count} or {ok, Count, Rows}

example:

```erlang
{ok, S1} = epgsql:parse(C, "one", "select $1", [int4]),
{ok, S2} = epgsql:parse(C, "two", "select $1 + $2", [int4, int4]),
[{ok, [{1}]}, {ok, [{3}]}] = epgsql:execute_batch(C, [{S1, [1]}, {S2, [1, 2]}]).
```

`epgsqla:execute_batch/3` sends `{C, Ref, Results}`
`epgsqli:execute_batch/3` sends
- `{C, Ref, {data, Row}}`
- `{C, Ref, {error, Reason}}`
- `{C, Ref, {complete, {_Type, Count}}}`
- `{C, Ref, {complete, _Type}}`
- `{C, Ref, done}` - execution of all queries from Batch has finished


## Data Representation
PG type       | Representation
--------------|-------------------------------------
  null        | `null`
  bool        | `true` | `false`
  char        | `$A` | `binary`
  intX        | `1`
  floatX      | `1.0`
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

  `timestamp` and `timestamptz` parameters can take `erlang:now()` format: `{MegaSeconds, Seconds, MicroSeconds}`

  `int4range` is a range type for ints (bigint not supported yet) that obeys inclusive/exclusive semantics,
  bracket and parentheses respectively. Additionally, infinities are represented by the atoms `minus_infinity`
  and `plus_infinity`

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
to notice and warning messages generated by the server, and "notifications" which
are generated by the `LISTEN/NOTIFY` mechanism.

Passing the `{async, Pid}` option to `epgsql:connect/3` will result in these async
messages being sent to the specified process, otherwise they will be dropped.

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


## Mailing list

  [Google groups](https://groups.google.com/forum/#!forum/epgsql)

## Contributing

epgsql is a community driven effort - we welcome contributions!
Here's how to create a patch that's easy to integrate:

* Create a new branch for the proposed fix.
* Make sure it includes a test and documentation, if appropriate.
* Open a pull request against the `devel` branch of epgsql.
* Passing build in travis

## Test Setup

In order to run the epgsql tests, you will need to set up a local
Postgres database that runs within its own, self-contained directory,
in order to avoid modifying the system installation of Postgres.

NOTE: you will need the postgis and hstore extensions to run these
tests!  On Ubuntu, you can install them with a command like this:

    apt-get install postgresql-9.3-postgis-2.1 postgresql-contrib

1. `./setup_test_db.sh` # This sets up an installation of Postgres in datadir/

2. `./start_test_db.sh` # Starts a Postgres instance on its own port (10432).

3. `make create_testdbs` # Creates the test database environment.

4. `make test` # Runs the tests

[![Build Status Master](https://travis-ci.org/epgsql/epgsql.svg?branch=master)](https://travis-ci.org/epgsql/epgsql)
[![Build Status Devel](https://travis-ci.org/epgsql/epgsql.svg?branch=devel)](https://travis-ci.org/epgsql/epgsql)
