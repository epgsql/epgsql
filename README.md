# Erlang PostgreSQL Database Client

Asynchronous fork of https://github.com/wg/epgsql originally here:
https://github.com/mabrek/epgsql and subsequently forked in order to
provide a common fork for community development.

* Motivation

  When you need to execute several queries, it involves a number network
  round-trips between the application and the database.
  The PostgreSQL frontend/backend protocol supports request pipelining.
  This means that you don't need to wait for the previous command to finish
  before sending the next command. This version of the driver makes full use
  of the protocol feature that allows faster execution.


* Difference highlights

  + 3 API sets: epgsql, epgsqla and epgsqli:
    epgsql maintains backwards compatibility with the original driver API,
    epgsqla delivers complete results as regular erlang messages,
    epgsqli delivers results as messages incrementally (row by row)
  + internal queue of client requests, so you don't need to wait for the
    response to send the next request
  + single process to hold driver state and receive socket data
  + execution of several parsed statements as a batch
  + binding timestamps in `erlang:now()` format
  see CHANGES for full list.

* Differences between devel branch and mabrek's original async fork:

  + Unnamed statements are used unless specified otherwise.  This may
    cause problems for people attempting to use the same connection
    concurrently, which will no longer work.

* Known problems

  SSL performance can degrade if the driver process has a large inbox
  (thousands of messages).

* Connect

         {ok, C} = epgsql:connect(Host, [Username], [Password], Opts).

  Host      - host to connect to.
  Username  - username to connect as, defaults to $USER.
  Password  - optional password to authenticate with.
  Opts      - property list of extra options. Supported properties:

    + `{database, String}`
    + `{port,     Integer}`
    + `{ssl,      Atom}`       true | false | required
    + `{ssl_opts, List}`       see ssl application docs in OTP
    + `{timeout,  Integer}`    milliseconds, defaults to 5000
    + `{async,    Pid}`        see Server Notifications section
  
  
  Example:
  
      {ok, C} = epgsql:connect("localhost", "username", [{database, "test_db"}]).
      ok = epgsql:close(C).

  The timeout parameter will trigger an `{error, timeout}` result when the
  socket fails to connect within Timeout milliseconds.

  Asynchronous connect example (applies to epgsqli too):

        {ok, C} = epgsqla:start_link(),
        Ref = epgsqla:connect(C, "localhost", "username", [{database, "test_db"}]),
        receive
          {C, Ref, connected} ->
              {ok, C};
          {C, Ref, Error = {error, _}} ->
              Error;
          {'EXIT', C, _Reason} ->
              {error, closed}
        end.


* Simple Query

        {ok, Columns, Rows}        = epgsql:squery(C, "select ...").
        {ok, Count}                = epgsql:squery(C, "update ...").
        {ok, Count, Columns, Rows} = epgsql:squery(C, "insert ... returning ...").

        {error, Error}             = epgsql:squery(C, "invalid SQL").

  + `Columns`       - list of column records, see epgsql.hrl for definition.
  + `Rows`          - list of tuples, one for each row.
  + `Count`         - integer count of rows inserted/updated/etc

  The simple query protocol returns all columns as text (Erlang binaries)
  and does not support binding parameters.

  Several queries separated by semicolon can be executed by squery.

        [{ok, _, [{<<"1">>}]}, {ok, _, [{<<"2">>}]}] =
          epgsql:squery(C, "select 1; select 2").

  `epgsqla:squery` returns result as a single message:

        Ref = epgsqla:squery(C, Sql),
        receive
          {C, Ref, Result} -> Result
        end.

  `Result` has same format as return value of epgsql:squery.

  `epgsqli:squery` returns results incrementally for each query inside Sql and
  for each row:

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
              %% execution of all queries from Sql has finished
              done;
        end.


* Extended Query

        {ok, Columns, Rows}        = epgsql:equery(C, "select ...", [Parameters]).
        {ok, Count}                = epgsql:equery(C, "update ...", [Parameters]).
        {ok, Count, Columns, Rows} = epgsql:equery(C, "insert ... returning ...", [Parameters]).

        {error, Error}             = epgsql:equery(C, "invalid SQL", [Parameters]).

  + `Parameters`    - optional list of values to be bound to $1, $2, $3, etc.

  The extended query protocol combines parse, bind, and execute using
  the unnamed prepared statement and portal. A "select" statement returns
  `{ok, Columns, Rows}`, "insert/update/delete" returns `{ok, Count}` or
  `{ok, Count, Columns, Rows}` when a "returning" clause is present. When
  an error occurs, all statements result in `{error, #error{}}`.

  PostgreSQL's binary format is used to return integers as Erlang
  integers, floats as floats, bytea/text/varchar columns as binaries,
  bools as true/false, etc. For details see `epgsql_binary.erl` and the
  Data Representation section below.

  Asynchronous api equery requires you to parse statement beforehand

        Ref = epgsqla:equery(C, Statement, [Parameters]),
        receive
          {C, Ref, Res} -> Res
        end.

  + `Statement` - parsed statement (see parse below)
  + `Res` has same format as return value of `epgsql:equery`.

  `epgsqli:equery(C, Statement, [Parameters])` sends same set of messages as
  squery including the final `{C, Ref, done}`.


* Parse/Bind/Execute

         {ok, Statement} = epgsql:parse(C, [StatementName], Sql, [ParameterTypes]).

  + `StatementName`   - optional, reusable, name for the prepared statement.
  + `ParameterTypes`  - optional list of PostgreSQL types for each parameter.

  For valid type names see `epgsql_types.erl`.

  `epgsqla:parse` sends `{C, Ref, {ok, Statement} | {error, Reason}}`.
  `epgsqli:parse` sends:

        {C, Ref, {types, Types}}
        {C, Ref, {columns, Columns}}
        {C, Ref, no_data} if statement will not return rows
        {C, Ref, {error, Reason}}

        ok = epgsql:bind(C, Statement, [PortalName], ParameterValues).

  + `PortalName`      - optional name for the result portal.

  both `epgsqla:bind` and `epgsqli:bind` send `{C, Ref, ok | {error, Reason}}`

        {ok | partial, Rows} = epgsql:execute(C, Statement, [PortalName], [MaxRows]).
        {ok, Count}          = epgsql:execute(C, Statement, [PortalName]).
        {ok, Count, Rows}    = epgsql:execute(C, Statement, [PortalName]).

  + `PortalName`      - optional portal name used in `bind/4`.
  + `MaxRows`         - maximum number of rows to return (0 for all rows).

  execute returns `{partial, Rows}` when more rows are available.

  `epgsqla:execute` sends `{C, Ref, Result}` where `Result` has the same
  format as the return value of `epgsql:execute`.

  `epgsqli:execute` sends

        {C, Ref, {data, Row}}
        {C, Ref, {error, Reason}}
        {C, Ref, suspended} partial result was sent, more rows are available
        {C, Ref, {complete, {_Type, Count}}}
        {C, Ref, {complete, _Type}}

        ok = epgsql:close(C, Statement).
        ok = epgsql:close(C, statement | portal, Name).
        ok = epgsql:sync(C).

  All epgsql functions return `{error, Error}` when an error occurs.

  epgsqla and epgsqli close and sync functions send `{C, Ref, ok}`.


* Batch execution

  Batch execution is bind + execute for several prepared statements.
  It uses unnamed portals and MaxRows = 0.

        Results = epgsql:execute_batch(C, Batch).

  + `Batch`   - list of `{Statement, ParameterValues}`
  + `Results` - list of `{ok, Count}` or `{ok, Count, Rows}`

  Example

        {ok, S1} = epgsql:parse(C, "one", "select $1", [int4]),
        {ok, S2} = epgsql:parse(C, "two", "select $1 + $2", [int4, int4]),
        [{ok, [{1}]}, {ok, [{3}]}] =
          epgsql:execute_batch(C, [{S1, [1]}, {S2, [1, 2]}]).

  `epgsqla:execute_batch` sends `{C, Ref, Results}`
  `epgsqli:execute_batch` sends

        {C, Ref, {data, Row}}
        {C, Ref, {error, Reason}}
        {C, Ref, {complete, {_Type, Count}}}
        {C, Ref, {complete, _Type}}
        {C, Ref, done} - execution of all queries from Batch has finished


* Data Representation

        null        = null
        bool        = true | false
        char        = $A | binary
        intX        = 1
        floatX      = 1.0
        date        = {Year, Month, Day}
        time        = {Hour, Minute, Second.Microsecond}
        timetz      = {time, Timezone}
        timestamp   = {date, time}
        timestamptz = {date, time}
        interval    = {time, Days, Months}
        text        = <<"a">>
        varchar     = <<"a">>
        bytea       = <<1, 2>>
        array       = [1, 2, 3]
        point       = {10.2, 100.12}

        record      = {int2, time, text, ...} (decode only)

        timestamp and timestamptz parameters can take erlang:now() format {MegaSeconds, Seconds, MicroSeconds}

* Errors

  Errors originating from the PostgreSQL backend are returned as `{error, #error{}}`,
  see `epgsql.hrl` for the record definition. epgsql functions may also return
  `{error, What}` where What is one of the following:

  + `{unsupported_auth_method, Method}`     - required auth method is unsupported
  + `timeout`                               - request timed out
  + `closed`                                - connection was closed
  + `sync_required`                         - error occured and epgsql:sync must be called

* Server Notifications

  PostgreSQL may deliver two types of asynchronous message: "notices" in response
  to notice and warning messages generated by the server, and "notifications" which
  are generated by the LISTEN/NOTIFY mechanism.

  Passing the `{async, Pid}` option to `epgsql:connect` will result in these async
  messages being sent to the specified process, otherwise they will be dropped.

  Message formats:

  `{epgsql, Connection, {notification, Channel, Pid, Payload}}`

  + `Connection`  - connection the notification occurred on
  + `Channel`     - channel the notification occurred on
  + `Pid`         - database session pid that sent notification
  +` Payload`     - optional payload, only available from PostgreSQL >= 9.0

          {epgsql, Connection, {notice, Error}}

  + `Connection`  - connection the notice occurred on
  + `Error`       - an `#error{}` record, see `epgsql.hrl`


* Mailing list / forum

https://groups.google.com/forum/#!forum/epgsql

## Test Setup

In order to run the epgsql tests, you will need to set up a local
Postgres database that runs within its own, self-contained directory,
in order to avoid modifying the system installation of Postgres.

1. `./setup_test_db.sh` # This sets up an installation of Postgres in datadir/

2. `./start_test_db.sh` # Starts a Postgres instance on its own port (10432).

3. `make create_testdbs` # Creates the test database environment.

3. `make test` # Runs the tests
