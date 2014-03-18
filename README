Erlang PostgreSQL Database Client

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

  + 3 API sets: pgsql, apgsql and ipgsql:
    pgsql maintains backwards compatibility with the original driver API,
    apgsql delivers complete results as regular erlang messages,
    ipgsql delivers results as messages incrementally (row by row)
  + internal queue of client requests, so you don't need to wait for the
    response to send the next request
  + single process to hold driver state and receive socket data
  + execution of several parsed statements as a batch
  + binding timestamps in erlang:now() format
  see CHANGES for full list.

* Differences between devel branch and mabrek's original async fork:

  + Unnamed statements are used unless specified otherwise.  This may
    cause problems for people attempting to use the same connection
    concurrently, which will no longer work.

* Known problems

  A timeout supplied at connect time works as a socket connect timeout,
  not a query timeout. It passes all tests from original driver except for
  the 3 failing timeout tests.
  SSL performance can degrade if the driver process has a large inbox
  (thousands of messages).
  Usage of unnamed prepared statement and portals leads to unpredicted results
  in case of concurrent access to same connection.


* Connect

  {ok, C} = pgsql:connect(Host, [Username], [Password], Opts).

  Host      - host to connect to.
  Username  - username to connect as, defaults to $USER.
  Password  - optional password to authenticate with.
  Opts      - property list of extra options. Supported properties:

    + {database, String}
    + {port,     Integer}
    + {ssl,      Atom}       true | false | required
    + {ssl_opts, List}       see ssl application docs in OTP
    + {timeout,  Integer}    milliseconds, defaults to 5000
    + {async,    Pid}        see Server Notifications section

  {ok, C} = pgsql:connect("localhost", "username", [{database, "test_db"}]).
  ok = pgsql:close(C).

  The timeout parameter will trigger an {error, timeout} result when the
  socket fails to connect within Timeout milliseconds.

  Asynchronous connect example (applies to ipgsql too):

  {ok, C} = apgsql:start_link(),
  Ref = apgsql:connect(C, "localhost", "username", [{database, "test_db"}]),
  receive
    {C, Ref, connected} ->
        {ok, C};
    {C, Ref, Error = {error, _}} ->
        Error;
    {'EXIT', C, _Reason} ->
        {error, closed}
  end.


* Simple Query

  {ok, Columns, Rows}        = pgsql:squery(C, "select ...").
  {ok, Count}                = pgsql:squery(C, "update ...").
  {ok, Count, Columns, Rows} = pgsql:squery(C, "insert ... returning ...").

  {error, Error}             = pgsql:squery(C, "invalid SQL").

  Columns       - list of column records, see pgsql.hrl for definition.
  Rows          - list of tuples, one for each row.
  Count         - integer count of rows inserted/updated/etc

  The simple query protocol returns all columns as text (Erlang binaries)
  and does not support binding parameters.

  Several queries separated by semicolon can be executed by squery.

  [{ok, _, [{<<"1">>}]}, {ok, _, [{<<"2">>}]}] =
    pgsql:squery(C, "select 1; select 2").

  apgsql:squery returns result as a single message:

  Ref = apgsql:squery(C, Sql),
  receive
    {C, Ref, Result} -> Result
  end.
  Result has same format as return value of pgsql:squery.

  ipgsql:squery returns results incrementally for each query inside Sql and
  for each row:

  Ref = ipgsql:squery(C, Sql),
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

  {ok, Columns, Rows}        = pgsql:equery(C, "select ...", [Parameters]).
  {ok, Count}                = pgsql:equery(C, "update ...", [Parameters]).
  {ok, Count, Columns, Rows} = pgsql:equery(C, "insert ... returning ...", [Parameters]).

  {error, Error}             = pgsql:equery(C, "invalid SQL", [Parameters]).

  Parameters    - optional list of values to be bound to $1, $2, $3, etc.

  The extended query protocol combines parse, bind, and execute using
  the unnamed prepared statement and portal. A "select" statement returns
  {ok, Columns, Rows}, "insert/update/delete" returns {ok, Count} or
  {ok, Count, Columns, Rows} when a "returning" clause is present. When
  an error occurs, all statements result in {error, #error{}}.

  PostgreSQL's binary format is used to return integers as Erlang
  integers, floats as floats, bytea/text/varchar columns as binaries,
  bools as true/false, etc. For details see pgsql_binary.erl and the
  Data Representation section below.

  Asynchronous api equery requires you to parse statement beforehand

  Ref = apgsql:equery(C, Statement, [Parameters]),
  receive
    {C, Ref, Res} -> Res
  end.
  Statement - parsed statement (see parse below)
  Res has same format as return value of pgsql:equery.

  ipgsql:equery(C, Statement, [Parameters]) sends same set of messages as
  squery including final {C, Ref, done}.


* Parse/Bind/Execute

  {ok, Statement} = pgsql:parse(C, [StatementName], Sql, [ParameterTypes]).

  StatementName   - optional, reusable, name for the prepared statement.
  ParameterTypes  - optional list of PostgreSQL types for each parameter.

  For valid type names see pgsql_types.erl.

  apgsql:parse sends {C, Ref, {ok, Statement} | {error, Reason}}.
  ipgsql:parse sends:
    {C, Ref, {types, Types}}
    {C, Ref, {columns, Columns}}
    {C, Ref, no_data} if statement will not return rows
    {C, Ref, {error, Reason}}

  ok = pgsql:bind(C, Statement, [PortalName], ParameterValues).

  PortalName      - optional name for the result portal.

  both apgsql:bind and ipgsql:bind send {C, Ref, ok | {error, Reason}}

  {ok | partial, Rows} = pgsql:execute(C, Statement, [PortalName], [MaxRows]).
  {ok, Count}          = pgsql:execute(C, Statement, [PortalName]).
  {ok, Count, Rows}    = pgsql:execute(C, Statement, [PortalName]).

  PortalName      - optional portal name used in bind/4.
  MaxRows         - maximum number of rows to return (0 for all rows).

  execute returns {partial, Rows} when more rows are available.

  apgsql:execute sends {C, Ref, Result} where Result has same format as
  return value of pgsql:execute.

  ipgsql:execute sends
    {C, Ref, {data, Row}}
    {C, Ref, {error, Reason}}
    {C, Ref, suspended} partial result was sent, more rows are available
    {C, Ref, {complete, {_Type, Count}}}
    {C, Ref, {complete, _Type}}

  ok = pgsql:close(C, Statement).
  ok = pgsql:close(C, statement | portal, Name).
  ok = pgsql:sync(C).

  All pgsql functions return {error, Error} when an error occurs.

  apgsql and ipgsql close and sync functions send {C, Ref, ok}.


* Batch execution

  Batch execution is bind + execute for several prepared statements.
  It uses unnamed portals and MaxRows = 0.

  Results = pgsql:execute_batch(C, Batch).

  Batch   - list of {Statement, ParameterValues}
  Results - list of {ok, Count} or {ok, Count, Rows}

  Example

  {ok, S1} = pgsql:parse(C, "one", "select $1", [int4]),
  {ok, S2} = pgsql:parse(C, "two", "select $1 + $2", [int4, int4]),
  [{ok, [{1}]}, {ok, [{3}]}] =
    pgsql:execute_batch(C, [{S1, [1]}, {S2, [1, 2]}]).

  apgsql:execute_batch sends {C, Ref, Results}
  ipgsql:execute_batch sends
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

  record      = {int2, time, text, ...} (decode only)

  timestamp and timestamptz parameters can take erlang:now() format {MegaSeconds, Seconds, MicroSeconds}

* Errors

  Errors originating from the PostgreSQL backend are returned as {error, #error{}},
  see pgsql.hrl for the record definition. epgsql functions may also return
  {error, What} where What is one of the following:

  {unsupported_auth_method, Method}     - required auth method is unsupported
  timeout                               - request timed out
  closed                                - connection was closed
  sync_required                         - error occured and pgsql:sync must be called

* Server Notifications

  PostgreSQL may deliver two types of asynchronous message: "notices" in response
  to notice and warning messages generated by the server, and "notifications" which
  are generated by the LISTEN/NOTIFY mechanism.

  Passing the {async, Pid} option to pgsql:connect will result in these async
  messages being sent to the specified process, otherwise they will be dropped.

  Message formats:

    {pgsql, Connection, {notification, Channel, Pid, Payload}}

      Connection  - connection the notification occurred on

      Channel     - channel the notification occurred on
      Pid         - database session pid that sent notification
      Payload     - optional payload, only available from PostgreSQL >= 9.0

    {pgsql, Connection, {notice, Error}}

      Connection  - connection the notice occurred on
      Error       - an #error{} record, see pgsql.hrl


* Mailing list

  https://groups.google.com/forum/#!forum/epgsql

