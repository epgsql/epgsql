# Pluggable commands

Starting from epgsql 4.0.0 it's possible to create custom epgsql commands. The term "command"
signifies a single `request -> response` sequence.
Under the hood it might contain many PostgreSQL protocol command requests and responses,
but from the point of view of an epgsql user, it's a single request that produces a single
response.
Examples of such commands are `connect`, `squery`, `equery`, `prepared_query`,
`parse`/`bind`/`execute` and so on. See [src/commands](src/commands) for a full list and
examples. Basically, almost all epgsql end-user APIs are implemented as a commands.

It is possible to send many commands without waiting for the results of previously sent ones
(pipelining) by using `epgsqla` or `epgsqli` interfaces.

## Possible usecases

Why you may want to implement a custom command? Some ideas:

* You are not satisfied by performance or functionality of epgsql's built-in commands
* To create a version of equery with built-in statement cache
* To create a single-roundtrip equery (currently equery works by combining `parse` and
  `equery` commands)
* To construct some tricky batching commands, eg, bulk-inserts

## This can be done by following steps

If you are not familiar with the PostgreSQL wire protocol, please, read at least the
[Message Flow](https://www.postgresql.org/docs/current/static/protocol-flow.html) and
[Message Formats](https://www.postgresql.org/docs/current/static/protocol-message-formats.html)
sections of the PostgreSQL documentation.
The entire [Frontend/Backend Protocol](https://www.postgresql.org/docs/current/static/protocol.html)
would be nice to know.

### Implement epgsql_command behaviour callback module

See [epgsql_command](src/epgsql_command.erl).

This module should have the following functions exported:

```erlang
init(any()) -> state().
```

Called only once when the command is received and is about to be executed by the epgsql connection
process. Command's arguments are passed as the callback's arguments, see `epgsql_sock:sync_command/3` and
`epgsql_sock:async_command/4`. Should initialize and return command's state that will be
passed to all subsequent callbacks. No PostgreSQL interactions should be done here.

```erlang
execute(pg_sock(), state()) ->
    {ok, pg_sock(), state()}
  | {send, epgsql_wire:packet_type(), iodata(), pg_sock(), state()}
  | {send_multi, [{epgsql_wire:packet_type(), iodata()}], pg_sock(), state()}
  | {stop, Reason :: any(), Response :: any(), pg_sock()}.
```

Client -> Server packets should be sent from this callback by `epgsql_sock:send_multi/2` or
`epgsql_sock:send/3` or by returning equivalent `send` or `send_multi` values.
`epgsql_wire` module is usually used to create wire protocol packets.
Please note that many packets might be sent at once. See `epgsql_cmd_equery` as an example.

This callback might be executed more than once for a single command execution if your command
requires a response for some of the packets to send next packet (more than one round-trip).
Since epgsql is asynchronous under the hood, you can't just do blocking `receive`.
See `handle_message/4 -> {requeue, ...}` and `epgsql_cmd_connect` as an example.

`pg_sock()` is an opaque state of a `epgsql_sock` process. There are some APIs to get or
set some fields on it in `epgsql_sock` module.

```erlang
handle_message(Type :: byte(), Payload :: binary() | query_error(),
               pg_sock(), state()) ->
    {noaction, pg_sock()}
  | {noaction, pg_sock(), state()}
  | {add_row, tuple(), pg_sock(), state()}
  | {add_result, Data :: any(), Notification :: any(), pg_sock(), state()}
  | {finish, Result :: any(), Notification :: any(), pg_sock()}
  | {requeue, pg_sock(), state()}
  | {stop, Reason :: any(), Response :: any(), pg_sock()}
  | {sync_required, Why :: any()}
  | unknown.

```

Server -> Client packet handling code. Packet `Type` byte is the integer ID of a
[protocol packet](https://www.postgresql.org/docs/current/static/protocol-message-formats.html), basically
the 1st byte of a packet. And `Payload` is the remaining bytes of a packet. `epgsql_wire` module
has some helpers that might help decode the packet payload.

In the case when the epgsql connection gets an error packet from the server, it will be decoded and `Payload`
will be `query_error()` instead of binary.

**NEVER** call `epgsql_sock:send/3`/`epgsql_sock:send_multi/2` from this callback! Use
`requeue` return instead: otherwise you will break pipelining!

This callback should return one of the following responses to control command's behaviour:

- `{noaction, pg_sock()}` - to do nothing (this usualy means that packet was ignored)
- `{noaction, pg_sock(), state()}` - do nothing, but update command's state
- `{add_row, tuple(), pg_sock(), state()}` - add a row to current resultset rows accumulator.
  You may get the current accumulated resultset by `epgsql_sock::get_rows(pg_sock())` (except
  when `epgsqli` interface is used).
- `{add_result, Result :: any(), Notification :: any(), pg_sock(), state()}` - add a
  new result to the list of results. Usualy all commands have only a single result, except `squery`, when
  multiple SQL queries were passed, separated by a semicolon and `execute_batch`.
  You will usually will just return something like `{ok, epgsql_sock:get_rows(PgSock)}` or an error as a result. `Notification` is used for `epgsqli` interface.
  You may get the current list of accumulated results with `epgsql_sock:get_results(pg_sock())`.
- `{finish, Results, Notification, pg_sock(), state()}` - returned when command was successfuly
  executed and no more actions needed. `Results` will be returned to a client as a result of command
  execution and the command will be descheduled from epgsql connection process.
  You usually use the result of `epgsql_sock:get_results/1` as a `Results`.
  `Notification` is used for `epgsqli` interface.
- `{requeue, pg_sock(), state()}` - asks the epgsql process to put this command in the execution queue
  once again (with a new state). This means that the `execute/2` callback will be executed again and
  new packets may be sent from client to server. This way you can implement chatty commands with
  multiple `request -> response` sequences. See `epgsql_cmd_connect` as an example.
- `{stop, Reason, Response, pg_sock()}` - returned when some unrecoverable error occured and
  you want to terminate epgsql connection process. `Response` will be returned as a command result
  and `Reason` will be process termination reason.
  Please, try to avoid use of this response if possible.
- `{sync_required, Why}` - returned to finish command execution, flush enqueued but not yet
  executed commands and to set epgsql process to `sync_required` state. In this state it
  will not accept any commands except `epgsql_cmd_sync`.
  This usualy means that multipacket protocol sequence was done out-of-order (eg, `bind` before `parse`),
  so, client and server states are out-of-sync and we need to reset them.
- `unknown` - command got unexpected packet. Connection process will be terminated with
  `{error, {unexpected_message, Type, Payload, state()}}`. Usualy returned from a
  catch-all last clause.

### Command now can be executed

By calling

- `epgsql_sock:sync_command(connection(), command(), Args :: any())` for a
  `gen_server:call`-style, synchronous behaviour (`epgsql`-like API)
- `epgsql_sock:async_command(connection(), cast, command(), Args :: any())` for asynchronous
  behaviour when whole resultset will be delivered as a single erlang message (`epgsqla`-like API)
- `epgsql_sock:async_command(connection(), incremental, command(), Args :: any())` for
  asynchronous behaviour when **each row** and some status info will be delivered as separate erlang
  messages (`epgsqli`-like API)

`command()` is the name of a module, implementing `epgsql_command` behaviour.
`Args` may be any (eg, SQL query / arguments / options), they will be passed to `init/1` callback as-is.

## Tips

* If you are implementing your command outside of a epgsql main tree, it might be handy to
  add `do(Conn, Arg1, Arg2...) -> epgsql_sock:sync_command(Conn, ?MODULE, Args).` to
  incapsulate `epgsql_sock` calls and provide end-user API.
* Don't be afraid of `requeue`. It might make your code more complex, but will make it possible to
  implement complex multistep logic inside of a single command
* `epgsql_sock` module has some APIs that might be used from within commands. Refer to that module's
  source code. `epgsql_wire` has some helpers to encode/decode wire protocol and data packets.
* Packet IDs are defined in `include/protocol.hrl`
* Again, never try to send packets from `handle_message/4` or `init/1` callbacks!
* Note that any error in callback functions will crash the epgsql connection process!
