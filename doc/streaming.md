# Streaming replication protocol

EPGSQL supports PostgreSQL streaming replication protocol.
See https://www.postgresql.org/docs/current/static/protocol-replication.html

## Use cases
***Consistent cache***

Erlang application uses in read-only mode data from PostgreSql DB tables 
(e.g. some configuration, rate/price plans, subscriber’s profiles) 
and needs to store the data in some internal structure (e.g. ETS table). 
The data in PostgreSql DB is updated by some third party applications. 
Erlang application needs to know about modifications in a near real-time mode. 
During start Erlang application uploads initial data from the tables 
and subscribes to receive any modifications for data in these tables and update internal ETS tables accordingly.
Using special output plugins for replication slots (for example: pglogical_output) 
we can filter tables and receive only changes for some particular tables.

***Consistent internal DB***

This case is similar to previous “Consistent cache”, 
but the difference is that amount of data in the table is huge 
and it is inefficient and takes too much time to upload data every time on startup, 
so Erlang application stores copy of data in some persistent internal storage(mnesia, riak, files). 
In this case application uploads initial data from the table only once during the first start. 
Afterwards it receives modifications and apply it in it’s storage. 
Even if application is down it will not lose any changes from DB 
and will receive all changes when it starts again.


## Usage
1. Initiate connection in replication mode.

    To initiate streaming replication connection, `replication` parameter with 
the value "database" should be set in the `epgsql:connect`.
Only simple queries `squery` can be used in replication mode and 
only special commands accepted in this mode 
(e.g DROP_REPLICATION_SLOT, CREATE_REPLICATION_SLOT, IDENTIFY_SYSTEM).

    Replication commands as well as replication connection mode available only in epgsql and not in epgsqla, epgsqli.

2. Create replication slot. 

    Replication slot will be updated as replication progresses so that the PostgreSQL server knows 
    which WAL segments are still needed by the standby.
    
    Use `epgsql:squery` to send CREATE_REPLICATION_SLOT command.
    
    When a new replication slot is created, a snapshot is exported, 
    which will show exactly the state of the database 
    after which all changes will be included in the change stream. 
    This snapshot can be used to create a new replica by using SET TRANSACTION SNAPSHOT 
    to read the state of the database at the moment the slot was created. 
    
    Note: you have to create new connection with the not-replciation mode to select initial state of tables, 
    since you cannot run SELECT in replication mode connection.

3. Start replication.

    Use `epgsql:start_replication` to start streaming. 

    ```erlang
    ok = epgsql:start_replication(Connection, ReplicationSlot, Callback, CbInitState, 
            WALPosition, PluginOpts, Opts).
    ```
    - `Connection`           - connection in replication mode
    - `ReplicationSlot`      - the name of the replication slot to stream changes from
    - `Callback`             - callback module which should have the callback functions
                                or a process which should be able to receive replication messages.
    - `CbInitState`          - initial state of callback module. 
    - `WALPosition`          - the WAL position XXX/XXX to begin streaming at.
                               "0/0" to let the server determine the start point.
    - `PluginOpts`           - optional options passed to the slot's logical decoding plugin. 
                               For example: "option_name1 'value1', option_name2 'value2'"
    - `Opts`                 - options of logical replication. 
                               See Logical replication options section for details.
    

    On success, PostgreSQL server responds with a CopyBothResponse message, and then starts to stream WAL records.
    PostgreSQL sends CopyData messages which contain:
    - Keepalive message. *epgsql* answers with standby status update message, to avoid a timeout disconnect.
    - XLogData message. *epgsql* calls `CallbackModule:handle_x_log_data` for each XLogData 
    or sends async messages `{epgsql, self(), {x_log_data, StartLSN, EndLSN, WALRecord}}`. 
    In case of async mode, the receiving process should report last processed LSN by calling 
    `standby_status_update(Connection, FlushedLSN, AppliedLSN)`.

    ```erlang
    %% Handles a XLogData Message (StartLSN, EndLSN, WALRecord, CbState).
    %% Return: {ok, LastFlushedLSN, LastAppliedLSN, NewCbState}
    -callback handle_x_log_data(lsn(), lsn(), binary(), cb_state()) -> {ok, lsn(), lsn(), cb_state()}.
     ```
 
Example:

```erlang
start_replication() -> 
    {ok, C} = epgsql:connect("localhost", "username", "psss", [
                {database, "test_db"},
                {replication, "database"}
            ]),
    
    Res = epgsql:squery(C, "CREATE_REPLICATION_SLOT ""epgsql_test"" LOGICAL ""test_decoding"""),
    io:format("~p~n", [Res]),
    
    ok = epgsql:start_replication(C, "epgsql_test", ?MODULE, [], "0/0").

handle_x_log_data(StartLSN, EndLSN, Data, CbState) ->
    io:format("~p~n", [{StartLSN, EndLSN, Data}]),
    {ok, EndLSN, EndLSN, CbState}.
```

##Logical replication options

* **align_lsn** - Default - false.

    During shutdown PG server waits to exit until XLOG records have been sent to the standby, 
    up to the shutdown checkpoint record and sends `Primary keepalive message` 
    with the special flag (which means that the client should reply to this message as soon as possible) 
    to get the last applied LSN from the standby.

    If epgsql uses for replication a decoding plugin which filter some WAL records 
    (for example pgoutput and PG publications with some tables) 
    then epgsql will not receive all WAL records and keep in the state not the latest LSN.
    In such case it is not be possible to stop PG server if epgsql replication is running, 
    because epgsql is not able to report latest LSN.

    To overcome this problem use option `align_lsn = true`.
    If this option enabled when epgsql gets `Primary keepalive message` with the reply required flag 
    it will send back to PG server LSN received in `Primary keepalive message`. 
    PG server will stop normally after receiving this latest LSN.
    If during PG server shutdown epgsql has some replication delay 
    then there is a risk to skip not yet received wal segments, 
    i.e. after restart PG server will send only new wal segments.
    
    However if you use epgsql replication to implement a case 'Consistent cache' 
    and re-create replication slots (or use temporary slots) 
    and load all data from tables on an application startup 
    then you do not have any risk to lose data. 
    
    Otherwise (if you do not load all data from tables during erlang app startup) 
    it is not recommended to set align_lsn to true. In this case to stop PG server stop epgsql replication first.
    
## Flow control

It is possible to set `{active, N}` on a TCP or SSL (since OTP 21.3) socket. E.g. for SSL:
```erlang
Opts = #{ host => "localhost"
        , username => "me"
        , password => "pwd"
        , database => "test"
        , ssl => true
        , ssl_opts => [{active, 10}]
        },
{ok, Conn} = epgsql:connect(Opts).
```

It is currently allowed only in the replication mode. Its main purpose is to control the flow of
replication messages from Postgresql database. If a database is under a high load and a process, which
handles the message stream, cannot keep up with it then setting this option gives the handling process
ability to get messages on-demand.

When the connection is in the asynchronous mode, a process which owns a connection will receive
```erlang
{epgsql, Connection, socket_passive}
```
The process decides when to activate connection's socket again. To do that it should call:
```erlang
epgsql:activate(Connection).
```
The `active` parameter of the socket will be set to the same value as it was configured in
the connection's options.

When the connection is in the synchronous mode, a provided callback module must implement
`handle_socket_passive/1` function, which receives a current callback state and should
return `{ok, NewCallbackState}`. The callback should not call `epgsql:activate/1` directly
because it results in a deadlock.
