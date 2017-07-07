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
            WALPosition, PluginOpts).
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
