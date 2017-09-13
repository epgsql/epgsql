%%% Behaviour module for epgsql_sock commands.
%%%
%%% Copyright (C) 2017 - Sergey Prokhorov.  All rights reserved.

-module(epgsql_command).

-export_type([command/0]).

-type command() :: module().
-type state() :: any().

%% Initialize command's state. Called when command is received by epgsql_sock process.
-callback init(any()) -> state().

%% Execute command. It should send commands to socket.
%% May be called many times if 'handle_message' will return 'requeue'.
-callback execute(epgsql_sock:pg_sock(), state()) ->
    {ok, epgsql_sock:pg_sock(), state()}.

%% Handle incoming packet
-callback handle_message(Type :: byte(), Payload :: binary() | epgsql:query_error(),
                         epgsql_sock:pg_sock(), state()) -> Reply when
      Reply :: {noaction, epgsql_sock:pg_sock()}
               %% Do nothing; remember changed state
             | {noaction, epgsql_sock:pg_sock(), state()}
               %% Add result to resultset (eg, `{ok, Count}' `{ok, Cols, Rows}', `{error, #error{}}'
               %% It may be returned many times for eg, `squery' with multiple
               %% queries separated by ';'
               %% See epgsql_sock:get_results/1
             | {add_result, Data :: any(), Notification :: any(), epgsql_sock:pg_sock(), state()}
               %% Add new row to current resultset;
               %% See epgsql_sock:get_rows/1
             | {add_row, tuple(), epgsql_sock:pg_sock(), state()}
               %% Finish command execution, reply to the client and go to next command
             | {finish, Result :: any(), Notification :: any(), epgsql_sock:pg_sock()}
               %% Stop `epgsql_sock' process
             | {stop, Reason :: any(), Response :: any(), epgsql_sock:pg_sock()}
               %% Call 'execute' and reschedule command.
               %% It's forbidden to call epgsql_sock:send from `handle_message'.
               %% If you need to do so, you should set some flag in state and
               %% reschedule command.
               %% See `epgsql_cmd_connect' for reference.
             | {requeue, epgsql_sock:pg_sock(), state()}
               %% Protocol synchronization error (eg, unexpected packet)
               %% Drop command queue and don't accept any command except 'sync'
             | {sync_required, Why :: any()}
               %% Unknown packet. Terminate `epgsql_sock' process
             | unknown.
