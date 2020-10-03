%%% @doc Behaviour module for epgsql_sock commands.
%%% @end
%%% Copyright (C) 2017 - Sergey Prokhorov.  All rights reserved.

-module(epgsql_command).
-export([init/2, execute/3, handle_message/5]).

-export_type([command/0, state/0]).

-type command() :: module().
-type state() :: any().

%% Initialize command's state. Called when command is received by epgsql_sock process.
-callback init(any()) -> state().

-type execute_return() ::
        {ok, epgsql_sock:pg_sock(), state()}
      | {send, epgsql_wire:packet_type(), PktData :: iodata(),
         epgsql_sock:pg_sock(), state()}
      | {send_multi, [{epgsql_wire:packet_type(), PktData :: iodata()}],
         epgsql_sock:pg_sock(), state()}
      | {stop, Reason :: any(), Response :: any(), epgsql_sock:pg_sock()}.

%% Execute command. It should send commands to socket.
%% May be called many times if 'handle_message' will return 'requeue'.
-callback execute(epgsql_sock:pg_sock(), state()) -> execute_return().

-type handle_message_return() ::
        {noaction, epgsql_sock:pg_sock()}
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
%% Handle incoming packet

-callback handle_message(Type :: byte(), Payload :: binary() | epgsql:query_error(),
                         epgsql_sock:pg_sock(), state()) -> handle_message_return().

-spec init(command(), any()) -> state().
init(Command, Args) ->
    Command:init(Args).

-spec execute(command(), epgsql_sock:pg_sock(), state()) -> execute_return().
execute(Command, PgSock, CmdState) ->
    Command:execute(PgSock, CmdState).

-spec handle_message(command(), Type :: byte(), Payload :: binary() | epgsql:query_error(),
                     epgsql_sock:pg_sock(), state()) -> handle_message_return().
handle_message(Command, Type, Payload, PgSock, State) ->
    Command:handle_message(Type, Payload, PgSock, State).
