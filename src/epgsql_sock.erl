%%% @doc GenServer holding all the connection state (including socket).
%%%
%%% See [https://www.postgresql.org/docs/current/static/protocol-flow.html]
%%%
%%% Commands in PostgreSQL protocol are pipelined: you don't have to wait for
%%% reply to be able to send next command.
%%% Commands are processed (and responses to them are generated) in FIFO order.
%%% eg, if you execute 2 SimpleQuery: #1 and #2, first you get all response
%%% packets for #1 and then all for #2:
%%% ```
%%% > SQuery #1
%%% > SQuery #2
%%% < RowDescription #1
%%% < DataRow #1.1
%%% < ...
%%% < DataRow #1.N
%%% < CommandComplete #1
%%% < RowDescription #2
%%% < DataRow #2.1
%%% < ...
%%% < DataRow #2.N
%%% < CommandComplete #2
%%% '''
%%% `epgsql_sock' is capable of utilizing the pipelining feature - as soon as
%%% it receives a new command, it sends it to the server immediately and then
%%% it puts command's callbacks and state into internal queue of all the commands
%%% which were sent to the server and waiting for response. So it knows in which
%%% order it should call each pipelined command's `handle_message' callback.
%%% But it can be easily broken if high-level command is poorly implemented or
%%% some conflicting low-level commands (such as `parse', `bind', `execute') are
%%% executed in a wrong order. In this case server and epgsql states become out of
%%% sync and {@link epgsql_cmd_sync} have to be executed in order to recover.
%%%
%%% {@link epgsql_cmd_copy_from_stdin} and {@link epgsql_cmd_start_replication} switches the
%%% "state machine" of connection process to a special "COPY mode" subprotocol.
%%% See [https://www.postgresql.org/docs/current/protocol-flow.html#PROTOCOL-COPY].
%%% @see epgsql_cmd_connect. epgsql_cmd_connect for network connection and authentication setup
%%% @end
%%% Copyright (C) 2009 - Will Glozer.  All rights reserved.
%%% Copyright (C) 2011 - Anton Lebedevich.  All rights reserved.

-module(epgsql_sock).

-behavior(gen_server).

-export([start_link/0,
         close/1,
         sync_command/3,
         async_command/4,
         get_parameter/2,
         set_notice_receiver/2,
         get_cmd_status/1,
         cancel/1,
         copy_send_rows/3,
         standby_status_update/3]).

-export([handle_call/3, handle_cast/2, handle_info/2, format_status/2]).
-export([init/1, code_change/3, terminate/2]).

%% loop callback
-export([on_message/3, on_replication/3, on_copy_from_stdin/3]).

%% Comand's APIs
-export([set_net_socket/3, init_replication_state/1, set_attr/3, get_codec/1,
         get_rows/1, get_results/1, notify/2, send/2, send/3, send_multi/2,
         get_parameter_internal/2,
         get_subproto_state/1, set_packet_handler/2]).

-export_type([transport/0, pg_sock/0, error/0]).

-include("epgsql.hrl").
-include("protocol.hrl").
-include("epgsql_replication.hrl").
-include("epgsql_copy.hrl").

-type transport() :: {call, any()}
                   | {cast, pid(), reference()}
                   | {incremental, pid(), reference()}.

-type tcp_socket() :: port(). %gen_tcp:socket() isn't exported prior to erl 18
-type repl_state() :: #repl{}.
-type copy_state() :: #copy{}.

-type error() :: {error, sync_required | closed | sock_closed | sock_error}.

-record(state, {mod :: gen_tcp | ssl | undefined,
                sock :: tcp_socket() | ssl:sslsocket() | undefined,
                data = <<>>,
                backend :: {Pid :: integer(), Key :: integer()} | undefined,
                handler = on_message :: on_message | on_replication | on_copy_from_stdin | undefined,
                codec :: epgsql_binary:codec() | undefined,
                queue = queue:new() :: queue:queue({epgsql_command:command(), any(), transport()}),
                current_cmd :: epgsql_command:command() | undefined,
                current_cmd_state :: any() | undefined,
                current_cmd_transport :: transport() | undefined,
                async :: undefined | atom() | pid(),
                parameters = [] :: [{Key :: binary(), Value :: binary()}],
                rows = [] :: [tuple()] | information_redacted,
                results = [],
                sync_required :: boolean() | undefined,
                txstatus :: byte() | undefined,  % $I | $T | $E,
                complete_status :: atom() | {atom(), integer()} | undefined,
                subproto_state :: repl_state() | copy_state() | undefined,
                connect_opts :: epgsql:connect_opts() | undefined}).

-opaque pg_sock() :: #state{}.

-ifndef(OTP_RELEASE).                           % pre-OTP21
-define(WITH_STACKTRACE(T, R, S), T:R -> S = erlang:get_stacktrace(), ).
-else.
-define(WITH_STACKTRACE(T, R, S), T:R:S ->).
-endif.

%% -- client interface --

start_link() ->
    gen_server:start_link(?MODULE, [], []).

close(C) when is_pid(C) ->
    catch gen_server:cast(C, stop),
    ok.

-spec sync_command(epgsql:connection(), epgsql_command:command(), any()) -> any().
sync_command(C, Command, Args) ->
    gen_server:call(C, {command, Command, Args}, infinity).

-spec async_command(epgsql:connection(), cast | incremental,
                    epgsql_command:command(), any()) -> reference().
async_command(C, Transport, Command, Args) ->
    Ref = make_ref(),
    Pid = self(),
    ok = gen_server:cast(C, {{Transport, Pid, Ref}, Command, Args}),
    Ref.

get_parameter(C, Name) ->
    gen_server:call(C, {get_parameter, to_binary(Name)}, infinity).

set_notice_receiver(C, PidOrName) when is_pid(PidOrName);
                                       is_atom(PidOrName) ->
    gen_server:call(C, {set_async_receiver, PidOrName}, infinity).

get_cmd_status(C) ->
    gen_server:call(C, get_cmd_status, infinity).

cancel(S) ->
    gen_server:cast(S, cancel).

copy_send_rows(C, Rows, Timeout) ->
    gen_server:call(C, {copy_send_rows, Rows}, Timeout).

standby_status_update(C, FlushedLSN, AppliedLSN) ->
    gen_server:call(C, {standby_status_update, FlushedLSN, AppliedLSN}).


%% -- command APIs --

%% send()
%% send_many()

-spec set_net_socket(gen_tcp | ssl, tcp_socket() | ssl:sslsocket(), pg_sock()) -> pg_sock().
set_net_socket(Mod, Socket, State) ->
    State1 = State#state{mod = Mod, sock = Socket},
    setopts(State1, [{active, true}]),
    State1.

-spec init_replication_state(pg_sock()) -> pg_sock().
init_replication_state(State) ->
    State#state{subproto_state = #repl{}}.

-spec set_attr(atom(), any(), pg_sock()) -> pg_sock().
set_attr(backend, {_Pid, _Key} = Backend, State) ->
    State#state{backend = Backend};
set_attr(async, Async, State) ->
    State#state{async = Async};
set_attr(txstatus, Status, State) ->
    State#state{txstatus = Status};
set_attr(codec, Codec, State) ->
    State#state{codec = Codec};
set_attr(sync_required, Value, State) ->
    State#state{sync_required = Value};
set_attr(subproto_state, Value, State) ->
    State#state{subproto_state = Value};
set_attr(connect_opts, ConnectOpts, State) ->
    State#state{connect_opts = ConnectOpts}.

%% XXX: be careful!
-spec set_packet_handler(atom(), pg_sock()) -> pg_sock().
set_packet_handler(Handler, State) ->
    State#state{handler = Handler}.

-spec get_codec(pg_sock()) -> epgsql_binary:codec().
get_codec(#state{codec = Codec}) ->
    Codec.

-spec get_subproto_state(pg_sock()) -> repl_state() | copy_state() | undefined.
get_subproto_state(#state{subproto_state = SubState}) ->
    SubState.

-spec get_rows(pg_sock()) -> [tuple()].
get_rows(#state{rows = Rows}) ->
    lists:reverse(Rows).

-spec get_results(pg_sock()) -> [any()].
get_results(#state{results = Results}) ->
    lists:reverse(Results).

-spec get_parameter_internal(binary(), pg_sock()) -> binary() | undefined.
get_parameter_internal(Name, #state{parameters = Parameters}) ->
    case lists:keysearch(Name, 1, Parameters) of
        {value, {Name, Value}} -> Value;
        false                  -> undefined
    end.


%% -- gen_server implementation --

init([]) ->
    {ok, #state{}}.

handle_call({command, Command, Args}, From, State) ->
    Transport = {call, From},
    command_new(Transport, Command, Args, State);

handle_call({get_parameter, Name}, _From, State) ->
    {reply, {ok, get_parameter_internal(Name, State)}, State};

handle_call({set_async_receiver, PidOrName}, _From, #state{async = Previous} = State) ->
    {reply, {ok, Previous}, State#state{async = PidOrName}};

handle_call(get_cmd_status, _From, #state{complete_status = Status} = State) ->
    {reply, {ok, Status}, State};

handle_call({standby_status_update, FlushedLSN, AppliedLSN}, _From,
            #state{handler = on_replication,
                   subproto_state = #repl{last_received_lsn = ReceivedLSN} = Repl} = State) ->
    send(State, ?COPY_DATA, epgsql_wire:encode_standby_status_update(ReceivedLSN, FlushedLSN, AppliedLSN)),
    Repl1 = Repl#repl{last_flushed_lsn = FlushedLSN,
                      last_applied_lsn = AppliedLSN},
    {reply, ok, State#state{subproto_state = Repl1}};

handle_call({copy_send_rows, Rows}, _From,
           #state{handler = Handler, subproto_state = CopyState} = State) ->
    Response = handle_copy_send_rows(Rows, Handler, CopyState, State),
    {reply, Response, State}.

handle_cast({{Method, From, Ref} = Transport, Command, Args}, State)
  when ((Method == cast) or (Method == incremental)),
       is_pid(From),
       is_reference(Ref)  ->
    command_new(Transport, Command, Args, State);

handle_cast(stop, State) ->
    send(State, ?TERMINATE, []),
    {stop, normal, flush_queue(State, {error, closed})};

handle_cast(cancel, State = #state{backend = {Pid, Key},
                                   connect_opts = ConnectOpts,
                                   mod = Mode}) ->
    SockOpts = [{active, false}, {packet, raw}, binary],
    Msg = <<16:?int32, 80877102:?int32, Pid:?int32, Key:?int32>>,
    case epgsql_cmd_connect:open_socket(SockOpts, ConnectOpts) of
      {ok, Mode, Sock} ->
          ok = apply(Mode, send, [Sock, Msg]),
          apply(Mode, close, [Sock]);
      {error, _Reason} ->
          noop
    end,
    {noreply, State}.

handle_info({DataTag, Sock, Data2}, #state{data = Data, sock = Sock} = State)
  when DataTag == tcp; DataTag == ssl ->
    loop(State#state{data = <<Data/binary, Data2/binary>>});

handle_info({Closed, Sock}, #state{sock = Sock} = State)
  when Closed == tcp_closed; Closed == ssl_closed ->
    {stop, sock_closed, flush_queue(State#state{sock = undefined}, {error, sock_closed})};

handle_info({Error, Sock, Reason}, #state{sock = Sock} = State)
  when Error == tcp_error; Error == ssl_error ->
    Why = {sock_error, Reason},
    {stop, Why, flush_queue(State, {error, Why})};

handle_info({inet_reply, _, ok}, State) ->
    {noreply, State};

handle_info({inet_reply, _, Status}, State) ->
    {stop, Status, flush_queue(State, {error, Status})};

handle_info({io_request, From, ReplyAs, Request}, State) ->
    Response = handle_io_request(Request, State),
    io_reply(Response, From, ReplyAs),
    {noreply, State}.

terminate(_Reason, #state{sock = undefined}) -> ok;
terminate(_Reason, #state{mod = gen_tcp, sock = Sock}) -> gen_tcp:close(Sock);
terminate(_Reason, #state{mod = ssl, sock = Sock}) -> ssl:close(Sock).

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

format_status(normal, [_PDict, State=#state{}]) ->
  [{data, [{"State", State}]}];
format_status(terminate, [_PDict, State]) ->
  %% Do not format the rows attribute when process terminates abnormally
  %% but allow it when is a sys:get_status/1.2
  State#state{rows = information_redacted}.

%% -- internal functions --

-spec command_new(transport(), epgsql_command:command(), any(), pg_sock()) ->
                         Result when
      Result :: {noreply, pg_sock()}
              | {stop, Reason :: any(), pg_sock()}.
command_new(Transport, Command, Args, State) ->
    CmdState = epgsql_command:init(Command, Args),
    command_exec(Transport, Command, CmdState, State).

-spec command_exec(transport(), epgsql_command:command(), any(), pg_sock()) ->
                          Result when
      Result :: {noreply, pg_sock()}
              | {stop, Reason :: any(), pg_sock()}.
command_exec(Transport, Command, _, State = #state{sync_required = true})
  when Command /= epgsql_cmd_sync ->
    {noreply,
     finish(State#state{current_cmd = Command,
                        current_cmd_transport = Transport},
            {error, sync_required})};
command_exec(Transport, Command, CmdState, State) ->
    case epgsql_command:execute(Command, State, CmdState) of
        {ok, State1, CmdState1} ->
            {noreply, command_enqueue(Transport, Command, CmdState1, State1)};
        {send, PktType, PktData, State1, CmdState1} ->
            ok = send(State1, PktType, PktData),
            {noreply, command_enqueue(Transport, Command, CmdState1, State1)};
        {send_multi, Packets, State1, CmdState1} when is_list(Packets) ->
            ok = send_multi(State1, Packets),
            {noreply, command_enqueue(Transport, Command, CmdState1, State1)};
        {stop, StopReason, Response, State1} ->
            reply(Transport, Response, Response),
            {stop, StopReason, State1}
    end.

-spec command_enqueue(transport(), epgsql_command:command(), epgsql_command:state(), pg_sock()) -> pg_sock().
command_enqueue(Transport, Command, CmdState, #state{current_cmd = undefined} = State) ->
    State#state{current_cmd = Command,
                current_cmd_state = CmdState,
                current_cmd_transport = Transport,
                complete_status = undefined};
command_enqueue(Transport, Command, CmdState, #state{queue = Q} = State) ->
    State#state{queue = queue:in({Command, CmdState, Transport}, Q),
                complete_status = undefined}.

-spec command_handle_message(byte(), binary() | epgsql:query_error(), pg_sock()) ->
                                    {noreply, pg_sock()}
                                  | {stop, any(), pg_sock()}.
command_handle_message(Msg, Payload,
                       #state{current_cmd = Command,
                              current_cmd_state = CmdState} = State) ->
    case epgsql_command:handle_message(Command, Msg, Payload, State, CmdState) of
        {add_row, Row, State1, CmdState1} ->
            {noreply, add_row(State1#state{current_cmd_state = CmdState1}, Row)};
        {add_result, Result, Notice, State1, CmdState1} ->
            {noreply,
             add_result(State1#state{current_cmd_state = CmdState1},
                        Notice, Result)};
        {finish, Result, Notice, State1} ->
            {noreply, finish(State1, Notice, Result)};
        {noaction, State1} ->
            {noreply, State1};
        {noaction, State1, CmdState1} ->
            {noreply, State1#state{current_cmd_state = CmdState1}};
        {requeue, State1, CmdState1} ->
            Transport = State1#state.current_cmd_transport,
            command_exec(Transport, Command, CmdState1,
                         State1#state{current_cmd = undefined});
        {stop, Reason, Response, State1} ->
            {stop, Reason, finish(State1, Response)};
        {sync_required, Why} ->
            %% Protocol error. Finish and flush all pending commands.
            {noreply, sync_required(finish(State#state{sync_required = true}, Why))};
        unknown ->
            {stop, {error, {unexpected_message, Msg, Command, CmdState}}, State}
    end.

command_next(#state{current_cmd = PrevCmd,
                    queue = Q} = State) when PrevCmd =/= undefined ->
    case queue:out(Q) of
        {empty, _} ->
            State#state{current_cmd = undefined,
                        current_cmd_state = undefined,
                        current_cmd_transport = undefined,
                        rows = [],
                        results = []};
        {{value, {Command, CmdState, Transport}}, Q1} ->
            State#state{current_cmd = Command,
                        current_cmd_state = CmdState,
                        current_cmd_transport = Transport,
                        queue = Q1,
                        rows = [],
                        results = []}
    end.


setopts(#state{mod = Mod, sock = Sock}, Opts) ->
    case Mod of
        gen_tcp -> inet:setopts(Sock, Opts);
        ssl     -> ssl:setopts(Sock, Opts)
    end.

%% This one only used in connection initiation to send client's
%% `StartupMessage' and `SSLRequest' packets
-spec send(pg_sock(), iodata()) -> ok | {error, any()}.
send(#state{mod = Mod, sock = Sock}, Data) ->
    do_send(Mod, Sock, epgsql_wire:encode_command(Data)).

-spec send(pg_sock(), epgsql_wire:packet_type(), iodata()) -> ok | {error, any()}.
send(#state{mod = Mod, sock = Sock}, Type, Data) ->
    do_send(Mod, Sock, epgsql_wire:encode_command(Type, Data)).

-spec send_multi(pg_sock(), [{epgsql_wire:packet_type(), iodata()}]) -> ok | {error, any()}.
send_multi(#state{mod = Mod, sock = Sock}, List) ->
    do_send(Mod, Sock, lists:map(fun({Type, Data}) ->
                                    epgsql_wire:encode_command(Type, Data)
                                 end, List)).

do_send(gen_tcp, Sock, Bin) ->
    %% Why not gen_tcp:send/2?
    %% See https://github.com/rabbitmq/rabbitmq-common/blob/v3.7.4/src/rabbit_writer.erl#L367-L384
    %% Because of that we also have `handle_info({inet_reply, ...`
    try erlang:port_command(Sock, Bin) of
        true ->
            ok
    catch
        error:_Error ->
            {error, einval}
    end;
do_send(ssl, Sock, Bin) ->
    ssl:send(Sock, Bin).

loop(#state{data = Data, handler = Handler, subproto_state = Repl} = State) ->
    case epgsql_wire:decode_message(Data) of
        {Type, Payload, Tail} ->
            case ?MODULE:Handler(Type, Payload, State#state{data = Tail}) of
                {noreply, State2} ->
                    loop(State2);
                R = {stop, _Reason2, _State2} ->
                    R
            end;
        _ ->
            %% in replication mode send feedback after each batch of messages
            case Handler == on_replication
                  andalso (Repl =/= undefined)
                  andalso (Repl#repl.feedback_required) of
                true ->
                    #repl{last_received_lsn = LastReceivedLSN,
                          last_flushed_lsn = LastFlushedLSN,
                          last_applied_lsn = LastAppliedLSN} = Repl,
                    send(State, ?COPY_DATA, epgsql_wire:encode_standby_status_update(
                        LastReceivedLSN, LastFlushedLSN, LastAppliedLSN)),
                    {noreply, State#state{subproto_state = Repl#repl{feedback_required = false}}};
                _ ->
                    {noreply, State}
            end
    end.

finish(State, Result) ->
    finish(State, Result, Result).

finish(State = #state{current_cmd_transport = Transport}, Notice, Result) ->
    reply(Transport, Notice, Result),
    command_next(State).

reply({cast, From, Ref}, _, Result) ->
    From ! {self(), Ref, Result};
reply({incremental, From, Ref}, Notice, _) ->
    From ! {self(), Ref, Notice};
reply({call, From}, _, Result) ->
    gen_server:reply(From, Result).

add_result(#state{results = Results, current_cmd_transport = Transport} = State, Notice, Result) ->
    Results2 = case Transport of
                   {incremental, From, Ref} ->
                       From ! {self(), Ref, Notice},
                       Results;
                   _ ->
                       [Result | Results]
               end,
    State#state{rows = [],
                results = Results2}.

add_row(#state{rows = Rows, current_cmd_transport = Transport} = State, Data) ->
    Rows2 = case Transport of
                {incremental, From, Ref} ->
                    From ! {self(), Ref, {data, Data}},
                    Rows;
                _ ->
                    [Data | Rows]
            end,
    State#state{rows = Rows2}.

notify(#state{current_cmd_transport = {incremental, From, Ref}} = State, Notice) ->
    From ! {self(), Ref, Notice},
    State;
notify(State, _) ->
    State.

%% Send asynchronous messages (notice / notification)
notify_async(#state{async = undefined}, _) ->
    false;
notify_async(#state{async = PidOrName}, Msg) ->
    try PidOrName ! {epgsql, self(), Msg} of
        _ -> true
    catch error:badarg ->
            %% no process registered under this name
            false
    end.

sync_required(#state{current_cmd = epgsql_cmd_sync} = State) ->
    State;
sync_required(#state{current_cmd = undefined} = State) ->
    State#state{sync_required = true};
sync_required(State) ->
    sync_required(finish(State, {error, sync_required})).

flush_queue(#state{current_cmd = undefined} = State, _) ->
    State;
flush_queue(State, Error) ->
    flush_queue(finish(State, Error), Error).

%% @doc Handler for IO protocol version of COPY FROM STDIN
%%
%% COPY FROM STDIN is implemented as Erlang
%% <a href="https://erlang.org/doc/apps/stdlib/io_protocol.html">io protocol</a>.
handle_io_request(_, #state{handler = Handler}) when Handler =/= on_copy_from_stdin ->
    %% Received IO request when `epgsql_cmd_copy_from_stdin' haven't yet been called or it was
    %% terminated with error and already sent `ReadyForQuery'
    {error, not_in_copy_mode};
handle_io_request(_, #state{subproto_state = #copy{last_error = Err}}) when Err =/= undefined ->
    {error, Err};
handle_io_request({put_chars, Encoding, Chars}, State) ->
    send(State, ?COPY_DATA, encode_chars(Encoding, Chars));
handle_io_request({put_chars, Encoding, Mod, Fun, Args}, State) ->
    try apply(Mod, Fun, Args) of
        Chars when is_binary(Chars);
                   is_list(Chars) ->
            handle_io_request({put_chars, Encoding, Chars}, State);
        Other ->
            {error, {fun_return_not_characters, Other}}
    catch ?WITH_STACKTRACE(T, R, S)
            {error, {fun_exception, {T, R, S}}}
    end;
handle_io_request({setopts, _}, _State) ->
    {error, request};
handle_io_request(getopts, _State) ->
    {error, request};
handle_io_request({requests, Requests}, State) ->
    try_requests(Requests, State, ok).

try_requests([Req | Requests], State, _) ->
    case handle_io_request(Req, State) of
        {error, _} = Err ->
            Err;
        Other ->
            try_requests(Requests, State, Other)
    end;
try_requests([], _, LastRes) ->
    LastRes.

io_reply(Result, From, ReplyAs) ->
    From ! {io_reply, ReplyAs, Result}.

%% @doc Handler for `copy_send_rows' API
%%
%% Only supports binary protocol right now.
%% But, in theory, can be used for text / csv formats as well, but we would need to add
%% some more callbacks to `epgsql_type' behaviour (eg, `encode_text')
handle_copy_send_rows(_Rows, Handler, _CopyState, _State) when Handler =/= on_copy_from_stdin ->
    {error, not_in_copy_mode};
handle_copy_send_rows(_, _, #copy{format = Format}, _) when Format =/= binary ->
    %% copy_send_rows only supports "binary" format
    {error, not_binary_format};
handle_copy_send_rows(_, _, #copy{last_error = LastError}, _) when LastError =/= undefined ->
    %% server already reported error in data stream asynchronously
    {error, LastError};
handle_copy_send_rows(Rows, _, #copy{binary_types = Types}, State) ->
    Data = [epgsql_wire:encode_copy_row(Values, Types, get_codec(State))
            || Values <- Rows],
    ok = send(State, ?COPY_DATA, Data).

encode_chars(_, Bin) when is_binary(Bin) ->
    Bin;
encode_chars(unicode, Chars) when is_list(Chars) ->
    unicode:characters_to_binary(Chars);
encode_chars(latin1, Chars) when is_list(Chars) ->
    unicode:characters_to_binary(Chars, latin1).


to_binary(B) when is_binary(B) -> B;
to_binary(L) when is_list(L)   -> list_to_binary(L).


%% -- backend message handling --

%% CommandComplete
on_message(?COMMAND_COMPLETE = Msg, Bin, State) ->
    Complete = epgsql_wire:decode_complete(Bin),
    command_handle_message(Msg, Bin, State#state{complete_status = Complete});

%% ReadyForQuery
on_message(?READY_FOR_QUERY = Msg, <<Status:8>> = Bin, State) ->
    command_handle_message(Msg, Bin, State#state{txstatus = Status});

%% Error
on_message(?ERROR = Msg, Err, #state{current_cmd = CurrentCmd} = State) ->
    Reason = epgsql_wire:decode_error(Err),
    case CurrentCmd of
        undefined ->
            %% Message generated by server asynchronously
            {stop, {shutdown, Reason}, State};
        _ ->
            command_handle_message(Msg, Reason, State)
    end;

%% NoticeResponse
on_message(?NOTICE, Data, State) ->
    notify_async(State, {notice, epgsql_wire:decode_error(Data)}),
    {noreply, State};

%% ParameterStatus
on_message(?PARAMETER_STATUS, Data, State) ->
    [Name, Value] = epgsql_wire:decode_strings(Data),
    Parameters2 = lists:keystore(Name, 1, State#state.parameters,
                                 {Name, Value}),
    {noreply, State#state{parameters = Parameters2}};

%% NotificationResponse
on_message(?NOTIFICATION, <<Pid:?int32, Strings/binary>>, State) ->
    {Channel1, Payload1} = case epgsql_wire:decode_strings(Strings) of
        [Channel, Payload] -> {Channel, Payload};
        [Channel]          -> {Channel, <<>>}
    end,
    notify_async(State, {notification, Channel1, Pid, Payload1}),
    {noreply, State};

%% ParseComplete
%% ParameterDescription
%% RowDescription
%% NoData
%% BindComplete
%% CloseComplete
%% DataRow
%% PortalSuspended
%% EmptyQueryResponse
%% CopyData
%% CopyBothResponse
on_message(Msg, Payload, State) ->
    command_handle_message(Msg, Payload, State).

%% @doc Handle "copy subprotocol" for COPY .. FROM STDIN
%%
%% Activated by `epgsql_cmd_copy_from_stdin', deactivated by `epgsql_cmd_copy_done' or error
on_copy_from_stdin(?READY_FOR_QUERY, <<Status:8>>,
                   #state{subproto_state = #copy{last_error = Err,
                                                 initiator = Pid}} = State) when Err =/= undefined ->
    %% Reporting error from here and not from ?ERROR so it's easier to be in sync state
    Pid ! {epgsql, self(), {error, Err}},
    {noreply, State#state{subproto_state = undefined,
                          handler = on_message,
                          txstatus = Status}};
on_copy_from_stdin(?ERROR, Err, #state{subproto_state = SubState} = State) ->
    Reason = epgsql_wire:decode_error(Err),
    {noreply, State#state{subproto_state = SubState#copy{last_error = Reason}}};
on_copy_from_stdin(M, Data, Sock) when M == ?NOTICE;
                                       M == ?NOTIFICATION;
                                       M == ?PARAMETER_STATUS ->
    on_message(M, Data, Sock).


%% CopyData for Replication mode
on_replication(?COPY_DATA, <<?PRIMARY_KEEPALIVE_MESSAGE:8, LSN:?int64, _Timestamp:?int64, ReplyRequired:8>>,
               #state{subproto_state = #repl{last_flushed_lsn = LastFlushedLSN,
                                             last_applied_lsn = LastAppliedLSN,
                                             align_lsn = AlignLsn} = Repl} = State) ->
    Repl1 =
        case ReplyRequired of
            1 when AlignLsn ->
                send(State, ?COPY_DATA,
                     epgsql_wire:encode_standby_status_update(LSN, LSN, LSN)),
                Repl#repl{feedback_required = false,
                     last_received_lsn = LSN, last_applied_lsn = LSN, last_flushed_lsn = LSN};
            1 when not AlignLsn ->
                send(State, ?COPY_DATA,
                     epgsql_wire:encode_standby_status_update(LSN, LastFlushedLSN, LastAppliedLSN)),
                Repl#repl{feedback_required = false,
                          last_received_lsn = LSN};
            _ ->
                Repl#repl{feedback_required = true,
                          last_received_lsn = LSN}
        end,
    {noreply, State#state{subproto_state = Repl1}};

%% CopyData for Replication mode
on_replication(?COPY_DATA, <<?X_LOG_DATA, StartLSN:?int64, EndLSN:?int64,
                             _Timestamp:?int64, WALRecord/binary>>,
               #state{subproto_state = Repl} = State) ->
    Repl1 = handle_xlog_data(StartLSN, EndLSN, WALRecord, Repl),
    {noreply, State#state{subproto_state = Repl1}};
on_replication(?ERROR, Err, State) ->
    Reason = epgsql_wire:decode_error(Err),
    {stop, {error, Reason}, State};
on_replication(M, Data, Sock) when M == ?NOTICE;
                                   M == ?NOTIFICATION;
                                   M == ?PARAMETER_STATUS ->
    on_message(M, Data, Sock).


handle_xlog_data(StartLSN, EndLSN, WALRecord, #repl{cbmodule = undefined,
                                                    receiver = Receiver} = Repl) ->
    %% with async messages
    Receiver ! {epgsql, self(), {x_log_data, StartLSN, EndLSN, WALRecord}},
    Repl#repl{feedback_required = true,
              last_received_lsn = EndLSN};
handle_xlog_data(StartLSN, EndLSN, WALRecord,
                 #repl{cbmodule = CbModule, cbstate = CbState, receiver = undefined} = Repl) ->
    %% with callback method
    {ok, LastFlushedLSN, LastAppliedLSN, NewCbState} =
        epgsql:handle_x_log_data(CbModule, StartLSN, EndLSN, WALRecord, CbState),
    Repl#repl{feedback_required = true,
              last_received_lsn = EndLSN,
              last_flushed_lsn = LastFlushedLSN,
              last_applied_lsn = LastAppliedLSN,
              cbstate = NewCbState}.
