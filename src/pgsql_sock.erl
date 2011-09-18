%%% Copyright (C) 2009 - Will Glozer.  All rights reserved.

-module(pgsql_sock).

-behavior(gen_server).

-export([start_link/4, cancel/1]).

-export([handle_call/3, handle_cast/2, handle_info/2]).
-export([init/1, code_change/3, terminate/2]).

-include("pgsql.hrl").
-include("pgsql_binary.hrl").

-record(state, {mod,
                sock,
                data,
                backend,
                on_message,
                on_timeout,
                ready,
                timeout}).

%% -- client interface --

start_link(Host, Username, Password, Opts) ->
    gen_server:start_link(?MODULE, [Host, Username, Password, Opts], []).

cancel(S) ->
    gen_server:cast(S, cancel).

%% -- gen_server implementation --

init([Host, Username, Password, Opts]) ->
    gen_server:cast(self(), {connect, Host, Username, Password, Opts}),
    %% TODO split connect/query timeout?
    Timeout = proplists:get_value(timeout, Opts, 5000),
    {ok, #state{timeout = Timeout}}.

handle_call(Call, _From, State) ->
    {stop, {unsupported_call, Call}, State}.

handle_cast({connect, Host, Username, Password, Opts},
            #state{timeout = Timeout} = State) ->
    Port = proplists:get_value(port, Opts, 5432),
    SockOpts = [{active, false}, {packet, raw}, binary, {nodelay, true}],
    {ok, Sock} = gen_tcp:connect(Host, Port, SockOpts, Timeout),

    State2 = case proplists:get_value(ssl, Opts) of
                 T when T == true; T == required ->
                     start_ssl(Sock, T, Opts, State);
                 _ ->
                     State#state{mod  = gen_tcp, sock = Sock}
             end,

    Opts2 = ["user", 0, Username, 0],
    case proplists:get_value(database, Opts, undefined) of
        undefined -> Opts3 = Opts2;
        Database  -> Opts3 = [Opts2 | ["database", 0, Database, 0]]
    end,
    send([<<196608:?int32>>, Opts3, 0], State2),
    %% TODO    Async   = proplists:get_value(async, Opts, undefined),
    setopts(State2, [{active, true}]),
    {noreply,
     State2#state{on_message = fun(M, S) ->
                                       auth(Username, Password, M, S)
                               end},
     Timeout};

handle_cast(cancel, State = #state{backend = {Pid, Key}}) ->
    {ok, {Addr, Port}} = inet:peername(State#state.sock),
    SockOpts = [{active, false}, {packet, raw}, binary],
    {ok, Sock} = gen_tcp:connect(Addr, Port, SockOpts),
    Msg = <<16:?int32, 80877102:?int32, Pid:?int32, Key:?int32>>,
    ok = gen_tcp:send(Sock, Msg),
    gen_tcp:close(Sock),
    {noreply, State}.

handle_info({Closed, Sock}, #state{sock = Sock} = State)
  when Closed == tcp_closed; Closed == ssl_closed ->
    {stop, sock_closed, State};

handle_info({Error, Sock, Reason}, #state{sock = Sock} = State)
  when Error == tcp_error; Error == ssl_error ->
    {stop, {sock_error, Reason}, State};

handle_info(timeout, #state{on_timeout = OnTimeout} = State) ->
    OnTimeout(State);

handle_info({_, Sock, Data2}, #state{data = Data, sock = Sock} = State) ->
    on_data({infinity, State#state{data = <<Data/binary, Data2/binary>>}}).

on_data({Timeout, #state{data = Data, on_message = OnMessage} = State}) ->
    case pgsql_wire:decode_message(Data) of
        {Message, Tail} ->
            on_data(OnMessage(Message, State#state{data = Tail}));
        _ ->
            {noreply, State, Timeout}
    end.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%% -- internal functions --

start_ssl(S, Flag, Opts, #state{timeout = Timeout} = State) ->
    ok = gen_tcp:send(S, <<8:?int32, 80877103:?int32>>),
    {ok, <<Code>>} = gen_tcp:recv(S, 1, Timeout),
    case Code of
        $S  ->
            case ssl:connect(S, Opts, Timeout) of
                {ok, S2}        -> State#state{mod = ssl, sock = S2};
                {error, Reason} -> exit({ssl_negotiation_failed, Reason})
            end;
        $N ->
            case Flag of
                true     -> State;
                required -> exit(ssl_not_available)
            end
    end.

setopts(#state{mod = Mod, sock = Sock}, Opts) ->
    case Mod of
        gen_tcp -> inet:setopts(Sock, Opts);
        ssl     -> ssl:setopts(Sock, Opts)
    end.

send(Data, #state{mod = Mod, sock = Sock}) ->
    Mod:send(Sock, pgsql_wire:encode(Data)).

send(Type, Data, #state{mod = Mod, sock = Sock}) ->
    Mod:send(Sock, pgsql_wire:encode(Type, Data)).

%% AuthenticationOk
auth(User, Password, {$R, <<0:?int32>>}, State) ->
    State#state{on_message = fun on_message/2}.

on_message({$N, Data}, State) ->
    %% TODO use it
    {notice, pgsql_wire:decode_error(Data)},
    {infinity, State};

on_message({$S, Data}, State) ->
    [Name, Value] = pgsql_wire:decode_strings(Data),
    %% TODO use it
    {parameter_status, Name, Value},
    {infinity, State};

on_message({$E, Data}, State) ->
    %% TODO use it
    {error, pgsql_wire:decode_error(Data)},
    {infinity, State};
    State;

on_message({$A, <<Pid:?int32, Strings/binary>>}, State) ->
    case pgsql_wire:decode_strings(Strings) of
        [Channel, Payload] -> ok;
        [Channel]          -> Payload = <<>>
    end,
    %% TODO use it
    {notification, Channel, Pid, Payload},
    {infinity, State};

on_message(_Msg, State) ->
    {infinity, State}.
