%%% Copyright (C) 2009 - Will Glozer.  All rights reserved.

-module(pgsql_sock).

-behavior(gen_server).

-export([start_link/4, cancel/3]).
-export([decode_string/1, lower_atom/1]).

-export([handle_call/3, handle_cast/2, handle_info/2]).
-export([init/1, code_change/3, terminate/2]).

-include("pgsql.hrl").
-include("pgsql_binary.hrl").

-record(state, {mod, sock, tail, backend}).

%% -- client interface --

start_link(Host, Username, Password, Opts) ->
    gen_server:start_link(?MODULE, [Host, Username, Password, Opts], []).

cancel(S) ->
    gen_server:cast(S, cancel}).

%% -- gen_server implementation --

init([Host, Username, Password, Opts]) ->
    %% TODO split connect/query timeout?
    Timeout = proplists:get_value(timeout, Opts, 5000),
    Port = proplists:get_value(port, Opts, 5432),
    SockOpts = [{active, false}, {packet, raw}, binary, {nodelay, true}],
    {ok, S} = gen_tcp:connect(Host, Port, SockOpts, Timeout),

    State = #state{
      mod  = gen_tcp,
      sock = S,
      decoder = pgsql_wire:init([]),
      timeout = Timeout},

    case proplists:get_value(ssl, Opts) of
        T when T == true; T == required ->
            ok = gen_tcp:send(S, <<8:?int32, 80877103:?int32>>),
            {ok, <<Code>>} = gen_tcp:recv(S, 1, Timeout),
            State2 = start_ssl(Code, T, Opts, State);
        _ ->
            State2 = State
    end,

    Opts2 = ["user", 0, Username, 0],
    case proplists:get_value(database, Opts, undefined) of
        undefined -> Opts3 = Opts2;
        Database  -> Opts3 = [Opts2 | ["database", 0, Database, 0]]
    end,
    send([<<196608:?int32>>, Opts3, 0], State2),
%% TODO    Async   = proplists:get_value(async, Opts, undefined),
%% TODO    setopts(State2, [{active, true}]),
    {ok, initialize(auth(User, Password, State2))}.

handle_call(Call, _From, State) ->
    {stop, {unsupported_call, Call}, State}.

handle_cast(cancel, State = #state{backend = {Pid, Key}}) ->
    {ok, {Addr, Port}} = inet:peername(State#state.sock),
    SockOpts = [{active, false}, {packet, raw}, binary],
    {ok, Sock} = gen_tcp:connect(Addr, Port, SockOpts),
    Msg = <<16:?int32, 80877102:?int32, Pid:?int32, Key:?int32>>,
    ok = gen_tcp:send(Sock, Msg),
    gen_tcp:close(Sock),
    {noreply, State}.

handle_info({Closed, _Sock}, State)
  when Closed == tcp_closed; Closed == ssl_closed ->
    {stop, sock_closed, State};

handle_info({Error, _Sock, Reason}, State)
  when Error == tcp_error; Error == ssl_error ->
    {stop, {sock_error, Reason}, State};

handle_info({_, _Sock, Data}, #state{tail = Tail} = State) ->
    on_tail(State#state{tail = <<Tail/binary, Data/binary>>}.

on_tail(#state{tail = Tail} = State) ->
    case pgsql_wire:decode_message(Tail) of
        {Message, Tail2} ->
            on_tail(on_message(Message, State#{tail = Tail2}));
        _ ->
            {noreply, State}
    end.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%% -- internal functions --

start_ssl($S, _Flag, Opts, State) ->
    #state{sock = S1, timeout = Timeout} = State,
    case ssl:connect(S1, Opts, Timeout) of
        {ok, S2}        -> State#state{mod = ssl, sock = S2};
        {error, Reason} -> exit({ssl_negotiation_failed, Reason})
    end;

start_ssl($N, Flag, _Opts, State) ->
    case Flag of
        true     -> State;
        required -> exit(ssl_not_available)
    end.

setopts(#state{mod = Mod, sock = Sock}, Opts) ->
    case Mod of
        gen_tcp -> inet:setopts(Sock, Opts);
        ssl     -> ssl:setopts(Sock, Opts)
    end.

send(Data, State#state{mod = Mod, sock = Sock}) ->
    Mod:send(Sock, pgsql_wire:encode(Data)).

send(Type, Data, State#state{mod = Mod, sock = Sock}) ->
    Mod:send(Sock, pgsql_wire:encode(Type, Data)).

auth(User, Password, State) ->
    #state{sock = S, decoder = D} = State,
    %% TODO receive authentication request, send response, wait AuthenticationOk
    .

on_message({$N, Data}, State) ->
    %% TODO use it
    {notice, pgsql_wire:decode_error(Data)},
    State;

on_message({$S, Data}, State) ->
    [Name, Value] = pgsql_wire:decode_strings(Data),
    %% TODO use it
    {parameter_status, Name, Value},
    State;

on_message({$E, Data}, State) ->
    %% TODO use it
    {error, decode_error(Data)},
    State;

on_message({$A, <<Pid:?int32, Strings/binary>>}, State) ->
    case pgsql_wire:decode_strings(Strings) of
        [Channel, Payload] -> ok;
        [Channel]          -> Payload = <<>>
    end,
    %% TODO use it
    {notification, Channel, Pid, Payload},
    State;

on_message(_Msg, State) ->
    State.
