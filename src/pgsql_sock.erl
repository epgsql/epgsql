%%% Copyright (C) 2009 - Will Glozer.  All rights reserved.

-module(pgsql_sock).

-behavior(gen_server).

-export([start_link/0, cancel/1]).

-export([handle_call/3, handle_cast/2, handle_info/2]).
-export([init/1, code_change/3, terminate/2]).

-include("pgsql.hrl").
-include("pgsql_binary.hrl").

-record(state, {mod,
                sock,
                data,
                backend,
                handler,
                queue = queue:new(),
                async,
                ready,
                timeout,
                parameters,
                txstatus}).

%% -- client interface --

start_link() ->
    gen_server:start_link(?MODULE, [], []).

cancel(S) ->
    gen_server:cast(S, cancel).

%% -- gen_server implementation --

init([]) ->
    {ok, #state{}}.

handle_call({connect, Host, Username, Password, Opts},
            From,
            #state{queue = Queue} = State) ->
    %% TODO split connect/query timeout?
    Timeout = proplists:get_value(timeout, Opts, 5000),
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
    send(State2, [<<196608:?int32>>, Opts3, 0]),
    Async   = proplists:get_value(async, Opts, undefined),
    setopts(State2, [{active, true}]),
    put(username, Username),
    put(password, Password),
    {noreply,
     State2#state{handler = auth,
                  queue = queue:in(From, Queue),
                  async = Async},
     Timeout}.

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

handle_info(timeout, #state{handler = Handler} = State) ->
    Handler(timeout, State);

handle_info({_, Sock, Data2}, #state{data = Data, sock = Sock} = State) ->
    loop(State#state{data = <<Data/binary, Data2/binary>>}, infinity).

loop(#state{data = Data, handler = Handler} = State, Timeout) ->
    case pgsql_wire:decode_message(Data) of
        {Message, Tail} ->
            case ?MODULE:Handler(Message, State#state{data = Tail}) of
                {noreply, State2} ->
                    loop(State2, infinity);
                {noreply, State2, Timeout2} ->
                    loop(State2, Timeout2);
                R = {stop, _Reason2, _State2} ->
                    R
            end;
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

send(#state{mod = Mod, sock = Sock}, Data) ->
    Mod:send(Sock, pgsql_wire:encode(Data)).

send(#state{mod = Mod, sock = Sock}, Type, Data) ->
    Mod:send(Sock, pgsql_wire:encode(Type, Data)).

reply(#state{queue = Q} = State, Message) ->
    {{value, {Pid, _}}, Q2} = queue:out(Q),
    Pid ! Message,
    State#state{queue = Q2}.

%% -- backend message handling --

%% AuthenticationOk
auth({$R, <<0:?int32>>}, State) ->
    #state{timeout = Timeout} = State,
    {noreply, State#state{handler = initializing}, Timeout};

%% AuthenticationCleartextPassword
auth({$R, <<3:?int32>>}, State) ->
    #state{timeout = Timeout} = State,
    send(State, $p, [get(password), 0]),
    {noreply, State, Timeout};

%% AuthenticationMD5Password
auth({$R, <<5:?int32, Salt:4/binary>>}, State) ->
    #state{timeout = Timeout} = State,
    Digest1 = hex(erlang:md5([get(password), get(username)])),
    Str = ["md5", hex(erlang:md5([Digest1, Salt])), 0],
    send(State, $p, Str),
    {noreply, State, Timeout};

auth({$R, <<M:?int32, _/binary>>}, State) ->
    case M of
        2 -> Method = kerberosV5;
        4 -> Method = crypt;
        6 -> Method = scm;
        7 -> Method = gss;
        8 -> Method = sspi;
        _ -> Method = unknown
    end,
    Error = {error, {unsupported_auth_method, Method}},
    {stop, Error, reply(State, Error)};

%% ErrorResponse
auth({error, E}, State) ->
    case E#error.code of
        <<"28000">> -> Why = invalid_authorization_specification;
        <<"28P01">> -> Why = invalid_password;
        Any         -> Why = Any
    end,
    Error = {error, Why},
    {stop, Error, reply(State, Error)};

auth(timeout, State) ->
    Error = {error, timeout},
    {stop, Error, reply(State, Error)};

auth(Other, State) ->
    on_message(Other, State).

%% BackendKeyData
initializing({$K, <<Pid:?int32, Key:?int32>>}, State) ->
    #state{timeout = Timeout} = State,
    State2 = State#state{backend = {Pid, Key}},
    {noreply, State2, Timeout};

initializing(timeout, State) ->
    Error = {error, timeout},
    {stop, Error, reply(State, Error)};

%% ReadyForQuery
initializing({$Z, <<Status:8>>}, State) ->
    #state{parameters = Parameters} = State,
    erase(username),
    erase(password),
    %% TODO decode dates to now() format
    case lists:keysearch(<<"integer_datetimes">>, 1, Parameters) of
        {value, {_, <<"on">>}}  -> put(datetime_mod, pgsql_idatetime);
        {value, {_, <<"off">>}} -> put(datetime_mod, pgsql_fdatetime)
    end,
    State2 = State#state{handler = on_message,
                         txstatus = Status,
                         ready = true},
    {noreply, reply(State2, {ok, self()})};

initializing({error, _} = Error, State) ->
    {stop, Error, reply(State, Error)};

initializing(Other, State) ->
    on_message(Other, State).

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


hex(Bin) ->
    HChar = fun(N) when N < 10 -> $0 + N;
               (N) when N < 16 -> $W + N
            end,
    <<<<(HChar(H)), (HChar(L))>> || <<H:4, L:4>> <= Bin>>.
