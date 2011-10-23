%%% Copyright (C) 2009 - Will Glozer.  All rights reserved.

-module(pgsql_sock).

-behavior(gen_server).

-export([start_link/0,
         connect/5,
         close/1,
         get_parameter/2,
         squery/2,
         equery/3,
         parse/4,
         bind/4,
         execute/4,
         describe/3,
         close/3,
         sync/1,
         cancel/1]).

-export([handle_call/3, handle_cast/2, handle_info/2]).
-export([init/1, code_change/3, terminate/2]).

%% state callbacks
-export([auth/2, initializing/2, on_message/2]).

-include("pgsql.hrl").
-include("pgsql_binary.hrl").

-record(state, {mod,
                sock,
                data = <<>>,
                backend,
                handler,
                queue = queue:new(),
                async,
                parameters = [],
                columns,
                sync_required,
                txstatus}).

%% -- client interface --

start_link() ->
    gen_server:start_link(?MODULE, [], []).

connect(C, Host, Username, Password, Opts) ->
    cast(C, {connect, Host, Username, Password, Opts}).

close(C) when is_pid(C) ->
    catch gen_server:cast(C, stop),
    ok.

get_parameter(C, Name) ->
    gen_server:call(C, {get_parameter, to_binary(Name)}, infinity).

squery(C, Sql) ->
    cast(C, {squery, Sql}).

equery(C, Statement, Parameters) ->
    cast(C, {equery, Statement, Parameters}).

parse(C, Name, Sql, Types) ->
    cast(C, {parse, Name, Sql, Types}).

bind(C, Statement, PortalName, Parameters) ->
    cast(C, {bind, Statement, PortalName, Parameters}).

execute(C, Statement, PortalName, MaxRows) ->
    cast(C, {execute, Statement, PortalName, MaxRows}).

describe(C, Type, Name) ->
    cast(C, {describe, Type, Name}).

close(C, Type, Name) ->
    cast(C, {close, Type, Name}).

sync(C) ->
    cast(C, sync).

cancel(S) ->
    gen_server:cast(S, cancel).

%% -- gen_server implementation --

init([]) ->
    {ok, #state{}}.

handle_call({get_parameter, Name}, _From, State) ->
    case lists:keysearch(Name, 1, State#state.parameters) of
        {value, {Name, Value}} -> Value;
        false                  -> Value = undefined
    end,
    {reply, {ok, Value}, State}.

handle_cast({{From, Ref}, Command}, State = #state{sync_required = true})
  when Command /= sync ->
    From ! {Ref, {error, sync_required}},
    {noreply, State};

handle_cast(Req = {_, {connect, Host, Username, Password, Opts}}, State) ->
    #state{queue = Q} = State,
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
                  queue = queue:in(Req, Q),
                  async = Async}};

handle_cast(stop, #state{queue = Q} = State) ->
    %% TODO flush queue
    {stop, normal, State};

handle_cast(Req = {_, {squery, Sql}}, State) ->
    #state{queue = Q} = State,
    send(State, $Q, [Sql, 0]),
    {noreply, State#state{queue = queue:in(Req, Q)}};

handle_cast(Req = {_, {equery, Statement, Parameters}}, State) ->
    #state{queue = Q} = State,
    #statement{name = StatementName, columns = Columns} = Statement,
    Bin1 = pgsql_wire:encode_parameters(Parameters),
    %% TODO do we really need Columns here?
    Bin2 = pgsql_wire:encode_formats(Columns),
    send(State, $B, ["", 0, StatementName, 0, Bin1, Bin2]),
    %% TODO send Describe there
    send(State, $E, ["", 0, <<0:?int32>>]),
    send(State, $C, [$S, "", 0]),
    send(State, $S, []),
    {noreply, State#state{queue = queue:in(Req, Q)}};

handle_cast(Req = {_, {parse, Name, Sql, Types}}, State) ->
    #state{queue = Q} = State,
    Bin = pgsql_wire:encode_types(Types),
    send(State, $P, [Name, 0, Sql, 0, Bin]),
    send(State, $D, [$S, Name, 0]), % TODO remove it
    send(State, $H, []),
    {noreply, State#state{queue = queue:in(Req, Q)}};

handle_cast(Req = {_, {bind, Statement, PortalName, Parameters}}, State) ->
    #state{queue = Q} = State,
    #statement{name = StatementName, columns = Columns, types = Types} = Statement,
    Typed_Parameters = lists:zip(Types, Parameters),
    Bin1 = pgsql_wire:encode_parameters(Typed_Parameters),
    Bin2 = pgsql_wire:encode_formats(Columns),
    send(State, $B, [PortalName, 0, StatementName, 0, Bin1, Bin2]),
    send(State, $H, []),
    {noreply, State#state{queue = queue:in(Req, Q)}};

handle_cast(Req = {_, {execute, _, PortalName, MaxRows}}, State) ->
    #state{queue = Q} = State,
    send(State, $E, [PortalName, 0, <<MaxRows:?int32>>]),
    send(State, $H, []),
    {noreply, State#state{queue = queue:in(Req, Q)}};

handle_cast(Req = {_, {describe, Type, Name}}, State) ->
    #state{queue = Q} = State,
    case Type of
        statement -> Type2 = $S;
        portal    -> Type2 = $P
    end,
    send(State, $D, [Type2, Name, 0]),
    send(State, $H, []),
    {noreply, State#state{queue = queue:in(Req, Q)}};

handle_cast(Req = {_, {close, Type, Name}}, State) ->
    #state{queue = Q} = State,
    case Type of
        statement -> Type2 = $S;
        portal    -> Type2 = $P
    end,
    send(State, $C, [Type2, Name, 0]),
    send(State, $H, []),
    {noreply, State#state{queue = queue:in(Req, Q)}};

handle_cast(Req = {_, sync}, State) ->
    #state{queue = Q} = State,
    send(State, $S, []),
    {noreply, State#state{queue = queue:in(Req, Q), sync_required = false}};

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
    %% TODO flush queue
    {stop, sock_closed, State};

handle_info({Error, Sock, Reason}, #state{sock = Sock} = State)
  when Error == tcp_error; Error == ssl_error ->
    %% TODO flush queue
    {stop, {sock_error, Reason}, State};

handle_info({_, Sock, Data2}, #state{data = Data, sock = Sock} = State) ->
    loop(State#state{data = <<Data/binary, Data2/binary>>}).

loop(#state{data = Data, handler = Handler} = State) ->
    case pgsql_wire:decode_message(Data) of
        {Message, Tail} ->
            case ?MODULE:Handler(Message, State#state{data = Tail}) of
                {noreply, State2} ->
                    loop(State2);
                R = {stop, _Reason2, _State2} ->
                    R
            end;
        _ ->
            {noreply, State}
    end.

terminate(_Reason, _State) ->
    %% TODO send termination msg, close socket ??
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%% -- internal functions --

cast(C, Command) ->
    Ref = make_ref(),
    gen_server:cast(C, {{self(), Ref}, Command}),
    Ref.

start_ssl(S, Flag, Opts, State) ->
    ok = gen_tcp:send(S, <<8:?int32, 80877103:?int32>>),
    Timeout = proplists:get_value(timeout, Opts, 5000),
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

notify(#state{queue = Q}, Message) ->
    {{From, Ref}, _} = queue:get(Q),
    From ! {Ref, Message}.

notify_async(#state{async = Pid}, Msg) ->
    case is_pid(Pid) of
        true  -> Pid ! {pgsql, self(), Msg};
        false -> false
    end.

request_tag(#state{queue = Q}) ->
    {_, Req} = queue:get(Q),
    element(1, Req).

%% -- backend message handling --

%% AuthenticationOk
auth({$R, <<0:?int32>>}, State) ->
    {noreply, State#state{handler = initializing}};

%% AuthenticationCleartextPassword
auth({$R, <<3:?int32>>}, State) ->
    send(State, $p, [get(password), 0]),
    {noreply, State};

%% AuthenticationMD5Password
auth({$R, <<5:?int32, Salt:4/binary>>}, State) ->
    Digest1 = hex(erlang:md5([get(password), get(username)])),
    Str = ["md5", hex(erlang:md5([Digest1, Salt])), 0],
    send(State, $p, Str),
    {noreply, State};

auth({$R, <<M:?int32, _/binary>>}, State) ->
    case M of
        2 -> Method = kerberosV5;
        4 -> Method = crypt;
        6 -> Method = scm;
        7 -> Method = gss;
        8 -> Method = sspi;
        _ -> Method = unknown
    end,
    notify(State, {error, {unsupported_auth_method, Method}}),
    {stop, normal, State};

%% ErrorResponse
auth({error, E}, State) ->
    case E#error.code of
        <<"28000">> -> Why = invalid_authorization_specification;
        <<"28P01">> -> Why = invalid_password;
        Any         -> Why = Any
    end,
    notify(State, {error, Why}),
    {stop, normal, State};

auth(Other, State) ->
    on_message(Other, State).

%% BackendKeyData
initializing({$K, <<Pid:?int32, Key:?int32>>}, State) ->
    State2 = State#state{backend = {Pid, Key}},
    {noreply, State2};

%% ReadyForQuery
initializing({$Z, <<Status:8>>}, State) ->
    #state{parameters = Parameters, queue = Q} = State,
    erase(username),
    erase(password),
    %% TODO decode dates to now() format
    case lists:keysearch(<<"integer_datetimes">>, 1, Parameters) of
        {value, {_, <<"on">>}}  -> put(datetime_mod, pgsql_idatetime);
        {value, {_, <<"off">>}} -> put(datetime_mod, pgsql_fdatetime)
    end,
    notify(State, connected),
    {noreply, State#state{handler = on_message,
                         txstatus = Status,
                         queue = queue:drop(Q)}};

initializing({error, _} = Error, State) ->
    notify(State, Error), 
    {stop, normal, State};

initializing(Other, State) ->
    on_message(Other, State).

%% ParseComplete
on_message({$1, <<>>}, State) ->
    {noreply, State};

%% ParameterDescription
on_message({$t, <<_Count:?int16, Bin/binary>>}, State) ->
    Types = [pgsql_types:oid2type(Oid) || <<Oid:?int32>> <= Bin],
    notify(State, {types, Types}),
    {noreply, State};

%% RowDescription
on_message({$T, <<Count:?int16, Bin/binary>>}, State) ->
    #state{queue = Q} = State,
    Columns = pgsql_wire:decode_columns(Count, Bin),
    Columns2 = [C#column{format = pgsql_wire:format(C#column.type)} || C <- Columns],
    notify(State, {columns, Columns2}),
    State2 = case request_tag(State) of
                 C when C == squery ->
                     State#state{columns = Columns2};
                 C when C == parse ->
                     State#state{queue = queue:drop(Q)}
             end,
    {noreply, State2};

%% NoData
on_message({$n, <<>>}, State) ->
    #state{queue = Q} = State,
    notify(State, no_data),
    {noreply, State#state{queue = queue:drop(Q)}};

%% BindComplete
on_message({$2, <<>>}, State) ->
    #state{queue = Q} = State,
    State2 = case queue:get(Q) of
                 {_, {equery, #statement{columns = C}, _}} ->
                     %% TODO remove this workaround, send Describe on handle_cast(equery)
                     notify(State, {columns, C}),
                     State;
                 {_, {bind, _, _, _}} ->
                     notify(State, ok),
                     State#state{queue = queue:drop(Q)}
    end,
    {noreply, State2};

%% CloseComplete
on_message({$3, <<>>}, State) ->
    #state{queue = Q} = State,
    State2 = case request_tag(State) of
                 C when C == close ->
                     notify(State, ok),
                     State#state{queue = queue:drop(Q)};
                 _ ->
                     State
             end,
    {noreply, State2};

%% DataRow
on_message({$D, <<_Count:?int16, Bin/binary>>}, State) ->
    #state{queue = Q} = State,
    Columns = case queue:get(Q) of
                  %% TODO use colums from State
                  {_, {equery, #statement{columns = C}, _}} ->
                      C;
                  {_, {execute, #statement{columns = C}, _, _}} ->
                      C;
                  {_, {squery, _}} ->
                      State#state.columns
              end,
    Data = pgsql_wire:decode_data(Columns, Bin),
    notify(State, {data, Data}),
    {noreply, State};

%% CommandComplete
on_message({$C, Bin}, State) ->
    #state{queue = Q} = State,
    Complete = pgsql_wire:decode_complete(Bin),
    notify(State, {complete, Complete}),
    State2 = case request_tag(State) of
                 C when C == execute ->
                     State#state{queue = queue:drop(Q)};
                 _ ->
                     State
             end,
    {noreply, State2};

%% EmptyQueryResponse
on_message({$I, _Bin}, State) ->
    notify(State, {complete, empty}),
    {noreply, State};

%% ReadyForQuery
on_message({$Z, <<_Status:8>>}, State) ->
    #state{queue = Q} = State,
    notify(State, done),
    {noreply, State#state{queue = queue:drop(Q)}};

on_message(Error = {error, _}, State) ->
    #state{queue = Q} = State,
    notify(State, Error),
    State2 = case request_tag(State) of
                 C when C == squery; C == equery ->
                     State;
                 _ ->
                     sync_required(State#state{queue = queue:drop(Q)})
             end,
    {noreply, State2};

%% NoticeResponse
on_message({$N, Data}, State) ->
    notify_async(State, {notice, pgsql_wire:decode_error(Data)}),
    {noreply, State};

%% ParameterStatus
on_message({$S, Data}, State) ->
    [Name, Value] = pgsql_wire:decode_strings(Data),
    Parameters2 = lists:keystore(Name, 1, State#state.parameters,
                                 {Name, Value}),
    {noreply, State#state{parameters = Parameters2}};

%% NotificationResponse
on_message({$A, <<Pid:?int32, Strings/binary>>}, State) ->
    case pgsql_wire:decode_strings(Strings) of
        [Channel, Payload] -> ok;
        [Channel]          -> Payload = <<>>
    end,
    %% TODO use it
    notify_async(State, {notification, Channel, Pid, Payload}),
    {noreply, State}.

sync_required(#state{queue = Q} = State) ->
    case queue:is_empty(Q) of
        false ->
            case request_tag(State) of
                T when T == sync ->
                    State;
                _ ->
                    notify(State, {error, sync_required}),
                    sync_required(State#state{queue = queue:drop(Q)})
            end;
        true ->
            State#state{sync_required = true}
    end.

to_binary(B) when is_binary(B) -> B;
to_binary(L) when is_list(L)   -> list_to_binary(L).

hex(Bin) ->
    HChar = fun(N) when N < 10 -> $0 + N;
               (N) when N < 16 -> $W + N
            end,
    <<<<(HChar(H)), (HChar(L))>> || <<H:4, L:4>> <= Bin>>.
