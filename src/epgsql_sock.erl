%%% Copyright (C) 2009 - Will Glozer.  All rights reserved.
%%% Copyright (C) 2011 - Anton Lebedevich.  All rights reserved.

-module(epgsql_sock).

-behavior(gen_server).

-export([start_link/0,
         close/1,
         get_parameter/2,
         set_notice_receiver/2,
         get_cmd_status/1,
         cancel/1]).

-export([handle_call/3, handle_cast/2, handle_info/2]).
-export([init/1, code_change/3, terminate/2]).

%% state callbacks
-export([auth/2, initializing/2, on_message/2]).

-include("epgsql.hrl").
-include("epgsql_binary.hrl").

%% Commands defined as per this page:
%% http://www.postgresql.org/docs/9.2/static/protocol-message-formats.html

%% Commands
-define(BIND, $B).
-define(CLOSE, $C).
-define(DESCRIBE, $D).
-define(EXECUTE, $E).
-define(FLUSH, $H).
-define(PASSWORD, $p).
-define(PARSE, $P).
-define(SIMPLEQUERY, $Q).
-define(AUTHENTICATION_REQUEST, $R).
-define(SYNC, $S).

%% Parameters

-define(PREPARED_STATEMENT, $S).
-define(PORTAL, $P).

%% Responses

-define(PARSE_COMPLETE, $1).
-define(BIND_COMPLETE, $2).
-define(CLOSE_COMPLETE, $3).
-define(NOTIFICATION, $A).
-define(COMMAND_COMPLETE, $C).
-define(DATA_ROW, $D).
-define(EMPTY_QUERY, $I).
-define(CANCELLATION_KEY, $K).
-define(NO_DATA, $n).
-define(NOTICE, $N).
-define(PORTAL_SUSPENDED, $s).
-define(PARAMETER_STATUS, $S).
-define(PARAMETER_DESCRIPTION, $t).
-define(ROW_DESCRIPTION, $T).
-define(READY_FOR_QUERY, $Z).
-define(COPY_BOTH_RESPONSE, $W).
-define(COPY_DATA, $d).

% CopyData replication messages
-define(X_LOG_DATA, $w).
-define(PRIMARY_KEEPALIVE_MESSAGE, $k).
-define(STANDBY_STATUS_UPDATE, $r).

-record(state, {mod,
                sock,
                data = <<>>,
                backend,
                handler,
                codec,
                queue = queue:new(),
                async,
                parameters = [],
                types = [],
                columns = [],
                rows = [],
                results = [],
                batch = [],
                sync_required,
                txstatus,
                complete_status :: undefined | atom() | {atom(), integer()},
                repl_last_received_lsn,
                repl_last_flushed_lsn,
                repl_last_applied_lsn,
                repl_feedback_required,
                repl_cbmodule,
                repl_cbstate,
                repl_receiver}).

%% -- client interface --

start_link() ->
    gen_server:start_link(?MODULE, [], []).

close(C) when is_pid(C) ->
    catch gen_server:cast(C, stop),
    ok.

get_parameter(C, Name) ->
    gen_server:call(C, {get_parameter, to_binary(Name)}, infinity).

set_notice_receiver(C, PidOrName) when is_pid(PidOrName);
                                       is_atom(PidOrName) ->
    gen_server:call(C, {set_async_receiver, PidOrName}, infinity).

get_cmd_status(C) ->
    gen_server:call(C, get_cmd_status, infinity).

cancel(S) ->
    gen_server:cast(S, cancel).

%% -- gen_server implementation --

init([]) ->
    {ok, #state{}}.

handle_call({update_type_cache, TypeInfos}, _From, #state{codec = Codec} = State) ->
    Codec2 = epgsql_binary:update_type_cache(TypeInfos, Codec),
    {reply, ok, State#state{codec = Codec2}};

handle_call({get_parameter, Name}, _From, State) ->
    Value1 = case lists:keysearch(Name, 1, State#state.parameters) of
        {value, {Name, Value}} -> Value;
        false                  -> undefined
    end,
    {reply, {ok, Value1}, State};

handle_call({set_async_receiver, PidOrName}, _From, #state{async = Previous} = State) ->
    {reply, {ok, Previous}, State#state{async = PidOrName}};

handle_call(get_cmd_status, _From, #state{complete_status = Status} = State) ->
    {reply, {ok, Status}, State};

handle_call({standby_status_update, FlushedLSN, AppliedLSN}, _From,
    #state{repl_last_received_lsn = ReceivedLSN} = State) ->
    send(State, ?COPY_DATA, epgsql_wire:encode_standby_status_update(ReceivedLSN, FlushedLSN, AppliedLSN)),
    {reply, ok, State#state{repl_last_flushed_lsn = FlushedLSN, repl_last_applied_lsn = AppliedLSN}};

handle_call(Command, From, State) ->
    #state{queue = Q} = State,
    Req = {{call, From}, Command},
    command(Command, State#state{queue = queue:in(Req, Q),
                                 complete_status = undefined}).

handle_cast({{Method, From, Ref}, Command} = Req, State)
  when ((Method == cast) or (Method == incremental)),
       is_pid(From),
       is_reference(Ref)  ->
    #state{queue = Q} = State,
    command(Command, State#state{queue = queue:in(Req, Q),
                                 complete_status = undefined});

handle_cast(stop, State) ->
    {stop, normal, flush_queue(State, {error, closed})};

handle_cast(cancel, State = #state{backend = {Pid, Key},
                                   sock = TimedOutSock}) ->
    {ok, {Addr, Port}} = case State#state.mod of
                             gen_tcp -> inet:peername(TimedOutSock);
                             ssl -> ssl:peername(TimedOutSock)
                         end,
    SockOpts = [{active, false}, {packet, raw}, binary],
    %% TODO timeout
    {ok, Sock} = gen_tcp:connect(Addr, Port, SockOpts),
    Msg = <<16:?int32, 80877102:?int32, Pid:?int32, Key:?int32>>,
    ok = gen_tcp:send(Sock, Msg),
    gen_tcp:close(Sock),
    {noreply, State}.

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

handle_info({_, Sock, Data2}, #state{data = Data, sock = Sock} = State) ->
    loop(State#state{data = <<Data/binary, Data2/binary>>}).

terminate(_Reason, #state{sock = undefined}) -> ok;
terminate(_Reason, #state{mod = gen_tcp, sock = Sock}) -> gen_tcp:close(Sock);
terminate(_Reason, #state{mod = ssl, sock = Sock}) -> ssl:close(Sock).

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%% -- internal functions --

command(Command, State = #state{sync_required = true})
  when Command /= sync ->
    {noreply, finish(State, {error, sync_required})};

command({connect, Host, Username, Password, Opts}, State) ->
    Timeout = proplists:get_value(timeout, Opts, 5000),
    Port = proplists:get_value(port, Opts, 5432),
    SockOpts = [{active, false}, {packet, raw}, binary, {nodelay, true}, {keepalive, true}],
    case gen_tcp:connect(Host, Port, SockOpts, Timeout) of
        {ok, Sock} ->

            %% Increase the buffer size.  Following the recommendation in the inet man page:
            %%
            %%    It is recommended to have val(buffer) >=
            %%    max(val(sndbuf),val(recbuf)).

            {ok, [{recbuf, RecBufSize}, {sndbuf, SndBufSize}]} =
                inet:getopts(Sock, [recbuf, sndbuf]),
            inet:setopts(Sock, [{buffer, max(RecBufSize, SndBufSize)}]),

            State2 = case proplists:get_value(ssl, Opts) of
                         T when T == true; T == required ->
                             start_ssl(Sock, T, Opts, State);
                         _ ->
                             State#state{mod  = gen_tcp, sock = Sock}
                     end,

            Opts2 = ["user", 0, Username, 0],
            Opts3 = case proplists:get_value(database, Opts, undefined) of
                undefined -> Opts2;
                Database  -> [Opts2 | ["database", 0, Database, 0]]
            end,

            Opts4 = case proplists:get_value(replication, Opts, undefined) of
                        undefined -> Opts3;
                        Replication  -> [Opts3 | ["replication", 0, Replication, 0]]
                    end,

            send(State2, [<<196608:?int32>>, Opts4, 0]),
            Async   = proplists:get_value(async, Opts, undefined),
            setopts(State2, [{active, true}]),
            put(username, Username),
            put(password, Password),
            {noreply,
             State2#state{handler = auth,
                          async = Async}};

        {error, Reason} = Error ->
            {stop, Reason, finish(State, Error)}
    end;

command({squery, Sql}, State) ->
    send(State, ?SIMPLEQUERY, [Sql, 0]),
    {noreply, State};

%% TODO add fast_equery command that doesn't need parsed statement,
%% uses default (text) column format,
%% sends Describe after Bind to get RowDescription
command({equery, Statement, Parameters}, #state{codec = Codec} = State) ->
    #statement{name = StatementName, columns = Columns} = Statement,
    Bin1 = epgsql_wire:encode_parameters(Parameters, Codec),
    Bin2 = epgsql_wire:encode_formats(Columns),
    send_multi(State, [
        {?BIND, ["", 0, StatementName, 0, Bin1, Bin2]},
        {?EXECUTE, ["", 0, <<0:?int32>>]},
        {?CLOSE, [?PREPARED_STATEMENT, StatementName, 0]},
        {?SYNC, []}
    ]),
    {noreply, State};

command({prepared_query, Statement, Parameters}, #state{codec = Codec} = State) ->
    #statement{name = StatementName, columns = Columns} = Statement,
    Bin1 = epgsql_wire:encode_parameters(Parameters, Codec),
    Bin2 = epgsql_wire:encode_formats(Columns),
    send_multi(State, [
        {?BIND, ["", 0, StatementName, 0, Bin1, Bin2]},
        {?EXECUTE, ["", 0, <<0:?int32>>]},
        {?SYNC, []}
    ]),
    {noreply, State};

command({parse, Name, Sql, Types}, State) ->
    Bin = epgsql_wire:encode_types(Types, State#state.codec),
    send_multi(State, [
        {?PARSE, [Name, 0, Sql, 0, Bin]},
        {?DESCRIBE, [?PREPARED_STATEMENT, Name, 0]},
        {?FLUSH, []}
    ]),
    {noreply, State};

command({bind, Statement, PortalName, Parameters}, #state{codec = Codec} = State) ->
    #statement{name = StatementName, columns = Columns, types = Types} = Statement,
    Typed_Parameters = lists:zip(Types, Parameters),
    Bin1 = epgsql_wire:encode_parameters(Typed_Parameters, Codec),
    Bin2 = epgsql_wire:encode_formats(Columns),
    send_multi(State, [
        {?BIND, [PortalName, 0, StatementName, 0, Bin1, Bin2]},
        {?FLUSH, []}
    ]),
    {noreply, State};

command({execute, _Statement, PortalName, MaxRows}, State) ->
    send_multi(State, [
        {?EXECUTE, [PortalName, 0, <<MaxRows:?int32>>]},
        {?FLUSH, []}
    ]),
    {noreply, State};

command({execute_batch, Batch}, #state{codec = Codec} = State) ->
    Commands =
        lists:foldr(
          fun({Statement, Parameters}, Acc) ->
                  #statement{name = StatementName,
                             columns = Columns,
                             types = Types} = Statement,
                  Typed_Parameters = lists:zip(Types, Parameters),
                  Bin1 = epgsql_wire:encode_parameters(Typed_Parameters, Codec),
                  Bin2 = epgsql_wire:encode_formats(Columns),
                  [{?BIND, [0, StatementName, 0, Bin1, Bin2]},
                   {?EXECUTE, [0, <<0:?int32>>]} | Acc]
          end,
          [{?SYNC, []}],
          Batch),
    send_multi(State, Commands),
    {noreply, State};

command({describe_statement, Name}, State) ->
    send_multi(State, [
        {?DESCRIBE, [?PREPARED_STATEMENT, Name, 0]},
        {?FLUSH, []}
    ]),
    {noreply, State};

command({describe_portal, Name}, State) ->
    send_multi(State, [
        {?DESCRIBE, [?PORTAL, Name, 0]},
        {?FLUSH, []}
    ]),
    {noreply, State};

command({close, Type, Name}, State) ->
    Type2 = case Type of
        statement -> ?PREPARED_STATEMENT;
        portal    -> ?PORTAL
    end,
    send_multi(State, [
        {?CLOSE, [Type2, Name, 0]},
        {?FLUSH, []}
    ]),
    {noreply, State};

command(sync, State) ->
    send(State, ?SYNC, []),
    {noreply, State#state{sync_required = false}};

command({start_replication, ReplicationSlot, Callback, CbInitState, WALPosition, PluginOpts}, State) ->
    Sql1 = ["START_REPLICATION SLOT """, ReplicationSlot, """ LOGICAL ", WALPosition],
    Sql2 =
        case PluginOpts of
            [] -> Sql1;
            PluginOpts -> [Sql1 , " (", PluginOpts, ")"]
        end,

    State2 =
        case Callback of
            Pid when is_pid(Pid) -> State#state{repl_receiver = Pid};
            Module -> State#state{repl_cbmodule = Module, repl_cbstate = CbInitState}
        end,

    Hex = [H || H <- WALPosition, H =/= $/],
    {ok, [LSN], _} = io_lib:fread("~16u", Hex),

    State3 = State2#state{repl_last_flushed_lsn = LSN, repl_last_applied_lsn = LSN},

    send(State3, ?SIMPLEQUERY, [Sql2, 0]),
    {noreply, State3}.

start_ssl(S, Flag, Opts, State) ->
    ok = gen_tcp:send(S, <<8:?int32, 80877103:?int32>>),
    Timeout = proplists:get_value(timeout, Opts, 5000),
    {ok, <<Code>>} = gen_tcp:recv(S, 1, Timeout),
    case Code of
        $S  ->
            SslOpts = proplists:get_value(ssl_opts, Opts, []),
            case ssl:connect(S, SslOpts, Timeout) of
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
    do_send(Mod, Sock, epgsql_wire:encode(Data)).

send(#state{mod = Mod, sock = Sock}, Type, Data) ->
    do_send(Mod, Sock, epgsql_wire:encode(Type, Data)).

send_multi(#state{mod = Mod, sock = Sock}, List) ->
    do_send(Mod, Sock, lists:map(fun({Type, Data}) ->
        epgsql_wire:encode(Type, Data)
    end, List)).

do_send(gen_tcp, Sock, Bin) ->
    try erlang:port_command(Sock, Bin) of
        true ->
            ok
    catch
        error:_Error ->
            {error,einval}
    end;

do_send(Mod, Sock, Bin) ->
    Mod:send(Sock, Bin).

loop(#state{data = Data, handler = Handler, repl_last_received_lsn = LastReceivedLSN,
    repl_last_flushed_lsn = LastFlushedLSN, repl_last_applied_lsn = LastAppliedLSN,
    repl_feedback_required = ReplFeedbackRequired} = State) ->
    case epgsql_wire:decode_message(Data) of
        {Message, Tail} ->
            case ?MODULE:Handler(Message, State#state{data = Tail}) of
                {noreply, State2} ->
                    loop(State2);
                R = {stop, _Reason2, _State2} ->
                    R
            end;
        _ ->
            %% in replication mode send feedback after each batch of messages
            case ReplFeedbackRequired of
                true ->
                    send(State, ?COPY_DATA, epgsql_wire:encode_standby_status_update(
                        LastReceivedLSN, LastFlushedLSN, LastAppliedLSN)),
                    {noreply, State#state{repl_feedback_required = false}};
                _ ->
                    {noreply, State}
            end
    end.

finish(State, Result) ->
    finish(State, Result, Result).

finish(State = #state{queue = Q}, Notice, Result) ->
    case queue:get(Q) of
        {{cast, From, Ref}, _} ->
            From ! {self(), Ref, Result};
        {{incremental, From, Ref}, _} ->
            From ! {self(), Ref, Notice};
        {{call, From}, _} ->
            gen_server:reply(From, Result)
    end,
    State#state{queue = queue:drop(Q),
                types = [],
                columns = [],
                rows = [],
                results = [],
                batch = []}.

add_result(State, Notice, Result) ->
    #state{queue = Q, results = Results, batch = Batch} = State,
    Results2 = case queue:get(Q) of
                   {{incremental, From, Ref}, _} ->
                       From ! {self(), Ref, Notice},
                       Results;
                   _ ->
                       [Result | Results]
               end,
    Batch2 = case Batch of
                 [] -> [];
                 _ -> tl(Batch)
             end,
    State#state{types = [],
                columns = [],
                rows = [],
                results = Results2,
                batch = Batch2}.

add_row(State = #state{queue = Q, rows = Rows}, Data) ->
    Rows2 = case queue:get(Q) of
                {{incremental, From, Ref}, _} ->
                    From ! {self(), Ref, {data, Data}},
                    Rows;
                _ ->
                    [Data | Rows]
            end,
    State#state{rows = Rows2}.

notify(State = #state{queue = Q}, Notice) ->
    case queue:get(Q) of
        {{incremental, From, Ref}, _} ->
            From ! {self(), Ref, Notice};
        _ ->
            ignore
    end,
    State.

notify_async(#state{async = undefined}, _) ->
    false;
notify_async(#state{async = PidOrName}, Msg) ->
    try PidOrName ! {epgsql, self(), Msg} of
        _ -> true
    catch error:badarg ->
            %% no process registered under this name
            false
    end.

command_tag(#state{queue = Q}) ->
    {_, Req} = queue:get(Q),
    if is_tuple(Req) ->
            element(1, Req);
       is_atom(Req) ->
            Req
    end.

get_columns(State) ->
    #state{queue = Q, columns = Columns, batch = Batch} = State,
    case queue:get(Q) of
        {_, {Command, #statement{columns = C}, _}}  when Command == equery; Command == prepared_query ->
            C;
        {_, {execute, #statement{columns = C}, _, _}} ->
            C;
        {_, {squery, _}} ->
            Columns;
        {_, {execute_batch, _}} ->
            [{#statement{columns = C}, _} | _] = Batch,
            C
    end.

make_statement(State) ->
    #state{queue = Q, types = Types, columns = Columns} = State,
    Name = case queue:get(Q) of
               {_, {parse, N, _, _}} -> N;
               {_, {describe_statement, N}} -> N
           end,
    #statement{name = Name, types = Types, columns = Columns}.

sync_required(#state{queue = Q} = State) ->
    case queue:is_empty(Q) of
        false ->
            case command_tag(State) of
                sync ->
                    State;
                _ ->
                    sync_required(finish(State, {error, sync_required}))
            end;
        true ->
            State#state{sync_required = true}
    end.

flush_queue(#state{queue = Q} = State, Error) ->
    case queue:is_empty(Q) of
        false ->
            flush_queue(finish(State, Error), Error);
        true -> State
    end.

to_binary(B) when is_binary(B) -> B;
to_binary(L) when is_list(L)   -> list_to_binary(L).

hex(Bin) ->
    HChar = fun(N) when N < 10 -> $0 + N;
               (N) when N < 16 -> $W + N
            end,
    <<<<(HChar(H)), (HChar(L))>> || <<H:4, L:4>> <= Bin>>.

%% -- backend message handling --

%% AuthenticationOk
auth({?AUTHENTICATION_REQUEST, <<0:?int32>>}, State) ->
    {noreply, State#state{handler = initializing}};

%% AuthenticationCleartextPassword
auth({?AUTHENTICATION_REQUEST, <<3:?int32>>}, State) ->
    send(State, ?PASSWORD, [get(password), 0]),
    {noreply, State};

%% AuthenticationMD5Password
auth({?AUTHENTICATION_REQUEST, <<5:?int32, Salt:4/binary>>}, State) ->
    Digest1 = hex(erlang:md5([get(password), get(username)])),
    Str = ["md5", hex(erlang:md5([Digest1, Salt])), 0],
    send(State, ?PASSWORD, Str),
    {noreply, State};

auth({?AUTHENTICATION_REQUEST, <<M:?int32, _/binary>>}, State) ->
    Method = case M of
        2 -> kerberosV5;
        4 -> crypt;
        6 -> scm;
        7 -> gss;
        8 -> sspi;
        _ -> unknown
    end,
    State2 = finish(State, {error, {unsupported_auth_method, Method}}),
    {stop, normal, State2};

%% ErrorResponse
auth({error, E}, State) ->
    Why = case E#error.code of
        <<"28000">> -> invalid_authorization_specification;
        <<"28P01">> -> invalid_password;
        Any         -> Any
    end,
    {stop, normal, finish(State, {error, Why})};

auth(Other, State) ->
    on_message(Other, State).

%% BackendKeyData
initializing({?CANCELLATION_KEY, <<Pid:?int32, Key:?int32>>}, State) ->
    {noreply, State#state{backend = {Pid, Key}}};

%% ReadyForQuery
initializing({?READY_FOR_QUERY, <<Status:8>>}, State) ->
    #state{parameters = Parameters} = State,
    erase(username),
    erase(password),
    %% TODO decode dates to now() format
    case lists:keysearch(<<"integer_datetimes">>, 1, Parameters) of
        {value, {_, <<"on">>}}  -> put(datetime_mod, epgsql_idatetime);
        {value, {_, <<"off">>}} -> put(datetime_mod, epgsql_fdatetime)
    end,
    State2 = finish(State#state{handler = on_message,
                               txstatus = Status,
                               codec = epgsql_binary:new_codec([])},
                   connected),
    {noreply, State2};

initializing({error, _} = Error, State) ->
    {stop, normal, finish(State, Error)};

initializing(Other, State) ->
    on_message(Other, State).

%% ParseComplete
on_message({?PARSE_COMPLETE, <<>>}, State) ->
    {noreply, State};

%% ParameterDescription
on_message({?PARAMETER_DESCRIPTION, <<_Count:?int16, Bin/binary>>}, State) ->
    Types = [epgsql_binary:oid2type(Oid, State#state.codec) || <<Oid:?int32>> <= Bin],
    State2 = notify(State#state{types = Types}, {types, Types}),
    {noreply, State2};

%% RowDescription
on_message({?ROW_DESCRIPTION, <<Count:?int16, Bin/binary>>}, State) ->
    Columns = epgsql_wire:decode_columns(Count, Bin, State#state.codec),
    Columns2 =
        case command_tag(State) of
            C when C == describe_portal; C == squery ->
                Columns;
            C when C == parse; C == describe_statement ->
                [Col#column{format = epgsql_wire:format(Col#column.type)}
                 || Col <- Columns]
        end,
    State2 = State#state{columns = Columns2},
    Message = {columns, Columns2},
    State3 = case command_tag(State2) of
                 squery ->
                     notify(State2, Message);
                 T when T == parse; T == describe_statement ->
                     finish(State2, Message, {ok, make_statement(State2)});
                 describe_portal ->
                     finish(State2, Message, {ok, Columns})
             end,
    {noreply, State3};

%% NoData
on_message({?NO_DATA, <<>>}, State) ->
    State2 = case command_tag(State) of
                 C when C == parse; C == describe_statement ->
                     finish(State, no_data, {ok, make_statement(State)});
                 describe_portal ->
                     finish(State, no_data, {ok, []})
             end,
    {noreply, State2};

%% BindComplete
on_message({?BIND_COMPLETE, <<>>}, State) ->
    State2 = case command_tag(State) of
                 Command when Command == equery; Command == prepared_query ->
                     %% TODO send Describe as a part of equery, needs text format support
                     notify(State, {columns, get_columns(State)});
                 bind ->
                     finish(State, ok);
                 execute_batch ->
                     Batch =
                         case State#state.batch of
                             [] ->
                                 {_, {_, B}} = queue:get(State#state.queue),
                                 B;
                             B -> B
                         end,
                     State#state{batch = Batch}
             end,
    {noreply, State2};

%% CloseComplete
on_message({?CLOSE_COMPLETE, <<>>}, State) ->
    State2 = case command_tag(State) of
                 Command when Command == equery; Command == prepared_query ->
                     State;
                 close ->
                     finish(State, ok)
             end,
    {noreply, State2};

%% DataRow
on_message({?DATA_ROW, <<_Count:?int16, Bin/binary>>}, State) ->
    Data = epgsql_wire:decode_data(get_columns(State), Bin, State#state.codec),
    {noreply, add_row(State, Data)};

%% PortalSuspended
on_message({?PORTAL_SUSPENDED, <<>>}, State) ->
    State2 = finish(State,
                   suspended,
                   {partial, lists:reverse(State#state.rows)}),
    {noreply, State2};

%% CommandComplete
on_message({?COMMAND_COMPLETE, Bin}, State0) ->
    Complete = epgsql_wire:decode_complete(Bin),
    State = State0#state{complete_status = Complete},
    Command = command_tag(State),
    Notice = {complete, Complete},
    Rows = lists:reverse(State#state.rows),
    Columns = get_columns(State),
    State2 = case {Command, Complete, Columns} of
                 {execute, {_, Count}, []} ->
                     finish(State, Notice, {ok, Count});
                 {execute, {_, Count}, _} ->
                     finish(State, Notice, {ok, Count, Rows});
                 {execute, _, _} ->
                     finish(State, Notice, {ok, Rows});
                 {execute_batch, {_, Count}, []} ->
                     add_result(State, Notice, {ok, Count});
                 {execute_batch, {_, Count}, _} ->
                     add_result(State, Notice, {ok, Count, Rows});
                 {execute_batch, _, _} ->
                     add_result(State, Notice, {ok, Rows});
                 {C, {_, Count}, []} when C == squery; C == equery; C == prepared_query ->
                     add_result(State, Notice, {ok, Count});
                 {C, {_, Count}, _} when C == squery; C == equery; C == prepared_query ->
                     add_result(State, Notice, {ok, Count, Columns, Rows});
                 {C, _, _} when C == squery; C == equery; C == prepared_query ->
                     add_result(State, Notice, {ok, Columns, Rows})
             end,
    {noreply, State2};

%% EmptyQueryResponse
on_message({?EMPTY_QUERY, _Bin}, State) ->
    Notice = {complete, empty},
    State2 = case command_tag(State) of
                 execute ->
                     finish(State, Notice, {ok, [], []});
                 C when C == squery; C == equery; C == prepared_query ->
                     add_result(State, Notice, {ok, [], []})
             end,
    {noreply, State2};

%% ReadyForQuery
on_message({?READY_FOR_QUERY, <<Status:8>>}, State) ->
    State2 = case command_tag(State) of
                 squery ->
                     case State#state.results of
                         [Result] ->
                             finish(State, done, Result);
                         Results ->
                             finish(State, done, lists:reverse(Results))
                     end;
                 execute_batch ->
                     finish(State, done, lists:reverse(State#state.results));
                 Command when Command == equery; Command == prepared_query ->
                     case State#state.results of
                         [Result] ->
                             finish(State, done, Result);
                         [] ->
                             finish(State, done)
                     end;
                 sync ->
                     finish(State, ok)
             end,
    {noreply, State2#state{txstatus = Status}};

on_message(Error = {error, Reason}, State) ->
    case queue:is_empty(State#state.queue) of
        true ->
            {stop, {shutdown, Reason}, State};
        false ->
            State2 = case command_tag(State) of
                C when C == squery; C == equery; C == execute_batch; C == prepared_query ->
                    add_result(State, Error, Error);
                _ ->
                    sync_required(finish(State, Error))
            end,
            {noreply, State2}
    end;

%% NoticeResponse
on_message({?NOTICE, Data}, State) ->
    notify_async(State, {notice, epgsql_wire:decode_error(Data)}),
    {noreply, State};

%% ParameterStatus
on_message({?PARAMETER_STATUS, Data}, State) ->
    [Name, Value] = epgsql_wire:decode_strings(Data),
    Parameters2 = lists:keystore(Name, 1, State#state.parameters,
                                 {Name, Value}),
    {noreply, State#state{parameters = Parameters2}};

%% NotificationResponse
on_message({?NOTIFICATION, <<Pid:?int32, Strings/binary>>}, State) ->
    {Channel1, Payload1} = case epgsql_wire:decode_strings(Strings) of
        [Channel, Payload] -> {Channel, Payload};
        [Channel]          -> {Channel, <<>>}
    end,
    notify_async(State, {notification, Channel1, Pid, Payload1}),
    {noreply, State};

%% CopyBothResponse
on_message({?COPY_BOTH_RESPONSE, _Data}, State) ->
    State2 = finish(State, ok),
    {noreply, State2};

%% CopyData for COPY command. COPY command not supported yet.
on_message({?COPY_DATA, _Data}, #state{repl_cbmodule = undefined, repl_receiver = undefined} = State) ->
    {stop, {error, copy_command_not_supported}, State};

%% CopyData for Replication mode
on_message({?COPY_DATA, <<?PRIMARY_KEEPALIVE_MESSAGE:8, LSN:?int64, _Timestamp:?int64, ReplyRequired:8>>},
    #state{repl_last_flushed_lsn = LastFlushedLSN, repl_last_applied_lsn = LastAppliedLSN} = State) ->
    case ReplyRequired of
        1 ->
            send(State, ?COPY_DATA,
                epgsql_wire:encode_standby_status_update(LSN, LastFlushedLSN, LastAppliedLSN)),
            {noreply, State#state{repl_feedback_required = false, repl_last_received_lsn = LSN}};
        _ ->
            {noreply, State#state{repl_feedback_required = true, repl_last_received_lsn = LSN}}
    end;

%% CopyData for Replication mode. with async messages
on_message({?COPY_DATA, <<?X_LOG_DATA, StartLSN:?int64, EndLSN:?int64, _Timestamp:?int64, WALRecord/binary>>},
    #state{repl_cbmodule = undefined, repl_receiver = Receiver} = State) ->
    Receiver ! {epgsql, self(), {x_log_data, StartLSN, EndLSN, WALRecord}},
    {noreply, State#state{repl_feedback_required = true, repl_last_received_lsn = EndLSN}};

%% CopyData for Replication mode. with callback method
on_message({?COPY_DATA, <<?X_LOG_DATA, StartLSN:?int64, EndLSN:?int64, _Timestamp:?int64, WALRecord/binary>>},
    #state{repl_cbmodule = CbModule, repl_cbstate = CbState, repl_receiver = undefined} = State) ->
    {ok, LastFlushedLSN, LastAppliedLSN, NewCbState} =
        CbModule:handle_x_log_data(StartLSN, EndLSN, WALRecord, CbState),
    {noreply, State#state{repl_feedback_required = true, repl_last_received_lsn = EndLSN,
        repl_last_flushed_lsn = LastFlushedLSN, repl_last_applied_lsn = LastAppliedLSN,
        repl_cbstate = NewCbState}}.
