%%% Special kind of command - it's exclusive: no other commands can run until
%%% this one finishes.
%%% It also uses some 'private' epgsql_sock's APIs
%%%
-module(epgsql_cmd_connect).
-behaviour(epgsql_command).
-export([init/1, execute/2, handle_message/4]).
-export_type([response/0]).

-type response() :: connected
                  | {error,
                     invalid_authorization_specification
                     | invalid_password
                     | epgsql:query_error()}.

-include("epgsql.hrl").
-include("protocol.hrl").

-record(connect,
        {opts :: list(),
         auth_method,
         stage = connect :: connect | auth | initialization}).

init({Host, Username, Password, Opts}) ->
    Opts1 = [{host, Host},
             {username, Username},
             {password, Password}
             | Opts],
    #connect{opts = Opts1}.

execute(PgSock, #connect{opts = Opts, stage = connect} = State) ->
    Host = get_val(host, Opts),
    Username = get_val(username, Opts),
    %% _ = get_val(password, Opts),
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

            PgSock1 = maybe_ssl(Sock, proplists:get_value(ssl, Opts, false), Opts, PgSock),

            Opts2 = ["user", 0, Username, 0],
            Opts3 = case proplists:get_value(database, Opts, undefined) of
                undefined -> Opts2;
                Database  -> [Opts2 | ["database", 0, Database, 0]]
            end,

            Replication = proplists:get_value(replication, Opts, undefined),
            Opts4 = case Replication of
                        undefined -> Opts3;
                        Replication  ->
                            [Opts3 | ["replication", 0, Replication, 0]]
                    end,
            PgSock2 = case Replication of
                          undefined -> PgSock1;
                          _ -> epgsql_sock:init_replication_state(PgSock1)
                      end,

            epgsql_sock:send(PgSock2, [<<196608:?int32>>, Opts4, 0]),
            PgSock3 = case proplists:get_value(async, Opts, undefined) of
                          undefined -> PgSock2;
                          Async -> epgsql_sock:set_attr(async, Async, PgSock2)
                      end,
            {ok, PgSock3, State#connect{stage = auth}};
        {error, Reason} = Error ->
            {stop, Reason, Error, PgSock} %FIXME: 'execute' supports only {ok, ...}
    end;
execute(PgSock, #connect{stage = auth, auth_method = cleartext, opts = Opts} = St) ->
    Password = get_val(password, Opts),
    epgsql_sock:send(PgSock, ?PASSWORD, [Password, 0]),
    {ok, PgSock, St};
execute(PgSock, #connect{stage = auth, auth_method = {md5, Salt}, opts = Opts} = St) ->
    User = get_val(username, Opts),
    Password = get_val(password, Opts),
    Digest1 = hex(erlang:md5([Password, User])),
    Str = ["md5", hex(erlang:md5([Digest1, Salt])), 0],
    epgsql_sock:send(PgSock, ?PASSWORD, Str),
    {ok, PgSock, St}.


maybe_ssl(S, false, _, PgSock) ->
    epgsql_sock:set_net_socket(gen_tcp, S, PgSock);
maybe_ssl(S, Flag, Opts, PgSock) ->
    ok = gen_tcp:send(S, <<8:?int32, 80877103:?int32>>),
    Timeout = proplists:get_value(timeout, Opts, 5000),
    {ok, <<Code>>} = gen_tcp:recv(S, 1, Timeout),
    case Code of
        $S  ->
            SslOpts = proplists:get_value(ssl_opts, Opts, []),
            case ssl:connect(S, SslOpts, Timeout) of
                {ok, S2}        ->
                    epgsql_sock:set_net_socket(ssl, S2, PgSock);
                {error, Reason} ->
                    exit({ssl_negotiation_failed, Reason})
            end;
        $N ->
            case Flag of
                true ->
                    epgsql_sock:set_net_socket(gen_tcp, S, PgSock);
                required ->
                    exit(ssl_not_available)
            end
    end.

%% --- Auth ---

%% AuthenticationOk
handle_message(?AUTHENTICATION_REQUEST, <<0:?int32>>, Sock, State) ->
    {noaction, Sock, State#connect{stage = initialization}};

%% AuthenticationCleartextPassword
handle_message(?AUTHENTICATION_REQUEST, <<3:?int32>>, Sock, St) ->
    {requeue, Sock, St#connect{stage = auth, auth_method = cleartext}};

%% AuthenticationMD5Password
handle_message(?AUTHENTICATION_REQUEST, <<5:?int32, Salt:4/binary>>, Sock, St) ->
    {requeue, Sock, St#connect{stage = auth, auth_method = {md5, Salt}}};

handle_message(?AUTHENTICATION_REQUEST, <<M:?int32, _/binary>>, Sock, _State) ->
    Method = case M of
        2 -> kerberosV5;
        4 -> crypt;
        6 -> scm;
        7 -> gss;
        8 -> sspi;
        _ -> unknown
    end,
    {stop, normal, {error, {unsupported_auth_method, Method}}, Sock};

%% --- Initialization ---

%% BackendKeyData
handle_message(?CANCELLATION_KEY, <<Pid:?int32, Key:?int32>>, Sock, _State) ->
    {noaction, epgsql_sock:set_attr(backend, {Pid, Key}, Sock)};

%% ReadyForQuery
handle_message(?READY_FOR_QUERY, _, Sock, _State) ->
    %% TODO decode dates to now() format
    case epgsql_sock:get_parameter_internal(<<"integer_datetimes">>, Sock) of
        <<"on">>  -> put(datetime_mod, epgsql_idatetime);
        <<"off">> -> put(datetime_mod, epgsql_fdatetime)
    end,
    Sock1 = epgsql_sock:set_attr(codec, epgsql_binary:new_codec([]), Sock),
    {finish, connected, connected, Sock1};


%% ErrorResponse
handle_message(?ERROR, Err, Sock, #connect{stage = auth} = _State) ->
    Why = case Err#error.code of
        <<"28000">> -> invalid_authorization_specification;
        <<"28P01">> -> invalid_password;
        Any         -> Any
    end,
    {stop, normal, {error, Why}, Sock};
handle_message(_, _, _, _) ->
    unknown.


get_val(Key, Proplist) ->
    Val = proplists:get_value(Key, Proplist),
    (Val =/= undefined) orelse error({required_option, Key}),
    Val.

hex(Bin) ->
    HChar = fun(N) when N < 10 -> $0 + N;
               (N) when N < 16 -> $W + N
            end,
    <<<<(HChar(H)), (HChar(L))>> || <<H:4, L:4>> <= Bin>>.
