%%% Special kind of command - it's exclusive: no other commands can run until
%%% this one finishes.
%%% It also uses some 'private' epgsql_sock's APIs
%%%
-module(epgsql_cmd_connect).
-behaviour(epgsql_command).
-export([hide_password/1, opts_hide_password/1]).
-export([init/1, execute/2, handle_message/4]).
-export_type([response/0, connect_error/0]).

-type response() :: connected
                  | {error, connect_error()}.
-type connect_error() ::
        invalid_authorization_specification
      | invalid_password
      | {unsupported_auth_method,
         kerberosV5 | crypt | scm | gss | sspi | {unknown, integer()} | {sasl, [binary()]}}
      | {sasl_server_final, any()}
      | epgsql:query_error().

-include("epgsql.hrl").
-include("protocol.hrl").

-type auth_fun() :: fun((init | binary(), _, _) ->
                                     {send, byte(), iodata(), any()}
                                   | ok
                                   | {error, any()}
                                   | unknown).

-record(connect,
        {opts :: map(),
         auth_fun :: auth_fun() | undefined,
         auth_state :: any() | undefined,
         auth_send :: {integer(), iodata()} | undefined,
         stage = connect :: connect | maybe_auth | auth | initialization}).

-define(SCRAM_AUTH_METHOD, <<"SCRAM-SHA-256">>).
-define(AUTH_OK, 0).
-define(AUTH_CLEARTEXT, 3).
-define(AUTH_MD5, 5).
-define(AUTH_SASL, 10).
-define(AUTH_SASL_CONTINUE, 11).
-define(AUTH_SASL_FINAL, 12).

init(#{host := _, username := _} = Opts) ->
    #connect{opts = Opts}.

execute(PgSock, #connect{opts = #{host := Host} = Opts, stage = connect} = State) ->
    Timeout = maps:get(timeout, Opts, 5000),
    Port = maps:get(port, Opts, 5432),
    SockOpts = [{active, false}, {packet, raw}, binary, {nodelay, true}, {keepalive, true}],
    case gen_tcp:connect(Host, Port, SockOpts, Timeout) of
        {ok, Sock} ->
            client_handshake(Sock, PgSock, State);
        {error, Reason} = Error ->
            {stop, Reason, Error, PgSock}
    end;
execute(PgSock, #connect{stage = auth, auth_send = {PacketId, Data}} = St) ->
    epgsql_sock:send(PgSock, PacketId, Data),
    {ok, PgSock, St#connect{auth_send = undefined}}.

client_handshake(Sock, PgSock, #connect{opts = #{username := Username} = Opts} = State) ->
    %% Increase the buffer size.  Following the recommendation in the inet man page:
    %%
    %%    It is recommended to have val(buffer) >=
    %%    max(val(sndbuf),val(recbuf)).

    {ok, [{recbuf, RecBufSize}, {sndbuf, SndBufSize}]} =
        inet:getopts(Sock, [recbuf, sndbuf]),
    inet:setopts(Sock, [{buffer, max(RecBufSize, SndBufSize)}]),

    case maybe_ssl(Sock, maps:get(ssl, Opts, false), Opts, PgSock) of
        {error, Reason} ->
            {stop, Reason, {error, Reason}, PgSock};
        PgSock1 ->
            Opts2 = ["user", 0, Username, 0],
            Opts3 = case maps:find(database, Opts) of
                        error -> Opts2;
                        {ok, Database}  -> [Opts2 | ["database", 0, Database, 0]]
                    end,

            {Opts4, PgSock2} =
                case Opts of
                    #{replication := Replication}  ->
                        {[Opts3 | ["replication", 0, Replication, 0]],
                         epgsql_sock:init_replication_state(PgSock1)};
                    _ -> {Opts3, PgSock1}
                end,

            epgsql_sock:send(PgSock2, [<<196608:?int32>>, Opts4, 0]),
            PgSock3 = case Opts of
                          #{async := Async} ->
                              epgsql_sock:set_attr(async, Async, PgSock2);
                          _ -> PgSock2
                      end,
            {ok, PgSock3, State#connect{stage = maybe_auth}}
    end.


%% @doc Replace `password' in Opts map with obfuscated one
opts_hide_password(#{password := Password} = Opts) ->
    HiddenPassword = hide_password(Password),
    Opts#{password => HiddenPassword};
opts_hide_password(Opts) -> Opts.


%% @doc this function wraps plaintext password to a lambda function, so, if
%% epgsql_sock process crashes when executing `connect` command, password will
%% not appear in a crash log
-spec hide_password(iodata()) -> fun( () -> iodata() ).
hide_password(Password) when is_list(Password);
                             is_binary(Password) ->
    fun() ->
            Password
    end;
hide_password(PasswordFun) when is_function(PasswordFun, 0) ->
    PasswordFun.


maybe_ssl(S, false, _, PgSock) ->
    epgsql_sock:set_net_socket(gen_tcp, S, PgSock);
maybe_ssl(S, Flag, Opts, PgSock) ->
    ok = gen_tcp:send(S, <<8:?int32, 80877103:?int32>>),
    Timeout = maps:get(timeout, Opts, 5000),
    {ok, <<Code>>} = gen_tcp:recv(S, 1, Timeout),
    case Code of
        $S  ->
            SslOpts = maps:get(ssl_opts, Opts, []),
            case ssl:connect(S, SslOpts, Timeout) of
                {ok, S2}        ->
                    epgsql_sock:set_net_socket(ssl, S2, PgSock);
                {error, Reason} ->
                    Err = {ssl_negotiation_failed, Reason},
                    {error, Err}
            end;
        $N ->
            case Flag of
                true ->
                    epgsql_sock:set_net_socket(gen_tcp, S, PgSock);
                required ->
                    {error, ssl_not_available}
            end
    end.

%% Auth sub-protocol

auth_init(<<?AUTH_CLEARTEXT:?int32>>, Sock, St) ->
    auth_init(fun auth_cleartext/3, undefined, Sock, St);
auth_init(<<?AUTH_MD5:?int32, Salt:4/binary>>, Sock, St) ->
    auth_init(fun auth_md5/3, Salt, Sock, St);
auth_init(<<?AUTH_SASL:?int32, MethodsB/binary>>, Sock, St) ->
    Methods = epgsql_wire:decode_strings(MethodsB),
    case lists:member(?SCRAM_AUTH_METHOD, Methods) of
        true ->
            auth_init(fun auth_scram/3, undefined, Sock, St);
        false ->
            {stop, normal, {error, {unsupported_auth_method,
                                    {sasl, lists:delete(<<>>, Methods)}}}}
    end;
auth_init(<<M:?int32, _/binary>>, Sock, _St) ->
    Method = case M of
                 2 -> kerberosV5;
                 4 -> crypt;
                 6 -> scm;
                 7 -> gss;
                 8 -> sspi;
                 _ -> {unknown, M}
             end,
    {stop, normal, {error, {unsupported_auth_method, Method}}, Sock}.

auth_init(Fun, InitState, PgSock, St) ->
    auth_handle(init, PgSock, St#connect{auth_fun = Fun, auth_state = InitState,
                                         stage = auth}).

auth_handle(Data, PgSock, #connect{auth_fun = Fun, auth_state = AuthSt} = St) ->
    case Fun(Data, AuthSt, St) of
        {send, SendPacketId, SendData, AuthSt1} ->
            {requeue, PgSock, St#connect{auth_state = AuthSt1,
                                         auth_send = {SendPacketId, SendData}}};
        ok -> {noaction, PgSock, St};
        {error, Reason} ->
            {stop, normal, {error, Reason}};
        unknown -> unknown
    end.

%% AuthenticationCleartextPassword
auth_cleartext(init, _AuthState, #connect{opts = Opts}) ->
    Password = get_password(Opts),
    {send, ?PASSWORD, [Password, 0], undefined};
auth_cleartext(_, _, _) -> unknown.

%% AuthenticationMD5Password
auth_md5(init, Salt, #connect{opts = Opts}) ->
    User = maps:get(username, Opts),
    Password = get_password(Opts),
    Digest1 = hex(erlang:md5([Password, User])),
    Str = ["md5", hex(erlang:md5([Digest1, Salt])), 0],
    {send, ?PASSWORD, Str, undefined};
auth_md5(_, _, _) -> unknown.

%% AuthenticationSASL
auth_scram(init, undefined, #connect{opts = Opts}) ->
    User = maps:get(username, Opts),
    Nonce = epgsql_scram:get_nonce(16),
    ClientFirst = epgsql_scram:get_client_first(User, Nonce),
    SaslInitialResponse = [?SCRAM_AUTH_METHOD, 0, <<(iolist_size(ClientFirst)):?int32>>, ClientFirst],
    {send, ?SASL_ANY_RESPONSE, SaslInitialResponse, {auth_request, Nonce}};
auth_scram(<<?AUTH_SASL_CONTINUE:?int32, ServerFirst/binary>>, {auth_request, Nonce}, #connect{opts = Opts}) ->
    User = maps:get(username, Opts),
    Password = get_password(Opts),
    ServerFirstParts = epgsql_scram:parse_server_first(ServerFirst, Nonce),
    {ClientFinalMessage, ServerProof} = epgsql_scram:get_client_final(ServerFirstParts, Nonce, User, Password),
    {send, ?SASL_ANY_RESPONSE, ClientFinalMessage, {server_final, ServerProof}};
auth_scram(<<?AUTH_SASL_FINAL:?int32, ServerFinalMsg/binary>>, {server_final, ServerProof}, _Conn) ->
    case epgsql_scram:parse_server_final(ServerFinalMsg) of
        {ok, ServerProof} -> ok;
        Other -> {error, {sasl_server_final, Other}}
    end;
auth_scram(_, _, _) ->
    unknown.


%% --- Auth ---

%% AuthenticationOk
handle_message(?AUTHENTICATION_REQUEST, <<?AUTH_OK:?int32>>, Sock, State) ->
    {noaction, Sock, State#connect{stage = initialization,
                                   auth_fun = undefined,
                                   auth_state = undefned,
                                   auth_send = undefined}};

handle_message(?AUTHENTICATION_REQUEST, Message, Sock, #connect{stage = Stage} = St) when Stage =/= auth ->
    auth_init(Message, Sock, St);

handle_message(?AUTHENTICATION_REQUEST, Packet, Sock, #connect{stage = auth} = St) ->
    auth_handle(Packet, Sock, St);

%% --- Initialization ---

%% BackendKeyData
handle_message(?CANCELLATION_KEY, <<Pid:?int32, Key:?int32>>, Sock, _State) ->
    {noaction, epgsql_sock:set_attr(backend, {Pid, Key}, Sock)};

%% ReadyForQuery
handle_message(?READY_FOR_QUERY, _, Sock, _State) ->
    Codec = epgsql_binary:new_codec(Sock, []),
    Sock1 = epgsql_sock:set_attr(codec, Codec, Sock),
    {finish, connected, connected, Sock1};


%% ErrorResponse
handle_message(?ERROR, #error{code = Code} = Err, Sock, #connect{stage = Stage} = _State) ->
    IsAuthStage = (Stage == auth) orelse (Stage == maybe_auth),
    Why = case Code of
              <<"28000">> when IsAuthStage ->
                  invalid_authorization_specification;
              <<"28P01">> when IsAuthStage ->
                  invalid_password;
              _ ->
                  Err
          end,
    {stop, normal, {error, Why}, Sock};
handle_message(_, _, _, _) ->
    unknown.


get_password(Opts) ->
    PasswordFun = maps:get(password, Opts),
    PasswordFun().


hex(Bin) ->
    HChar = fun(N) when N < 10 -> $0 + N;
               (N) when N < 16 -> $W + N
            end,
    <<<<(HChar(H)), (HChar(L))>> || <<H:4, L:4>> <= Bin>>.
