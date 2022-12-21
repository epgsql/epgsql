%%% @doc Connects to the server and performs all the necessary handshakes.
%%%
%%% Special kind of command - it's exclusive: no other commands can run until
%%% this one finishes.
%%% It also uses some "private" epgsql_sock's APIs
%%%
-module(epgsql_cmd_connect).
-behaviour(epgsql_command).
-export([hide_password/1, opts_hide_password/1, open_socket/2]).
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

execute(PgSock, #connect{opts = #{username := Username} = Opts, stage = connect} = State) ->
    SockOpts = prepare_tcp_opts(maps:get(tcp_opts, Opts, [])),
    FilteredOpts = filter_sensitive_info(Opts),
    PgSock1 = epgsql_sock:set_attr(connect_opts, FilteredOpts, PgSock),
    case open_socket(SockOpts, Opts) of
        {ok, Mode, Sock} ->
            PgSock2 = epgsql_sock:set_net_socket(Mode, Sock, PgSock1),
            Opts2 = ["user", 0, Username, 0],
            Opts3 = case maps:find(database, Opts) of
                        error -> Opts2;
                        {ok, Database}  -> [Opts2 | ["database", 0, Database, 0]]
                    end,
           {Opts4, PgSock3} =
               case Opts of
                   #{replication := Replication}  ->
                       {[Opts3 | ["replication", 0, Replication, 0]],
                        epgsql_sock:init_replication_state(PgSock2)};
                   _ -> {Opts3, PgSock2}
               end,
            Opts5 = case Opts of
                        #{application_name := ApplicationName}  ->
                            [Opts4 | ["application_name", 0, ApplicationName, 0]];
                        _ ->
                            Opts4
                    end,
           ok = epgsql_sock:send(PgSock3, [<<196608:?int32>>, Opts5, 0]),
           PgSock4 = case Opts of
                         #{async := Async} ->
                             epgsql_sock:set_attr(async, Async, PgSock3);
                         _ -> PgSock3
                     end,
           {ok, PgSock4, State#connect{stage = maybe_auth}};
        {error, Reason} = Error ->
            {stop, Reason, Error, PgSock}
    end;
execute(PgSock, #connect{stage = auth, auth_send = {PacketType, Data}} = St) ->
    {send, PacketType, Data, PgSock, St#connect{auth_send = undefined}}.

-spec open_socket([{atom(), any()}], epgsql:connect_opts()) ->
    {ok , gen_tcp | ssl, gen_tcp:socket() | ssl:sslsocket()} | {error, any()}.
open_socket(SockOpts, #{host := Host} = ConnectOpts) ->
    Timeout = maps:get(timeout, ConnectOpts, 5000),
    Deadline = deadline(Timeout),
    Port = maps:get(port, ConnectOpts, 5432),
    case gen_tcp:connect(Host, Port, SockOpts, Timeout) of
       {ok, Sock} ->
           client_handshake(Sock, ConnectOpts, Deadline);
       {error, _Reason} = Error ->
           Error
    end.

client_handshake(Sock, ConnectOpts, Deadline) ->
    case maps:is_key(tcp_opts, ConnectOpts) of
        false ->
            %% Increase the buffer size.  Following the recommendation in the inet man page:
            %%
            %%    It is recommended to have val(buffer) >=
            %%    max(val(sndbuf),val(recbuf)).
            {ok, [{recbuf, RecBufSize}, {sndbuf, SndBufSize}]} =
                inet:getopts(Sock, [recbuf, sndbuf]),
            inet:setopts(Sock, [{buffer, max(RecBufSize, SndBufSize)}]);
        true ->
            %% All TCP options are provided by the user
            noop
    end,
    maybe_ssl(Sock, maps:get(ssl, ConnectOpts, false), ConnectOpts, Deadline).

maybe_ssl(Sock, false, _ConnectOpts, _Deadline) ->
    {ok, gen_tcp, Sock};
maybe_ssl(Sock, Flag, ConnectOpts, Deadline) ->
    ok = gen_tcp:send(Sock, <<8:?int32, 80877103:?int32>>),
    Timeout0 = timeout(Deadline),
    case gen_tcp:recv(Sock, 1, Timeout0) of
        {ok, <<$S>>}  ->
            SslOpts = maps:get(ssl_opts, ConnectOpts, []),
            Timeout = timeout(Deadline),
            case ssl:connect(Sock, [{active, false} | SslOpts], Timeout) of
                {ok, Sock2} ->
                    {ok, ssl, Sock2};
                {error, Reason} ->
                    Err = {ssl_negotiation_failed, Reason},
                    {error, Err}
            end;
        {ok, <<$N>>} ->
            case Flag of
                true ->
                   {ok, gen_tcp, Sock};
                required ->
                    {error, ssl_not_available}
            end;
        {error, Reason} ->
            {error, Reason}
    end.

%% @doc Replace `password' in Opts map with obfuscated one
opts_hide_password(#{password := Password} = Opts) ->
    HiddenPassword = hide_password(Password),
    Opts#{password => HiddenPassword};
opts_hide_password(Opts) -> Opts.

%% @doc password and username are sensitive data that should not be stored in a
%% permanent state that might crash during code upgrade
filter_sensitive_info(Opts0) ->
  maps:without([password, username], Opts0).

%% @doc this function wraps plaintext password to a lambda function, so, if
%% epgsql_sock process crashes when executing `connect' command, password will
%% not appear in a crash log
-spec hide_password(iodata()) -> fun( () -> iodata() ).
hide_password(Password) when is_list(Password);
                             is_binary(Password) ->
    fun() ->
            Password
    end;
hide_password(PasswordFun) when is_function(PasswordFun, 0) ->
    PasswordFun.

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
                                   auth_state = undefined,
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
handle_message(?READY_FOR_QUERY, _, Sock, #connect{opts = Opts}) ->
    CodecOpts = maps:with([nulls], Opts),
    Codec = epgsql_binary:new_codec(Sock, CodecOpts),
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

prepare_tcp_opts([]) ->
    [{active, false}, {packet, raw}, {mode, binary}, {nodelay, true}, {keepalive, true}];
prepare_tcp_opts(Opts0) ->
    case lists:filter(fun(binary) -> true;
                         (list) -> true;
                         ({mode, _}) -> true;
                         ({packet, _}) -> true;
                         ({packet_size, _}) -> true;
                         ({header, _}) -> true;
                         ({active, _}) -> true;
                         (_) -> false
                      end, Opts0) of
        [] ->
            [{active, false}, {packet, raw}, {mode, binary} | Opts0];
        Forbidden ->
            error({forbidden_tcp_opts, Forbidden})
    end.


get_password(Opts) ->
    PasswordFun = maps:get(password, Opts),
    PasswordFun().


hex(Bin) ->
    HChar = fun(N) when N < 10 -> $0 + N;
               (N) when N < 16 -> $W + N
            end,
    <<<<(HChar(H)), (HChar(L))>> || <<H:4, L:4>> <= Bin>>.

deadline(Timeout) ->
    erlang:monotonic_time(milli_seconds) + Timeout.

timeout(Deadline) ->
    erlang:max(0, Deadline - erlang:monotonic_time(milli_seconds)).
