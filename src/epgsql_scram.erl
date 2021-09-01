%%% coding: utf-8
%%% @doc
%%% SCRAM--SHA-256 helper functions
%%%
%%% <ul>
%%%  <li>[https://www.postgresql.org/docs/current/static/sasl-authentication.html]</li>
%%%  <li>[https://en.wikipedia.org/wiki/Salted_Challenge_Response_Authentication_Mechanism]</li>
%%%  <li>[https://tools.ietf.org/html/rfc7677]</li>
%%%  <li>[https://tools.ietf.org/html/rfc5802]</li>
%%% </ul>
%%% @end

-module(epgsql_scram).
-export([get_nonce/1,
         get_client_first/2,
         get_client_final/4,
         parse_server_first/2,
         parse_server_final/1]).
-export([hi/3,
         hmac/2,
         h/1,
         bin_xor/2]).

-type nonce() :: binary().
-type server_first() :: [{nonce, nonce()} |
                         {salt, binary()} |
                         {i, pos_integer()} |
                         {raw, binary()}].

-spec get_client_first(iodata(), nonce()) -> iodata().
get_client_first(UserName, Nonce) ->
    %% Username is ignored by postgresql
    [<<"n,,">> | client_first_bare(UserName, Nonce)].

client_first_bare(UserName, Nonce) ->
    [<<"n=">>, UserName, <<",r=">>, Nonce].

%% @doc Generate unique ASCII string.
%% Resulting string length isn't guaranteed, but it's guaranteed to be unique and will
%% contain `NumRandomBytes' of random data.
-spec get_nonce(pos_integer()) -> nonce().
get_nonce(NumRandomBytes) when NumRandomBytes < 255 ->
    Random = crypto:strong_rand_bytes(NumRandomBytes),
    Unique = binary:encode_unsigned(unique()),
    NonceBin = <<NumRandomBytes, Random:NumRandomBytes/binary, Unique/binary>>,
    base64:encode(NonceBin).

-spec parse_server_first(binary(), nonce()) -> server_first().
parse_server_first(ServerFirst, ClientNonce) ->
    PartsB = binary:split(ServerFirst, <<",">>, [global]),
    (length(PartsB) == 3) orelse error({invalid_server_first, ServerFirst}),
    Parts =
        lists:map(
          fun(<<"r=", R/binary>>) ->
                  {nonce, R};
             (<<"s=", S/binary>>) ->
                  {salt, base64:decode(S)};
             (<<"i=", I/binary>>) ->
                  {i, binary_to_integer(I)}
          end, PartsB),
    check_nonce(ClientNonce, proplists:get_value(nonce, Parts)),
    [{raw, ServerFirst} | Parts].

%% SaltedPassword  := Hi(Normalize(password), salt, i)
%% ClientKey       := HMAC(SaltedPassword, "Client Key")
%% StoredKey       := H(ClientKey)
%% AuthMessage     := client-first-message-bare + "," + server-first-message + "," + client-final-message-without-proof
%% ClientSignature := HMAC(StoredKey, AuthMessage)
%% ClientProof     := ClientKey XOR ClientSignature
-spec get_client_final(server_first(), nonce(), iodata(), iodata()) ->
                              {ClientFinal :: iodata(), ServerSignature :: binary()}.
get_client_final(SrvFirst, ClientNonce, UserName, Password) ->
    ChannelBinding = <<"c=biws">>,                 %channel-binding isn't implemented
    Nonce = [<<"r=">>, proplists:get_value(nonce, SrvFirst)],

    Salt = proplists:get_value(salt, SrvFirst),
    I = proplists:get_value(i, SrvFirst),

    SaltedPassword = hi(epgsql_sasl_prep_profile:validate(Password), Salt, I),
    ClientKey = hmac(SaltedPassword, "Client Key"),
    StoredKey = h(ClientKey),
    ClientFirstBare = client_first_bare(UserName, ClientNonce),
    ServerFirst = proplists:get_value(raw, SrvFirst),
    ClientFinalWithoutProof = [ChannelBinding, ",", Nonce],
    AuthMessage = [ClientFirstBare, ",", ServerFirst, ",", ClientFinalWithoutProof],
    ClientSignature = hmac(StoredKey, AuthMessage),
    ClientProof = bin_xor(ClientKey, ClientSignature),

    ServerKey = hmac(SaltedPassword, "Server Key"),
    ServerSignature = hmac(ServerKey, AuthMessage),

    {[ClientFinalWithoutProof, ",p=", base64:encode(ClientProof)], ServerSignature}.

-spec parse_server_final(binary()) -> {ok, binary()} | {error, binary()}.
parse_server_final(<<"v=", ServerFinal/binary>>) ->
    [ServerFinal1 | _] = binary:split(ServerFinal, <<",">>),
    {ok, base64:decode(ServerFinal1)};
parse_server_final(<<"e=", ServerError/binary>>) ->
    {error, ServerError}.

%% Helpers

check_nonce(ClientNonce, ServerNonce) ->
    Size = size(ClientNonce),
    <<ClientNonce:Size/binary, _/binary>> = ServerNonce,
    true.

hi(Str, Salt, I) ->
    U1 = hmac(Str, <<Salt/binary, 1:32/integer-big>>),
    hi1(Str, U1, U1, I - 1).

hi1(_Str, _U, Hi, 0) ->
    Hi;
hi1(Str, U, Hi, I) ->
    U2 = hmac(Str, U),
    Hi1 = bin_xor(Hi, U2),
    hi1(Str, U2, Hi1, I - 1).

-ifdef(OTP_RELEASE).
 -if(?OTP_RELEASE >= 23).
 hmac(Key, Str) ->
     crypto:mac(hmac, sha256, Key, Str).
 -else.
 hmac(Key, Str) ->
     crypto:hmac(sha256, Key, Str).
 -endif.
-else.
hmac(Key, Str) ->
    crypto:hmac(sha256, Key, Str).
-endif.

h(Str) ->
    crypto:hash(sha256, Str).

%% word 'xor' is reserved
bin_xor(B1, B2) ->
    crypto:exor(B1, B2).

unique() ->
    erlang:unique_integer([positive]).


-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").

exchange_test() ->
    Password = <<"foobar">>,
    Nonce = <<"9IZ2O01zb9IgiIZ1WJ/zgpJB">>,
    Username = <<>>,

    ClientFirst = <<"n,,n=,r=9IZ2O01zb9IgiIZ1WJ/zgpJB">>,
    ServerFirst = <<"r=9IZ2O01zb9IgiIZ1WJ/zgpJBjx/oIRLs02gGSHcw1KEty3eY,s=fs3IXBy7U7+IvVjZ,i=4096">>,
    ClientFinal = <<"c=biws,r=9IZ2O01zb9IgiIZ1WJ/zgpJBjx/oIRLs02gGSHcw1KEty3eY,p=AmNKosjJzS31NTlQ"
                    "YNs5BTeQjdHdk7lOflDo5re2an8=">>,
    ServerFinal = <<"v=U+ppxD5XUKtradnv8e2MkeupiA8FU87Sg8CXzXHDAzw=">>,

    ?assertEqual(ClientFirst, iolist_to_binary(get_client_first(Username, Nonce))),
    SF = parse_server_first(ServerFirst, Nonce),
    {CF, ServerProof} = get_client_final(SF, Nonce, Username, Password),
    ?assertEqual(ClientFinal, iolist_to_binary(CF)),
    ?assertEqual({ok, ServerProof}, parse_server_final(ServerFinal)).

-endif.
