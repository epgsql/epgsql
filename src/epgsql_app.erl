-module(epgsql_app).

-export([start/0]).

-behavior(application).

-export([start/2, stop/1]).

start() ->
    [application:start(A) || A <- [crypto, public_key, ssl, epgsql]].

start(normal, _) ->
    epgsql_sup:start_link().

stop(_) ->
    ok.
