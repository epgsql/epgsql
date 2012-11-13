-module(epgsql_sup).

-export([start_link/0]).

-behaviour(supervisor).

-export([init/1]).

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

init([]) ->
   {ok, Pools} = application:get_env(pools), 
   {ok, { {one_for_one, 10, 10}, [
            poolsup(Pool, Opts) || {Pool, Opts} <- Pools]}}.

poolsup(Pool, Opts) ->
    PoolId = epgsql_pool_sup:id(Pool),
    {PoolId, {epgsql_pool_sup, start_link, [PoolId, Pool, Opts]},
        transient, 16#ffffffff, supervisor, [epgsql_pool_sup]}.

