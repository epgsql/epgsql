-module(epgsql_pool_sup).

-export([id/1, start_link/3]).

-behaviour(supervisor).

-export([init/1]).

id(Pool) ->
    list_to_atom(lists:concat(["epgsql_pool_sup:", Pool])).

start_link(Id, Pool, Opts) ->
    supervisor:start_link({local, Id}, ?MODULE, [Pool, Opts]). 

init([Pool, Opts]) ->
    PoolSize = proplists:get_value(pool_size, Opts, 2),
    {ok, {{one_for_one, 10, 10},
            [{epgsql_pool:id(Pool), {epgsql_pool, start_link, [Pool]}, transient,
                16#ffffffff, worker, [epgsql_pool]} |
             [{connid(Pool, I), {epgsql, connect, [Pool, Opts]}, transient, 
                16#ffffffff, worker, [pgsql_conn]} || I <- lists:seq(1, PoolSize)]]}}.
    
connid(Pool, I) ->
    list_to_atom(lists:concat([pgsql_conn, ":", Pool, ":", integer_to_list(I)])). 

