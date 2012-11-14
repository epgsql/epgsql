-module(epgsql_pool).

-behaviour(gen_server).

-export([start_link/1,
         id/1,
         all_conns/1,
         add_conn/2,
         get_conn/1]).

-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).

-define(HIBERNATE_AFTER_MIN, 1000).
-define(DESIRED_HIBERNATE, 10000).

-record(state, { queue }).

id(Pool) when is_atom(Pool) ->
    list_to_atom(lists:concat([epgsql_pool,":", Pool])).

start_link(Pool) ->
    gen_server:start_link({local, id(Pool)}, ?MODULE, [], [{timeout, infinity}]).

all_conns(Pool) ->
    gen_server:call(id(Pool), all_conns).

get_conn(Pool) ->
    case get(pgsql_conn) of
    undefined -> 
        gen_server:call(id(Pool), get_conn, infinity);
    Pid -> 
        Pid
    end.

add_conn(Pool, Pid) ->
    gen_server:cast(id(Pool), {add, Pid}).

%%----------------------------------------------------------------------------

init([]) ->
    {ok, #state { queue = queue:new() }}.

handle_call(all_conns, _From, #state{queue = Q} = State) ->
    {reply, queue:to_list(Q), State};

handle_call(get_conn, _From, State = #state{queue = Q}) ->
    case queue:out(Q) of
    {empty, _} ->
        {reply, {error, empty_pool}, State};
    {{value, Pid}, Q1} ->
        {reply, Pid, State#state{queue = queue:in(Pid, Q1)}}
    end;

handle_call(Msg, _From, State) ->
    {stop, {unexpected_call, Msg}, State}.

handle_cast({add, Pid}, State = #state{queue = Q}) ->
    %TODO: monitor??
    {noreply, State#state{queue = queue:in(Pid, Q)}};

handle_cast(Msg, State) ->
    {stop, {unexpected_cast, Msg}, State}.

handle_info(Msg, State) ->
    {stop, {unexpected_info, Msg}, State}.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

terminate(_Reason, State) ->
    State.

