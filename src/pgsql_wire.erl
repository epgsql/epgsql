-module(pgsql_wire).

-export([init/1,
         decode/2,
         encode/2]).

-record(state, {}).

init(Options) ->
    #state{}.

decode(Data, State = #state{}) ->
    {ok, State}.

encode(Data, State = #state{}) ->
    {Data, State}.
