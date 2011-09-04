-module(pgsql_wire).

-export([init/1,
         decode/2,
         encode/2,
         encode/3]).

-include("pgsql_binary.hrl").

-record(state, {options, tail}).

init(Options) ->
    #state{options = Options}.

decode(Data, State = #state{}) ->
    {ok, State}.

encode(Data, State = #state{}) ->
    Bin = iolist_to_binary(Data),
    Msg = <<(byte_size(Bin) + 4):?int32, Bin/binary>>,
    {Msg, State}.

encode(Type, Data, State = #state{}) ->
    Bin = iolist_to_binary(Data),
    Msg = <<Type:8, (byte_size(Bin) + 4):?int32, Bin/binary>>,
    {Msg, State}.
