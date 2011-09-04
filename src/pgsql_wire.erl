-module(pgsql_wire).

-export([init/1,
         decode_messages/2,
         encode/2,
         encode/3]).

-include("pgsql_binary.hrl").

-record(state, {options, tail}).

init(Options) ->
    #state{options = Options}.

decode_messages(Bin, #state{tail = Tail} = State) ->
    decode_messages([], <<Bin/binary, Tail/binary>>, State).

encode(Data, State = #state{}) ->
    Bin = iolist_to_binary(Data),
    <<(byte_size(Bin) + 4):?int32, Bin/binary>>.

encode(Type, Data, State = #state{}) ->
    Bin = iolist_to_binary(Data),
    <<Type:8, (byte_size(Bin) + 4):?int32, Bin/binary>>.

decode_messages(Acc, <<Type:8, Len:?int32, Rest/binary>> = Bin, State) ->
    Len2 = Len - 4,
    case Rest of
        <<Data:Len2/binary, Tail/binary>> ->
            decode([{Type, Data} | Acc], Tail, State);
        _Other ->
            {lists:reverse(Acc), State#state{tail = Bin}}
    end;

decode_messages(Acc, Bin, State) ->
    {lists:reverse(Acc), State#state{tail = Bin}}.
