-module(pgsql_wire).

-export([init/1,
         decode_messages/2,
         decode_error/1,
         decode_strings/1,
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


%% decode a single null-terminated string
decode_string(Bin) ->
    decode_string(Bin, <<>>).

decode_string(<<0, Rest/binary>>, Str) ->
    {Str, Rest};
decode_string(<<C, Rest/binary>>, Str) ->
    decode_string(Rest, <<Str/binary, C>>).

%% decode multiple null-terminated string
decode_strings(Bin) ->
    decode_strings(Bin, []).

decode_strings(<<>>, Acc) ->
    lists:reverse(Acc);
decode_strings(Bin, Acc) ->
    {Str, Rest} = decode_string(Bin),
    decode_strings(Rest, [Str | Acc]).

%% decode field
decode_fields(Bin) ->
    decode_fields(Bin, []).

decode_fields(<<0>>, Acc) ->
    Acc;
decode_fields(<<Type:8, Rest/binary>>, Acc) ->
    {Str, Rest2} = decode_string(Rest),
    decode_fields(Rest2, [{Type, Str} | Acc]).

%% decode ErrorResponse
decode_error(Bin) ->
    Fields = decode_fields(Bin),
    Error = #error{
      severity = lower_atom(proplists:get_value($S, Fields)),
      code     = proplists:get_value($C, Fields),
      message  = proplists:get_value($M, Fields),
      extra    = decode_error_extra(Fields)},
    Error.

decode_error_extra(Fields) ->
    Types = [{$D, detail}, {$H, hint}, {$P, position}],
    decode_error_extra(Types, Fields, []).

decode_error_extra([], _Fields, Extra) ->
    Extra;
decode_error_extra([{Type, Name} | T], Fields, Extra) ->
    case proplists:get_value(Type, Fields) of
        undefined -> decode_error_extra(T, Fields, Extra);
        Value     -> decode_error_extra(T, Fields, [{Name, Value} | Extra])
    end.

lower_atom(Str) when is_binary(Str) ->
    lower_atom(binary_to_list(Str));
lower_atom(Str) when is_list(Str) ->
    list_to_atom(string:to_lower(Str)).
