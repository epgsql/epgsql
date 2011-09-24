-module(pgsql_wire).

-export([decode_message/1,
         decode_error/1,
         decode_strings/1,
         encode/2]).

-include("pgsql.hrl").
-include("pgsql_binary.hrl").

decode_message(<<Type:8, Len:?int32, Rest/binary>> = Bin) ->
    Len2 = Len - 4,
    case Rest of
        <<Data:Len2/binary, Tail/binary>> ->
            case Type of
                $E ->
                    {{error, decode_error(Data)}, Tail};
                _ ->
                    {{Type, Data}, Tail}
            end;
        _Other ->
            Bin
    end;

decode_message(Bin) ->
    Bin.

%% decode a single null-terminated string
%% TODO signature changed, returns [Str, Rest], old code expects {Str, Rest}
decode_string(Bin) ->
    binary:split(Bin, <<0>>).

%% decode multiple null-terminated string
decode_strings(Bin) ->
    binary:split(Bin, <<0>>, [global, trim]).

%% decode field
decode_fields(Bin) ->
    decode_fields(Bin, []).

decode_fields(<<0>>, Acc) ->
    Acc;
decode_fields(<<Type:8, Rest/binary>>, Acc) ->
    [Str, Rest2] = decode_string(Rest),
    decode_fields(Rest2, [{Type, Str} | Acc]).

%% decode ErrorResponse
%% TODO add fields from http://www.postgresql.org/docs/9.0/interactive/protocol-error-fields.html
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

encode(Data) ->
    Bin = iolist_to_binary(Data),
    <<(byte_size(Bin) + 4):?int32, Bin/binary>>.

encode(Type, Data) ->
    Bin = iolist_to_binary(Data),
    <<Type:8, (byte_size(Bin) + 4):?int32, Bin/binary>>.
