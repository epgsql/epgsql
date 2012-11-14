-module(epgsql_builder).

-import(io_lib, [format/2]).
-import(list, [concat/1]).
-import(string, [join/2]).

-export([select/1, select/2, select/3]).
-export([insert/2, insert/3]).
-export([update/3]).
-export([delete/1, delete/2]).
-export([truncate/1]).

select(Table) ->
    binary(format("select * from ~s;", [Table])).

select(Table, Fields) when is_list(Fields) ->
    binary(format("select ~s from ~s;", [fields(Fields), Table]));

select(Table, Where) when is_tuple(Where) ->
    binary(format("select * from ~s where ~s;", [Table, where(Where)])).

select(Table, Fields, Where) when is_list(Fields) and is_tuple(Where) ->
    binary(format("select ~s from ~s where ~s;", 
                   [fields(Fields), Table, where(Where)])).

insert(Table, Record) ->
	{Fields, Values} = lists:unzip(Record),
    insert(Table, Fields, [Values]).

insert(Table, Fields, Rows) -> 
	Str = fun(Row) -> join([value(V) || V <- Row], ",") end,
	Rows1 = [concat(["(", Str(Row), ")"]) || Row <- Rows],
    binary(format("insert into ~s(~s) values~s;", 
                  [Table, fields(Fields), join(Rows1, ",")])).

update(Table, Record, Where) when is_list(Record) ->
    Columns = join([concat([list(F), "=", value(V)]) 
                    || {F, V} <- Record], ","),
    binary(format("update ~s set ~s where ~s;", 
                  [Table, Columns, where(Where)])).

delete(Table) ->
    binary(format("delete from ~s;", [Table])).

delete(Table, Where) when is_tuple(Where) ->
    binary(format("delete from ~s where ~s;", 
                   [Table, where(Where)])).

truncate(Table) ->
    binary(format("truncate table ~s;", [Table])).

where({'and', L, R}) ->
	where(L) ++ " and " ++ where(R);

where({'and', List}) when is_list(List) ->
	string:join([where(E) || E <- List], " and ");

where({'or', L, R}) ->
	where(L) ++ " or " ++ where(R);

where({'or', List}) when is_list(List) ->
	string:join([where(E) || E <- List], " or ");

where({like, Field, Value}) ->	
	list(Field) ++ " like " ++ value(Value);

where({'<', Field, Value}) ->	
	atom_to_list(Field) ++ " < " ++ value(Value);

where({'>', Field, Value}) ->	
	atom_to_list(Field) ++ " > " ++ value(Value);

where({'in', Field, Values}) ->	
	InStr = string:join([value(Value) || Value <- Values], ","),
	atom_to_list(Field) ++ " in (" ++ InStr ++ ")";

where({Field, Value}) ->
	atom_to_list(Field) ++ " = " ++ value(Value).

fields(Fields) ->
    string:join([list(F) || F <- Fields], " ,").

value(Val) when Val == undefined; Val == null ->
    "NULL";
value(Val) when is_binary(Val) ->
    quote(Val);
value(Val) when is_atom(Val) ->
    quote(atom_to_list(Val));
value(Val) when is_list(Val) ->
    quote(Val);
value(Val) when is_integer(Val) ->
    integer_to_list(Val);
value(Val) when is_float(Val) ->
    [Res] = io_lib:format("~w", [Val]),
    Res;
value({datetime, Val}) ->
    value(Val);
value({{Year, Month, Day}, {Hour, Minute, Second}}) ->
    Res = two_digits([Year, Month, Day, Hour, Minute, Second]),
    lists:flatten(Res);
value({TimeType, Val})
  when TimeType == 'date';
       TimeType == 'time' ->
    value(Val);
value({Time1, Time2, Time3}) ->
    Res = two_digits([Time1, Time2, Time3]),
    lists:flatten(Res);
value(Val) ->
    {error, {unrecognized_value, Val}}.

two_digits(Nums) when is_list(Nums) ->
    [two_digits(Num) || Num <- Nums];
two_digits(Num) ->
    [Str] = io_lib:format("~b", [Num]),
    case length(Str) of
	1 -> [$0 | Str];
	_ -> Str
    end.

%%  Quote a string or binary value so that it can be included safely in a query.
quote(Bin) when is_binary(Bin) ->
    quote(binary_to_list(Bin));
quote(String) when is_list(String) ->
    [39 | lists:reverse([39 | quote(String, [])])].

quote([], Acc) ->
    Acc;
quote([0 | Rest], Acc) ->
    quote(Rest, [$0, $\\ | Acc]);
quote([10 | Rest], Acc) ->
    quote(Rest, [$n, $\\ | Acc]);
quote([13 | Rest], Acc) ->
    quote(Rest, [$r, $\\ | Acc]);
quote([$\\ | Rest], Acc) ->
    quote(Rest, [$\\ , $\\ | Acc]);
quote([39 | Rest], Acc) ->		%% 39 is $'
    quote(Rest, [39, $\\ | Acc]);	%% 39 is $'
quote([34 | Rest], Acc) ->		%% 34 is $"
    quote(Rest, [34, $\\ | Acc]);	%% 34 is $"
quote([26 | Rest], Acc) ->
    quote(Rest, [$Z, $\\ | Acc]);
quote([C | Rest], Acc) ->
    quote(Rest, [C | Acc]).

binary(L) when is_list(L) -> iolist_to_binary(L).

list(L) when is_list(L) -> L;

list(A) when is_atom(A) -> atom_to_list(A);

list(B) when is_binary(B) -> binary_to_list(B).

