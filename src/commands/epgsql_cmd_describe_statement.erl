%% Almost the same as "parse"
%% > Describe
%% < ParameterDescription
%% < RowDescription | NoData
-module(epgsql_cmd_describe_statement).
-behaviour(epgsql_command).
-export([init/1, execute/2, handle_message/4]).
-export_type([response/0]).

-include("epgsql.hrl").
-include("../epgsql_binary.hrl").

-type response() :: {ok, #statement{}} | {error, epgsql:query_error()}.

-record(desc_stmt,
        {name :: iodata(),
         parameter_descr}).

init(Name) ->
    #desc_stmt{name = Name}.

execute(Sock, #desc_stmt{name = Name} = St) ->
    epgsql_sock:send_multi(
      Sock,
      [
       {?DESCRIBE, [?PREPARED_STATEMENT, Name, 0]},
       {?FLUSH, []}
      ]),
    {ok, Sock, St}.

handle_message(?PARAMETER_DESCRIPTION, Bin, Sock, State) ->
    Codec = epgsql_sock:get_codec(Sock),
    Types = epgsql_wire:decode_parameters(Bin, Codec),
    Sock2 = epgsql_sock:notify(Sock, {types, Types}),
    {noaction, Sock2, State#desc_stmt{parameter_descr = Types}};
handle_message(?ROW_DESCRIPTION, <<Count:?int16, Bin/binary>>, Sock,
               #desc_stmt{name = Name, parameter_descr = Params}) ->
    Codec = epgsql_sock:get_codec(Sock),
    Columns = epgsql_wire:decode_columns(Count, Bin, Codec),
    Columns2 = [Col#column{format = epgsql_wire:format(Col#column.type, Codec)}
                || Col <- Columns],
    Result = {ok, #statement{name = Name, types = Params, columns = Columns2}},
    {finish, Result, {columns, Columns2}, Sock};
handle_message(?NO_DATA, <<>>, Sock, #desc_stmt{name = Name, parameter_descr = Params}) ->
    Result = {ok, #statement{name = Name, types = Params, columns = []}},
    {finish, Result, no_data, Sock};
handle_message(?ERROR, Error, _Sock, _State) ->
    Result = {error, Error},
    {sync_required, Result};
handle_message(_, _, _, _) ->
    unknown.
