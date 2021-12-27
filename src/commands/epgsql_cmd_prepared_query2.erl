%% @doc Almost the same as equery, but don't execute 'CLOSE'
%%
%% So, statement can be reused multiple times.
%% ```
%% > Bind
%% < BindComplete
%% > Execute
%% < DataRow*
%% < CommandComplete
%% > Sync
%% < ReadyForQuery
%% '''
-module(epgsql_cmd_prepared_query2).
-behaviour(epgsql_command).
-export([init/1, execute/2, handle_message/4]).
-export_type([response/0]).

-include("epgsql.hrl").
-include("protocol.hrl").

-type response() :: {ok, Count :: non_neg_integer(), Cols :: [epgsql:column()], Rows :: [tuple()]}
                  | {ok, Count :: non_neg_integer()}
                  | {ok, Cols :: [epgsql:column()], Rows :: [tuple()]}
                  | {ok, #statement{}}
                  | {error, epgsql:query_error()}.

-record(pquery2,
        {name :: iolist(),
         sql :: iodata(),
         params :: list(),
         types :: [atom()],
         parameter_typenames = [] :: [epgsql:type_name() | {array, epgsql:type_name()}],
         parameter_descr = [] :: [epgsql_oid_db:oid_info()],
         stmt = undefined :: #statement{} | undefined,
         decoder}).

init({Name, SQL, Parameters, Types}) ->
    #pquery2{name = Name, sql = SQL, params = Parameters, types = Types}.

execute(Sock, #pquery2{name = Name, sql = SQL, params = Params} = State) ->
    case maps:get(Name, epgsql_sock:get_stmts(Sock), undefined) of
        undefined ->
            Codec = epgsql_sock:get_codec(Sock),
            Bin = epgsql_wire:encode_types(State#pquery2.types, Codec),
            Commands =
                [
                    epgsql_wire:encode_parse(Name, SQL, Bin),
                    epgsql_wire:encode_describe(statement, Name),
                    epgsql_wire:encode_flush()
                ],
            {send_multi, Commands, Sock, State};
        #statement{types = Types} = Stmt ->
            TypedParams = lists:zip(Types, Params),
            #statement{name = StatementName, columns = Columns} = Stmt,
            Codec = epgsql_sock:get_codec(Sock),
            Bin1 = epgsql_wire:encode_parameters(TypedParams, Codec),
            Bin2 = epgsql_wire:encode_formats(Columns),
            Commands =
                [
                    epgsql_wire:encode_bind("", StatementName, Bin1, Bin2),
                    epgsql_wire:encode_execute("", 0),
                    epgsql_wire:encode_sync()
                ],
            {send_multi, Commands, Sock, State#pquery2{stmt = Stmt}}
    end.

%% parse
handle_message(?PARSE_COMPLETE, <<>>, Sock, _State) ->
    {noaction, Sock};
handle_message(?PARAMETER_DESCRIPTION, Bin, Sock, State) ->
    Codec = epgsql_sock:get_codec(Sock),
    TypeInfos = epgsql_wire:decode_parameters(Bin, Codec),
    OidInfos = [epgsql_binary:typeinfo_to_oid_info(Type, Codec) || Type <- TypeInfos],
    TypeNames = [epgsql_binary:typeinfo_to_name_array(Type, Codec) || Type <- TypeInfos],
    Sock2 = epgsql_sock:notify(Sock, {types, TypeNames}),
    {noaction, Sock2, State#pquery2{parameter_descr = OidInfos,
                                    parameter_typenames = TypeNames}};
handle_message(?ROW_DESCRIPTION, <<Count:?int16, Bin/binary>>, Sock,
               #pquery2{name = Name, parameter_descr = Params,
                        parameter_typenames = TypeNames} = State) ->
    Codec = epgsql_sock:get_codec(Sock),
    Columns = epgsql_wire:decode_columns(Count, Bin, Codec),
    Columns2 = [Col#column{format = epgsql_wire:format(Col, Codec)}
                || Col <- Columns],
    Stmt = #statement{name = Name,
                      types = TypeNames,
                      columns = Columns2,
                      parameter_info = Params},
    NSock = store_statment(Stmt, Sock),    
    {requeue, NSock, State};
handle_message(?NO_DATA, <<>>, Sock, #pquery2{name = Name, parameter_descr = Params,
                                              parameter_typenames = TypeNames} = State) ->
    Stmt = #statement{name = Name,
                      types = TypeNames,
                      columns = [],
                      parameter_info = Params},
    NSock = store_statment(Stmt, Sock),
    {requeue, NSock, State};
handle_message(?ERROR, Error, _Sock, #pquery2{stmt = undefined}) ->
    Result = {error, Error},
    {sync_required, Result};
%% prepared query
handle_message(?BIND_COMPLETE, <<>>, Sock, #pquery2{stmt = Stmt} = State) ->
    #statement{columns = Columns} = Stmt,
    epgsql_sock:notify(Sock, {columns, Columns}), % Why do we need this?
    Codec = epgsql_sock:get_codec(Sock),
    Decoder = epgsql_wire:build_decoder(Columns, Codec),
    {noaction, Sock, State#pquery2{decoder = Decoder}};
handle_message(?DATA_ROW, <<_Count:?int16, Bin/binary>>,
               Sock, #pquery2{decoder = Decoder} = State) ->
    Row = epgsql_wire:decode_data(Bin, Decoder),
    {add_row, Row, Sock, State};
handle_message(?EMPTY_QUERY, _, Sock, State) ->
    {add_result, {ok, [], []}, {complete, empty}, Sock, State};
handle_message(?COMMAND_COMPLETE, Bin, Sock, #pquery2{stmt = Stmt} = State) ->
    Complete = epgsql_wire:decode_complete(Bin),
    #statement{columns = Cols} = Stmt,
    Rows = epgsql_sock:get_rows(Sock),
    Result = case Complete of
                 {_, Count} when Cols == [] ->
                     {ok, Count};
                 {_, Count} ->
                     {ok, Count, Cols, Rows};
                 _ ->
                     {ok, Cols, Rows}
             end,
    {add_result, Result, {complete, Complete}, Sock, State};
handle_message(?READY_FOR_QUERY, _Status, Sock, _State) ->
    case epgsql_sock:get_results(Sock) of
        [Result] ->
            {finish, Result, done, Sock};
        [] ->
            {finish, done, done, Sock}
    end;
handle_message(?ERROR, Error, Sock, State) ->
    Result = {error, Error},
    {add_result, Result, Result, Sock, State};
handle_message(_, _, _, _) ->
    unknown.

store_statment(Stmt, Sock) ->
    Stmts1 = maps:put(Stmt#statement.name, Stmt, epgsql_sock:get_stmts(Sock)),
    epgsql_sock:set_stmts(Stmts1, Sock).
    
