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
-module(epgsql_cmd_prepared_query).
-behaviour(epgsql_command).
-export([init/1, execute/2, handle_message/4]).
-export_type([response/0]).

-type response() :: {ok, Count :: non_neg_integer(), Cols :: [epgsql:column()], Rows :: [tuple()]}
                  | {ok, Count :: non_neg_integer()}
                  | {ok, Cols :: [epgsql:column()], Rows :: [tuple()]}
                  | {error, epgsql:query_error()}.

-include("epgsql.hrl").
-include("protocol.hrl").

-record(pquery,
        {stmt :: #statement{},
         params :: list(),
         decoder}).

init({Stmt, TypedParams}) ->
    #pquery{stmt = Stmt,
            params = TypedParams}.

execute(Sock, #pquery{stmt = Stmt, params = TypedParams} = St) ->
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
    {send_multi, Commands, Sock, St}.

handle_message(?BIND_COMPLETE, <<>>, Sock, #pquery{stmt = Stmt} = State) ->
    #statement{columns = Columns} = Stmt,
    epgsql_sock:notify(Sock, {columns, Columns}), % Why do we need this?
    Codec = epgsql_sock:get_codec(Sock),
    Decoder = epgsql_wire:build_decoder(Columns, Codec),
    {noaction, Sock, State#pquery{decoder = Decoder}};
handle_message(?DATA_ROW, <<_Count:?int16, Bin/binary>>,
               Sock, #pquery{decoder = Decoder} = St) ->
    Row = epgsql_wire:decode_data(Bin, Decoder),
    {add_row, Row, Sock, St};
handle_message(?EMPTY_QUERY, _, Sock, St) ->
    {add_result, {ok, [], []}, {complete, empty}, Sock, St};
handle_message(?COMMAND_COMPLETE, Bin, Sock, #pquery{stmt = Stmt} = St) ->
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
    {add_result, Result, {complete, Complete}, Sock, St};
handle_message(?READY_FOR_QUERY, _Status, Sock, _State) ->
    case epgsql_sock:get_results(Sock) of
        [Result] ->
            {finish, Result, done, Sock};
        [] ->
            {finish, done, done, Sock}
    end;
handle_message(?ERROR, Error, Sock, St) ->
    Result = {error, Error},
    {add_result, Result, Result, Sock, St};
handle_message(_, _, _, _) ->
    unknown.
