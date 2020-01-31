%% > {Bind
%% <  BindComplete
%% >  Execute
%% <  DataRow*
%% <  CommandComplete}*
%% > Sync
%% < ReadyForQuery
-module(epgsql_cmd_batch).
-behaviour(epgsql_command).
-export([init/1, execute/2, handle_message/4]).
-export_type([arguments/0, response/0]).

-type arguments() ::
        {epgsql:statement(), [ [epgsql:bind_param()] ]} |
        [{epgsql:statement(), [epgsql:bind_param()]}].

-type response() :: [{ok, Count :: non_neg_integer(), Rows :: [tuple()]}
                     | {ok, Count :: non_neg_integer()}
                     | {ok, Rows :: [tuple()]}
                     | {error, epgsql:query_error()}].

-include("epgsql.hrl").
-include("protocol.hrl").

-record(batch,
        {batch :: [ [epgsql:bind_param()] ] | [{#statement{}, [epgsql:bind_param()]}],
         statement :: #statement{} | undefined,
         decoder :: epgsql_wire:row_decoder() | undefined}).

-spec init(arguments()) -> #batch{}.
init({#statement{} = Statement, Batch}) ->
    #batch{statement = Statement,
           batch = Batch};
init(Batch) when is_list(Batch) ->
    #batch{batch = Batch}.

execute(Sock, #batch{batch = Batch, statement = undefined} = State) ->
    Codec = epgsql_sock:get_codec(Sock),
    Commands =
        lists:foldr(
          fun({Statement, Parameters}, Acc) ->
                  #statement{name = StatementName,
                             columns = Columns,
                             types = Types} = Statement,
                  TypedParameters = lists:zip(Types, Parameters),
                  Bin1 = epgsql_wire:encode_parameters(TypedParameters, Codec),
                  Bin2 = epgsql_wire:encode_formats(Columns),
                  [{?BIND, [0, StatementName, 0, Bin1, Bin2]},
                   {?EXECUTE, [0, <<0:?int32>>]} | Acc]
          end,
          [{?SYNC, []}],
          Batch),
    epgsql_sock:send_multi(Sock, Commands),
    {ok, Sock, State};
execute(Sock, #batch{batch = Batch,
                     statement = #statement{name = StatementName,
                                            columns = Columns,
                                            types = Types}} = State) ->
    Codec = epgsql_sock:get_codec(Sock),
    BinFormats = epgsql_wire:encode_formats(Columns),
    Commands =
        lists:foldr(
          fun(Parameters, Acc) ->
                  TypedParameters = lists:zip(Types, Parameters),
                  BinParams = epgsql_wire:encode_parameters(TypedParameters, Codec),
                  [{?BIND, [0, StatementName, 0, BinParams, BinFormats]},
                   {?EXECUTE, [0, <<0:?int32>>]} | Acc]
          end,
          [{?SYNC, []}],
          Batch),
    epgsql_sock:send_multi(Sock, Commands),
    {ok, Sock, State}.

handle_message(?BIND_COMPLETE, <<>>, Sock, State) ->
    Columns = current_cols(State),
    Codec = epgsql_sock:get_codec(Sock),
    Decoder = epgsql_wire:build_decoder(Columns, Codec),
    {noaction, Sock, State#batch{decoder = Decoder}};
handle_message(?DATA_ROW, <<_Count:?int16, Bin/binary>>, Sock,
               #batch{decoder = Decoder} = State) ->
    Row = epgsql_wire:decode_data(Bin, Decoder),
    {add_row, Row, Sock, State};
%% handle_message(?EMPTY_QUERY, _, Sock, _State) ->
%%     Sock1 = epgsql_sock:add_result(Sock, {complete, empty}, {ok, [], []}),
%%     {noaction, Sock1};
handle_message(?COMMAND_COMPLETE, Bin, Sock,
               #batch{batch = [_ | Batch]} = State) ->
    Columns = current_cols(State),
    Complete = epgsql_wire:decode_complete(Bin),
    Rows = epgsql_sock:get_rows(Sock),
    Result = case Complete of
                 {_, Count} when Columns == [] ->
                     {ok, Count};
                 {_, Count} ->
                     {ok, Count, Rows};
                 _ ->
                     {ok, Rows}
             end,
    {add_result, Result, {complete, Complete}, Sock, State#batch{batch = Batch}};
handle_message(?READY_FOR_QUERY, _Status, Sock, #batch{batch = B} = _State) when
      length(B) =< 1 ->
    Results = epgsql_sock:get_results(Sock),
    {finish, Results, done, Sock};
handle_message(?ERROR, Error, Sock, #batch{batch = [_ | Batch]} = State) ->
    Result = {error, Error},
    {add_result, Result, Result, Sock, State#batch{batch = Batch}};
handle_message(_, _, _, _) ->
    unknown.

%% Helpers

current_cols(Batch) ->
    #statement{columns = Columns} = current_stmt(Batch),
    Columns.

current_stmt(#batch{batch = [{Stmt, _} | _], statement = undefined}) ->
    Stmt;
current_stmt(#batch{statement = #statement{} = Stmt}) ->
    Stmt.
