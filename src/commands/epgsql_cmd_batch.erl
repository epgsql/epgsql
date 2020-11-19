%% @doc Execute multiple extended queries in a single network round-trip
%%
%% There are 2 kinds of interface:
%% <ol>
%%  <li>To execute multiple queries, each with it's own `statement()'</li>
%%  <li>To execute multiple queries, but by binding different parameters to the
%%  same `statement()'</li>
%% </ol>
%% ```
%% > {Bind
%% <  BindComplete
%% >  Execute
%% <  DataRow*
%% <  CommandComplete}*
%% > Sync
%% < ReadyForQuery
%% '''
-module(epgsql_cmd_batch).
-behaviour(epgsql_command).
-export([init/1, execute/2, handle_message/4]).
-export_type([arguments/0, response/0]).


-include("epgsql.hrl").
-include("protocol.hrl").

-record(batch,
        {batch :: [ [epgsql:bind_param()] ] | [{#statement{}, [epgsql:bind_param()]}],
         statement :: #statement{} | undefined,
         decoder :: epgsql_wire:row_decoder() | undefined}).

-type arguments() ::
        {epgsql:statement(), [ [epgsql:bind_param()] ]} |
        [{epgsql:statement(), [epgsql:bind_param()]}].

-type response() :: [{ok, Count :: non_neg_integer(), Rows :: [tuple()]}
                     | {ok, Count :: non_neg_integer()}
                     | {ok, Rows :: [tuple()]}
                     | {error, epgsql:query_error()}
                     ].
-type state() :: #batch{}.

-spec init(arguments()) -> state().
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
                  BinFormats = epgsql_wire:encode_formats(Columns),
                  add_command(StatementName, Types, Parameters, BinFormats, Codec, Acc)
          end,
          [epgsql_wire:encode_sync()],
          Batch),
    {send_multi, Commands, Sock, State};
execute(Sock, #batch{batch = Batch,
                     statement = #statement{name = StatementName,
                                            columns = Columns,
                                            types = Types}} = State) ->
    Codec = epgsql_sock:get_codec(Sock),
    BinFormats = epgsql_wire:encode_formats(Columns),
    %% TODO: build some kind of encoder and reuse it for each batch item
    Commands =
        lists:foldr(
          fun(Parameters, Acc) ->
                  add_command(StatementName, Types, Parameters, BinFormats, Codec, Acc)
          end,
          [epgsql_wire:encode_sync()],
          Batch),
    {send_multi, Commands, Sock, State}.

add_command(StmtName, Types, Params, BinFormats, Codec, Acc) ->
    TypedParameters = lists:zip(Types, Params),
    BinParams = epgsql_wire:encode_parameters(TypedParameters, Codec),
    [epgsql_wire:encode_bind("", StmtName, BinParams, BinFormats),
     epgsql_wire:encode_execute("", 0) | Acc].

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
handle_message(?READY_FOR_QUERY, _Status, Sock, _State) ->
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
