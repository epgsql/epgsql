%% @doc Executes a portal.
%%
%% It's possible to tell the server to only return limited number of rows by
%% providing non-zero `MaxRows' parameter.
%% ```
%% > Execute
%% < DataRow*
%% < CommandComplete | PortalSuspended
%% '''
%% @see epgsql_cmd_parse
%% @see epgsql_cmd_bind
-module(epgsql_cmd_execute).
-behaviour(epgsql_command).
-export([init/1, execute/2, handle_message/4]).
-export_type([response/0]).

-type response() :: {ok, Count :: non_neg_integer(), Rows :: [tuple()]}
                  | {ok, Count :: non_neg_integer()}
                  | {ok | partial, Rows :: [tuple()]}
                  | {error, epgsql:query_error()}.

-include("epgsql.hrl").
-include("protocol.hrl").

-record(execute,
        {stmt :: #statement{},
         portal_name :: iodata(),
         max_rows :: non_neg_integer(),
         decoder}).

init({Stmt, PortalName, MaxRows}) ->
    #execute{stmt = Stmt, portal_name = PortalName, max_rows = MaxRows}.

execute(Sock, #execute{stmt = Stmt, portal_name = PortalName, max_rows = MaxRows} = State) ->
    #statement{columns = Columns} = Stmt,
    Codec = epgsql_sock:get_codec(Sock),
    Decoder = epgsql_wire:build_decoder(Columns, Codec),
    Commands =
      [
       epgsql_wire:encode_execute(PortalName, MaxRows),
       epgsql_wire:encode_flush()
      ],
    {send_multi, Commands, Sock, State#execute{decoder = Decoder}}.

handle_message(?DATA_ROW, <<_Count:?int16, Bin/binary>>, Sock,
               #execute{decoder = Decoder} = St) ->
    Row = epgsql_wire:decode_data(Bin, Decoder),
    {add_row, Row, Sock, St};
handle_message(?EMPTY_QUERY, _, Sock, _State) ->
    {finish, {ok, [], []}, {complete, empty}, Sock};
handle_message(?COMMAND_COMPLETE, Bin, Sock,
               #execute{stmt = #statement{columns = Cols}}) ->
    Complete = epgsql_wire:decode_complete(Bin),
    Rows = epgsql_sock:get_rows(Sock),
    Result = case Complete of
                 {_, Count} when Cols == [] ->
                     {ok, Count};
                 {_, Count} ->
                     {ok, Count, Rows};
                 _ ->
                     {ok, Rows}
             end,
    {finish, Result, {complete, Complete}, Sock};
handle_message(?PORTAL_SUSPENDED, <<>>, Sock, _State) ->
    Rows = epgsql_sock:get_rows(Sock),
    {finish, {partial, Rows}, suspended, Sock};
handle_message(?ERROR, Error, _Sock, _State) ->
    {sync_required, {error, Error}};
handle_message(_, _, _, _) ->
    unknown.
