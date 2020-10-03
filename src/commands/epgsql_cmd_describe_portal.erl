%% @doc Asks the server to provide description of portal's results columns
%%
%% ```
%% > Describe(PORTAL)
%% < RowDescription | NoData
%% '''
-module(epgsql_cmd_describe_portal).
-behaviour(epgsql_command).
-export([init/1, execute/2, handle_message/4]).
-export_type([response/0]).

-include("epgsql.hrl").
-include("protocol.hrl").

-type response() :: {ok, [epgsql:column()]} | {error, epgsql:query_error()}.

-record(desc_portal,
        {name :: iodata(),
         parameter_descr}).

init(Name) ->
    #desc_portal{name = Name}.

execute(Sock, #desc_portal{name = Name} = St) ->
    Commands =
      [
       epgsql_wire:encode_describe(portal, Name),
       epgsql_wire:encode_flush()
      ],
    {send_multi, Commands, Sock, St}.

handle_message(?ROW_DESCRIPTION, <<Count:?int16, Bin/binary>>, Sock, _St) ->
    Codec = epgsql_sock:get_codec(Sock),
    Columns = epgsql_wire:decode_columns(Count, Bin, Codec),
    {finish, {ok, Columns}, {columns, Columns}, Sock};
handle_message(?NO_DATA, <<>>, Sock, _State) ->
    {finish, {ok, []}, no_data, Sock};
handle_message(?ERROR, Error, _Sock, _State) ->
    Result = {error, Error},
    {sync_required, Result};
handle_message(_, _, _, _) ->
    unknown.
