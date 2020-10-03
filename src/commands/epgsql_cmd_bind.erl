%% @doc Binds placeholder parameters to prepared statement, creating a "portal"
%%
%% ```
%% > Bind
%% < BindComplete
%% '''
%% @see epgsql_cmd_parse
%% @see epgsql_cmd_execute
-module(epgsql_cmd_bind).
-behaviour(epgsql_command).
-export([init/1, execute/2, handle_message/4]).
-export_type([response/0]).

-type response() :: ok | {error, epgsql:query_error()}.

-include("epgsql.hrl").
-include("protocol.hrl").

-record(bind,
        {stmt :: #statement{},
         portal :: iodata(),
         params :: list()}).

init({Stmt, PortalName, Params}) ->
    #bind{stmt = Stmt, portal = PortalName, params = Params}.

execute(Sock, #bind{stmt = Stmt, portal = PortalName, params = Params} = St) ->
    #statement{name = StatementName, columns = Columns, types = Types} = Stmt,
    Codec = epgsql_sock:get_codec(Sock),
    TypedParams = lists:zip(Types, Params),
    Bin1 = epgsql_wire:encode_parameters(TypedParams, Codec),
    Bin2 = epgsql_wire:encode_formats(Columns),
    Commands = [
       epgsql_wire:encode_bind(PortalName, StatementName, Bin1, Bin2),
       epgsql_wire:encode_flush()
      ],
    {send_multi, Commands, Sock, St}.

handle_message(?BIND_COMPLETE, <<>>, Sock, _State) ->
    {finish, ok, ok, Sock};
handle_message(?ERROR, Error, _Sock, _State) ->
    {sync_required, {error, Error}};
handle_message(_, _, _, _) ->
    unknown.
