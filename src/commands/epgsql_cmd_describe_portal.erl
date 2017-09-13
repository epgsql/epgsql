%% > Describe
%% < RowDescription | NoData
-module(epgsql_cmd_describe_portal).
-behaviour(epgsql_command).
-export([init/1, execute/2, handle_message/4]).
-export_type([response/0]).

-include("epgsql.hrl").
-include("../epgsql_binary.hrl").

-type response() :: {ok, [epgsql:column()]} | {error, epgsql:query_error()}.

-record(desc_portal,
        {name :: iodata(),
         parameter_descr}).

init(Name) ->
    #desc_portal{name = Name}.

execute(Sock, #desc_portal{name = Name} = St) ->
    epgsql_sock:send_multi(
      Sock,
      [
       {?DESCRIBE, [?PORTAL, Name, 0]},
       {?FLUSH, []}
      ]),
    {ok, Sock, St}.

handle_message(?ROW_DESCRIPTION, <<Count:?int16, Bin/binary>>, Sock, St) ->
    Codec = epgsql_sock:get_codec(Sock),
    Columns = epgsql_wire:decode_columns(Count, Bin, Codec),
    {finish, {ok, Columns}, {columns, Columns}, St};
handle_message(?NO_DATA, <<>>, _Sock, _State) ->
    {finish, {ok, []}, no_data};
handle_message(?ERROR, Error, _Sock, _State) ->
    Result = {error, Error},
    {sync_required, Result};
handle_message(_, _, _, _) ->
    unknown.
