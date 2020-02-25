%% @doc Closes statement / portal
%%
%% ```
%% > Close
%% < CloseComplete
%% '''
-module(epgsql_cmd_close).
-behaviour(epgsql_command).
-export([init/1, execute/2, handle_message/4]).
-export_type([response/0]).

-type response() :: ok | {error, epgsql:query_error()}.

-include("epgsql.hrl").
-include("protocol.hrl").

-record(close,
        {type :: statement | portal,
         name :: iodata()}).

init({Type, Name}) ->
    #close{type = Type, name = Name}.

execute(Sock, #close{type = Type, name = Name} = St) ->
    Type2 = case Type of
        statement -> ?PREPARED_STATEMENT;
        portal    -> ?PORTAL
    end,
    epgsql_sock:send_multi(
      Sock,
      [
       {?CLOSE, [Type2, Name, 0]},
       {?FLUSH, []}
      ]),
    {ok, Sock, St}.

handle_message(?CLOSE_COMPLETE, <<>>, Sock, _St) ->
    {finish, ok, ok, Sock};
handle_message(?ERROR, Error, _Sock, _State) ->
    {sync_required, {error, Error}};
handle_message(_, _, _, _) ->
    unknown.
