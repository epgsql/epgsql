%% @doc Asks server to provide input parameter and result rows information.
%%
%% Almost the same as {@link epgsql_cmd_parse}.
%%
%% ```
%% > Describe(STATEMENT)
%% < ParameterDescription
%% < RowDescription | NoData
%% '''
-module(epgsql_cmd_describe_statement).
-behaviour(epgsql_command).
-export([init/1, execute/2, handle_message/4]).
-export_type([response/0]).

-include("epgsql.hrl").
-include("protocol.hrl").

-type response() :: {ok, #statement{}} | {error, epgsql:query_error()}.

-record(desc_stmt,
        {name :: iodata(),
         parameter_typenames = [],
         parameter_descr = []}).

init(Name) ->
    #desc_stmt{name = Name}.

execute(Sock, #desc_stmt{name = Name} = St) ->
    Commands =
      [
       epgsql_wire:encode_describe(statement, Name),
       epgsql_wire:encode_flush()
      ],
    {send_multi, Commands, Sock, St}.

handle_message(?PARAMETER_DESCRIPTION, Bin, Sock, State) ->
    Codec = epgsql_sock:get_codec(Sock),
    TypeInfos = epgsql_wire:decode_parameters(Bin, Codec),
    OidInfos = [epgsql_binary:typeinfo_to_oid_info(Type, Codec) || Type <- TypeInfos],
    TypeNames = [epgsql_binary:typeinfo_to_name_array(Type, Codec) || Type <- TypeInfos],
    Sock2 = epgsql_sock:notify(Sock, {types, TypeNames}),
    {noaction, Sock2, State#desc_stmt{parameter_descr = OidInfos,
                                      parameter_typenames = TypeNames}};
handle_message(?ROW_DESCRIPTION, <<Count:?int16, Bin/binary>>, Sock,
               #desc_stmt{name = Name, parameter_descr = Params,
                          parameter_typenames = TypeNames}) ->
    Codec = epgsql_sock:get_codec(Sock),
    Columns = epgsql_wire:decode_columns(Count, Bin, Codec),
    Columns2 = [Col#column{format = epgsql_wire:format(Col, Codec)}
                || Col <- Columns],
    Result = {ok, #statement{name = Name,
                             types = TypeNames,
                             parameter_info = Params,
                             columns = Columns2}},
    {finish, Result, {columns, Columns2}, Sock};
handle_message(?NO_DATA, <<>>, Sock, #desc_stmt{name = Name, parameter_descr = Params,
                                                parameter_typenames = TypeNames}) ->
    Result = {ok, #statement{name = Name,
                             types = TypeNames,
                             parameter_info = Params,
                             columns = []}},
    {finish, Result, no_data, Sock};
handle_message(?ERROR, Error, _Sock, _State) ->
    Result = {error, Error},
    {sync_required, Result};
handle_message(_, _, _, _) ->
    unknown.
