%% @doc Special command. Executes Squery over pg_type table and updates codecs.
-module(epgsql_cmd_update_type_cache).
-behaviour(epgsql_command).
-export([init/1, execute/2, handle_message/4]).
-export_type([response/0]).

-type response() ::
        {ok, [epgsql:type_name()]}
      | {error, epgsql:query_error()}.

-include("protocol.hrl").

-record(upd,
        {codecs :: [{epgsql_codec:codec_mod(), Opts :: any()}],
         codec_entries :: [epgsql_codec:codec_entry()] | undefined,
         decoder :: epgsql_wire:row_decoder() | undefined}).

init(Codecs) ->
    #upd{codecs = Codecs}.

execute(Sock, #upd{codecs = Codecs} = State) ->
    CodecEntries = epgsql_codec:init_mods(Codecs, Sock),
    TypeNames = [element(1, Entry) || Entry <- CodecEntries],
    Query = epgsql_oid_db:build_query(TypeNames),
    {PktType, PktData} = epgsql_wire:encode_query(Query),
    {send, PktType, PktData, Sock, State#upd{codec_entries = CodecEntries}}.

handle_message(?ROW_DESCRIPTION, <<Count:?int16, Bin/binary>>, Sock, State) ->
    Codec = epgsql_sock:get_codec(Sock),
    Columns = epgsql_wire:decode_columns(Count, Bin, Codec),
    Decoder = epgsql_wire:build_decoder(Columns, Codec),
    {noaction, Sock, State#upd{decoder = Decoder}};
handle_message(?DATA_ROW, <<_Count:?int16, Bin/binary>>,
               Sock, #upd{decoder = Decoder} = St) ->
    Row = epgsql_wire:decode_data(Bin, Decoder),
    {add_row, Row, Sock, St};
handle_message(?COMMAND_COMPLETE, Bin, Sock, St) ->
    Complete = epgsql_wire:decode_complete(Bin),
    Rows = epgsql_sock:get_rows(Sock),
    {add_result, Rows, {complete, Complete}, Sock, St};
handle_message(?READY_FOR_QUERY, _Status, Sock, State) ->
    [Result] = epgsql_sock:get_results(Sock),
    handle_result(Result, Sock, State);
handle_message(?ERROR, Error, Sock, St) ->
    Result = {error, Error},
    {add_result, Result, Result, Sock, St};
handle_message(_, _, _, _) ->
    unknown.

handle_result({error, _} = Err, Sock, _State) ->
    {finish, Err, done, Sock};
handle_result(Rows, Sock, #upd{codec_entries = CodecEntries} = _State) ->
    OidEntries = epgsql_oid_db:parse_rows(Rows),
    Types = epgsql_oid_db:join_codecs_oids(OidEntries, CodecEntries),

    Codec = epgsql_sock:get_codec(Sock),
    Codec1 = epgsql_binary:update_codec(Types, Codec),
    Sock1 = epgsql_sock:set_attr(codec, Codec1, Sock),

    TypeNames = [element(1, Entry) || Entry <- CodecEntries],
    {finish, {ok, TypeNames}, done, Sock1}.
