%%% @doc
%%% Behaviour for postgresql datatype codecs.
%%%
%%% XXX: this module and callbacks "know nothing" about OIDs.
%%% XXX: state of codec shouldn't leave epgsql_sock process. If you need to
%%% return "pointer" to data type/codec, it's better to return OID or type name.
%%% @end
%%% @see epgsql_binary
%%% @end
%%% Created : 12 Oct 2017 by Sergey Prokhorov <me@seriyps.ru>

-module(epgsql_codec).
-export([init_mods/2, encode/4, decode/4, decode_text/4]).

-export_type([codec_state/0, codec_mod/0, codec_entry/0]).

%%
%% Behaviour
%%
-type codec_state() :: any().
-type codec_mod() :: module().

-optional_callbacks([decode_text/3]).

%% Called on connection start-up
-callback init(any(), epgsql_sock:pg_sock()) -> codec_state().

%% List of supported type names
-callback names() -> [epgsql:type_name()].

%% Encode Erlang representation to PG binary
%% Called for each parameter, binary protocol (equery)
-callback encode(Cell :: any(), epgsql:type_name(), codec_state()) -> iodata().

%% Decode PG binary to erlang representation
%% Called for each cell in each row, binary protocol (equery)
-callback decode(Cell :: binary(), epgsql:type_name(), codec_state()) -> any().

%% Decode PG string representation (text protocol) to erlang term.
%% Called for each cell in each row, text protocol (squery)
-callback decode_text(Cell :: binary(), epgsql:type_name(), codec_state()) ->
    any().

%% ==========
-type codec_entry() :: {epgsql:type_name(),
                        Mod :: codec_mod(),
                        CallbackState :: any()}.

-spec init_mods([{codec_mod(), any()}], epgsql_sock:pg_sock()) ->
                       ordsets:ordset(codec_entry()).
init_mods(Codecs, PgSock) ->
    ModState = [{Mod, Mod:init(Opts, PgSock)} || {Mod, Opts} <- Codecs],
    build_mapping(ModState, sets:new(), []).

build_mapping([{Mod, _State} = MS | ModStates], Set, Acc) ->
    Names = Mod:names(),
    {Set1, Acc1} = add_names(Names, MS, Set, Acc),
    build_mapping(ModStates, Set1, Acc1);
build_mapping([], _, Acc) ->
    ordsets:from_list(Acc).

add_names([Name | Names], {Mod, State} = MS, Set, Acc) ->
    case sets:is_element(Name, Set) of
        true ->
            add_names(Names, MS, Set, Acc);
        false ->
            Set1 = sets:add_element(Name, Set),
            Acc1 = [{Name, Mod, State} | Acc],
            add_names(Names, MS, Set1, Acc1)
    end;
add_names([], _, Set, Acc) ->
    {Set, Acc}.

-spec encode(codec_mod(), any(), epgsql:type_name(), codec_state()) -> iodata().
encode(Mod, Cell, TypeName, CodecState) ->
    Mod:encode(Cell, TypeName, CodecState).

-spec decode(codec_mod(), binary(), epgsql:type_name(), codec_state()) -> any().
decode(Mod, Cell, TypeName, CodecState) ->
    Mod:decode(Cell, TypeName, CodecState).

-spec decode_text(codec_mod(), binary(), epgsql:type_name(), codec_state()) -> any().
decode_text(Mod, Cell, TypeName, CodecState) ->
    Mod:decode(Cell, TypeName, CodecState).
