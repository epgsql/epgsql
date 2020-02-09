%%% @doc
%%% Codec for `time', `timetz', `date', `timestamp', `timestamptz', `interval'
%%%
%%% It supports both integer and float datetime representations (see
%%% [https://www.postgresql.org/docs/current/runtime-config-preset.html#GUC-INTEGER-DATETIMES]).
%%% But float representation support might be eventually removed.
%%%
%%% <ul>
%%%  <li>[https://www.postgresql.org/docs/current/static/datatype-datetime.html]</li>
%%%  <li>$PG$/src/backend/utils/adt/timestamp.c // `timestamp', `timestamptz', `interval'</li>
%%%  <li>$PG$/src/backend/utils/adt/datetime.c // helpers</li>
%%%  <li>$PG$/src/backend/utils/adt/date.c // `time', `timetz', `date'</li>
%%% </ul>
%%% @end
%%% Created : 12 Oct 2017 by Sergey Prokhorov <me@seriyps.ru>

-module(epgsql_codec_datetime).
-behaviour(epgsql_codec).

-export([init/2, names/0, encode/3, decode/3, decode_text/3]).

-export_type([data/0]).
-export_type([pg_date/0,
              pg_time/0,
              pg_datetime/0,
              pg_interval/0,
              pg_timetz/0]).

-type data() :: pg_date() | pg_time() | pg_datetime() | pg_interval() | pg_timetz().

%% Ranges are from https://www.postgresql.org/docs/current/static/datatype-datetime.html
-type pg_date() ::
        {Year :: -4712..294276,
         Month :: 1..12,
         Day :: 1..31}.
-type pg_time() ::
        {Hour :: 0..24,  % Max value is 24:00:00
         Minute :: 0..59,
         Second :: 0..59 | float()}.
-type pg_timetz() :: {pg_time(), UtcOffset :: integer()}.
-type pg_datetime() :: {pg_date(), pg_time()}.
-type pg_interval() :: {pg_time(), Days :: integer(), Months :: integer()}.


init(_, Sock) ->
    case epgsql_sock:get_parameter_internal(<<"integer_datetimes">>, Sock) of
        <<"on">>  -> epgsql_idatetime;
        <<"off">> -> epgsql_fdatetime
    end.

names() ->
    [time, timetz, date, timestamp, timestamptz, interval].

%% FIXME: move common logic out from fdatetime/idatetime; make them more
%% low-level
encode(Val, Type, epgsql_idatetime) ->
    epgsql_idatetime:encode(Type, Val);
encode(Val, Type, epgsql_fdatetime) ->
    epgsql_fdatetime:encode(Type, Val).

decode(Bin, Type, epgsql_idatetime) ->
    epgsql_idatetime:decode(Type, Bin);
decode(Bin, Type, epgsql_fdatetime) ->
    epgsql_fdatetime:decode(Type, Bin).

decode_text(V, _, _) -> V.
