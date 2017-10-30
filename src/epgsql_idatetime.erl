%%% Copyright (C) 2008 - Will Glozer.  All rights reserved.

-module(epgsql_idatetime).

-export([decode/2, encode/2]).
-export([j2date/1, date2j/1]).

-include("protocol.hrl").

-define(postgres_epoc_jdate, 2451545).
-define(postgres_epoc_usecs, 946684800000000).

-define(mins_per_hour, 60).
-define(secs_per_minute, 60).

-define(usecs_per_day, 86400000000).
-define(usecs_per_hour, 3600000000).
-define(usecs_per_minute, 60000000).
-define(usecs_per_sec, 1000000).

decode(date, <<J:?int32>>)                         -> j2date(?postgres_epoc_jdate + J);
decode(time, <<N:?int64>>)                         -> i2time(N);
decode(timetz, <<N:?int64, TZ:?int32>>)            -> {i2time(N), TZ};
decode(timestamp, <<N:?int64>>)                    -> i2timestamp(N);
decode(timestamptz, <<N:?int64>>)                  -> i2timestamp(N);
decode(interval, <<N:?int64, D:?int32, M:?int32>>) -> {i2time(N), D, M}.

encode(date, D)         -> <<(date2j(D) - ?postgres_epoc_jdate):?int32>>;
encode(time, T)         -> <<(time2i(T)):?int64>>;
encode(timetz, {T, TZ}) -> <<(time2i(T)):?int64, TZ:?int32>>;
encode(timestamp, TS = {_, _, _})   -> <<(now2i(TS)):?int64>>;
encode(timestamp, TS)   -> <<(timestamp2i(TS)):?int64>>;
encode(timestamptz, TS = {_, _, _})   -> <<(now2i(TS)):?int64>>;
encode(timestamptz, TS) -> <<(timestamp2i(TS)):?int64>>;
encode(interval, {T, D, M}) -> <<(time2i(T)):?int64, D:?int32, M:?int32>>.

%% Julian calendar
%% See $PG$/src/backend/utils/adt/datetime.c
j2date(N) ->
    J = N + 32044,
    Q1 = J div 146097,
    Extra = (J - Q1 * 146097) * 4 + 3,
    J2 = J + 60 + Q1 * 3 + Extra div 146097,
    Q2 = J2 div 1461,
    J3 = J2 - Q2 * 1461,
    Y = J3 * 4 div 1461,
    J4 = case Y of
        0 -> ((J3 + 306) rem 366) + 123;
        _ -> ((J3 + 305) rem 365) + 123
    end,
    Year = (Y + Q2 * 4) - 4800,
    Q3 = J4 * 2141 div 65536,
    Day = J4 - 7834 * Q3 div 256,
    Month = (Q3 + 10) rem 12 + 1,
    {Year, Month, Day}.

date2j({Y, M, D}) ->
    M2 = case M > 2 of
        true ->
            M + 1;
        false ->
            M + 13
    end,
    Y2 = case M > 2 of
        true ->
            Y + 4800;
        false ->
            Y + 4799
    end,
    C = Y2 div 100,
    J1 = Y2 * 365 - 32167,
    J2 = J1 + (Y2 div 4 - C + C div 4),
    J2 + 7834 * M2 div 256 + D.

i2time(N) ->
    Hour = N div ?usecs_per_hour,
    R1 = N - Hour * ?usecs_per_hour,
    Min = R1 div ?usecs_per_minute,
    R2 = R1 - Min * ?usecs_per_minute,
    Sec = R2 div ?usecs_per_sec,
    US = R2 - Sec * ?usecs_per_sec,
    {Hour, Min, Sec + US / ?usecs_per_sec}.

time2i({H, M, S}) ->
    US = trunc(round(S * ?usecs_per_sec)),
    ((H * ?mins_per_hour + M) * ?secs_per_minute) * ?usecs_per_sec + US.

i2timestamp(N) ->
    case tmodulo(N, ?usecs_per_day) of
        {T, D} when T < 0 -> i2timestamp2(D - 1 + ?postgres_epoc_jdate, T + ?usecs_per_day);
        {T, D}            -> i2timestamp2(D + ?postgres_epoc_jdate, T)
    end.

i2timestamp2(D, T) ->
    {j2date(D), i2time(T)}.

timestamp2i({Date, Time}) ->
    D = date2j(Date) - ?postgres_epoc_jdate,
    D * ?usecs_per_day + time2i(Time).

now2i({MegaSecs, Secs, MicroSecs}) ->
    (MegaSecs * 1000000 + Secs) * 1000000 + MicroSecs - ?postgres_epoc_usecs.

tmodulo(T, U) ->
    case T div U of
        0 -> {T, 0};
        Q -> {T - (Q * U), Q}
    end.
