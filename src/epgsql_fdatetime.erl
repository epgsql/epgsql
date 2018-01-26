%%% Copyright (C) 2008 - Will Glozer.  All rights reserved.

-module(epgsql_fdatetime).

-export([decode/2, encode/2]).

-include("protocol.hrl").

-define(POSTGRES_EPOC_JDATE, 2451545).
-define(POSTGRES_EPOC_SECS, 946684800).

-define(MINS_PER_HOUR, 60).
-define(SECS_PER_DAY, 86400.0).
-define(SECS_PER_HOUR, 3600.0).
-define(SECS_PER_MINUTE, 60.0).

decode(date, <<J:1/big-signed-unit:32>>)             -> epgsql_idatetime:j2date(?POSTGRES_EPOC_JDATE + J);
decode(time, <<N:1/big-float-unit:64>>)              -> f2time(N);
decode(timetz, <<N:1/big-float-unit:64, TZ:?int32>>) -> {f2time(N), TZ};
decode(timestamp, <<N:1/big-float-unit:64>>)         -> f2timestamp(N);
decode(timestamptz, <<N:1/big-float-unit:64>>)       -> f2timestamp(N);
decode(interval, <<N:1/big-float-unit:64, D:?int32, M:?int32>>) -> {f2time(N), D, M}.

encode(date, D)         -> <<(epgsql_idatetime:date2j(D) - ?POSTGRES_EPOC_JDATE):1/big-signed-unit:32>>;
encode(time, T)         -> <<(time2f(T)):1/big-float-unit:64>>;
encode(timetz, {T, TZ}) -> <<(time2f(T)):1/big-float-unit:64, TZ:?int32>>;
encode(timestamp, TS = {_, _, _})   -> <<(now2f(TS)):1/big-float-unit:64>>;
encode(timestamp, TS)   -> <<(timestamp2f(TS)):1/big-float-unit:64>>;
encode(timestamptz, TS = {_, _, _})   -> <<(now2f(TS)):1/big-float-unit:64>>;
encode(timestamptz, TS) -> <<(timestamp2f(TS)):1/big-float-unit:64>>;
encode(interval, {T, D, M}) -> <<(time2f(T)):1/big-float-unit:64, D:?int32, M:?int32>>.

f2time(N) ->
    {R1, Hour} = tmodulo(N, ?SECS_PER_HOUR),
    {R2, Min}  = tmodulo(R1, ?SECS_PER_MINUTE),
    {R3, Sec}  = tmodulo(R2, 1.0),
    case timeround(R3) of
        US when US >= 1.0 -> f2time(ceiling(N));
        US                -> {Hour, Min, Sec + US}
    end.

time2f({H, M, S}) ->
    ((H * ?MINS_PER_HOUR + M) * ?SECS_PER_MINUTE) + S.

f2timestamp(N) ->
    case tmodulo(N, ?SECS_PER_DAY) of
        {T, D} when T < 0 -> f2timestamp2(D - 1 + ?POSTGRES_EPOC_JDATE, T + ?SECS_PER_DAY);
        {T, D}            -> f2timestamp2(D + ?POSTGRES_EPOC_JDATE, T)
    end.

f2timestamp2(D, T) ->
    {_H, _M, S} = Time = f2time(T),
    Date = epgsql_idatetime:j2date(D),
    case tsround(S - trunc(S)) of
        N when N >= 1.0 ->
            case ceiling(T) of
                T2 when T2 > ?SECS_PER_DAY -> f2timestamp2(D + 1, 0.0);
                T2                         -> f2timestamp2(T2, D)
            end;
        _ -> ok
    end,
    {Date, Time}.

timestamp2f({Date, Time}) ->
    D = epgsql_idatetime:date2j(Date) - ?POSTGRES_EPOC_JDATE,
    D * ?SECS_PER_DAY + time2f(Time).

now2f({MegaSecs, Secs, MicroSecs}) ->
    MegaSecs * 1000000 + Secs + MicroSecs / 1000000.0 - ?POSTGRES_EPOC_SECS.

tmodulo(T, U) ->
    Q = case T < 0 of
        true  -> ceiling(T / U);
        false -> flooring(T / U)
    end,
    case Q of
        0 -> {T, Q};
        _ -> {T - rint(Q * U), Q}
    end.

rint(N)      -> round(N) * 1.0.
timeround(J) -> rint(J * 10000000000.0) / 10000000000.0.
tsround(J)   -> rint(J * 1000000.0) / 1000000.0.

flooring(X) ->
    T = erlang:trunc(X),
    case (X - T) of
        N when N < 0 -> T - 1;
        N when N > 0 -> T;
        _            -> T
    end.

ceiling(X) ->
    T = erlang:trunc(X),
    case (X - T) of
        N when N < 0 -> T;
        N when N > 0 -> T + 1;
        _            -> T
    end.
