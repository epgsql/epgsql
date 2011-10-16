%%% Copyright (C) 2008 - Will Glozer.  All rights reserved.

-module(pgsql).

-export([connect/2, connect/3, connect/4, close/1]).
-export([get_parameter/2, squery/2, equery/2, equery/3]).
-export([parse/2, parse/3, parse/4, describe/2, describe/3]).
-export([bind/3, bind/4, execute/2, execute/3, execute/4]).
-export([close/2, close/3, sync/1]).
-export([with_transaction/2]).

-include("pgsql.hrl").

%% -- client interface --

connect(Host, Opts) ->
    connect(Host, os:getenv("USER"), "", Opts).

connect(Host, Username, Opts) ->
    connect(Host, Username, "", Opts).

connect(Host, Username, Password, Opts) ->
    {ok, C} = pgsql_sock:start_link(),
    Ref = pgsql_sock:connect(C, Host, Username, Password, Opts),
    receive
        {Ref, connected} ->
            {ok, C};
        {Ref, Error = {error, _}} ->
            Error;
        {'EXIT', C, _Reason} ->
            {error, closed}
    end.

close(C) ->
    pgsql_sock:close(C).

get_parameter(C, Name) ->
    pgsql_sock:get_parameter(C, Name).

squery(C, Sql) ->
    ok = pgsql_sock:squery(C, Sql),
    case receive_results(C, []) of
        [Result] -> Result;
        Results  -> Results
    end.

equery(C, Sql) ->
    equery(C, Sql, []).

equery(C, Sql, Parameters) ->
    case parse(C, Sql) of
        {ok, S} ->
            ok = bind(C, S, Parameters),
            ok = pgsql_sock:execute(C, S, "", 0),
            ok = close(C, S),
            ok = sync(C),
            receive_result(C, undefined);
        Error ->
            Error
    end.

%% parse

parse(C, Sql) ->
    parse(C, "", Sql, []).

parse(C, Sql, Types) ->
    parse(C, "", Sql, Types).

parse(C, Name, Sql, Types) ->
    pgsql_sock:parse(C, Name, Sql, Types).

%% bind

bind(C, Statement, Parameters) ->
    bind(C, Statement, "", Parameters).

bind(C, Statement, PortalName, Parameters) ->
    pgsql_sock:bind(C, Statement, PortalName, Parameters).

%% execute

execute(C, S) ->
    execute(C, S, "", 0).

execute(C, S, N) ->
    execute(C, S, "", N).

execute(C, S, PortalName, N) ->
    pgsql_sock:execute(C, S, PortalName, N),
    receive_extended_result(C).

%% statement/portal functions

describe(C, #statement{name = Name}) ->
    pgsql_sock:describe(C, statement, Name).

describe(C, Type, Name) ->
    pgsql_sock:describe(C, Type, Name).

close(C, #statement{name = Name}) ->
    pgsql_sock:close(C, statement, Name).

close(C, Type, Name) ->
    pgsql_sock:close(C, Type, Name).

sync(C) ->
    pgsql_sock:sync(C).

%% misc helper functions
with_transaction(C, F) ->
    try {ok, [], []} = squery(C, "BEGIN"),
        R = F(C),
        {ok, [], []} = squery(C, "COMMIT"),
        R
    catch
        _:Why ->
            squery(C, "ROLLBACK"),
            {rollback, Why}
    end.

%% -- internal functions --

receive_result(C, Result) ->
    try receive_result(C, [], []) of
        done    -> Result;
        R       -> receive_result(C, R)
    catch
        throw:E -> E
    end.

receive_results(C, Results) ->
    try receive_result(C, [], []) of
        done    -> lists:reverse(Results);
        R       -> receive_results(C, [R | Results])
    catch
        throw:E -> E
    end.

receive_result(C, Cols, Rows) ->
    receive
        {pgsql, C, {columns, Cols2}} ->
            receive_result(C, Cols2, Rows);
        {pgsql, C, {data, Row}} ->
            receive_result(C, Cols, [Row | Rows]);
        {pgsql, C, {error, _E} = Error} ->
            Error;
        {pgsql, C, {complete, {_Type, Count}}} ->
            case Rows of
                [] -> {ok, Count};
                _L -> {ok, Count, Cols, lists:reverse(Rows)}
            end;
        {pgsql, C, {complete, _Type}} ->
            {ok, Cols, lists:reverse(Rows)};
        {pgsql, C, done} ->
            done;
        {pgsql, C, timeout} ->
            throw({error, timeout});
        {'EXIT', C, _Reason} ->
            throw({error, closed})
    end.

receive_extended_result(C)->
    receive_extended_result(C, []).

receive_extended_result(C, Rows) ->
    receive
        {pgsql, C, {data, Row}} ->
            receive_extended_result(C, [Row | Rows]);
        {pgsql, C, {error, _E} = Error} ->
            Error;
        {pgsql, C, suspended} ->
            {partial, lists:reverse(Rows)};
        {pgsql, C, {complete, {_Type, Count}}} ->
            case Rows of
                [] -> {ok, Count};
                _L -> {ok, Count, lists:reverse(Rows)}
            end;
        {pgsql, C, {complete, _Type}} ->
            {ok, lists:reverse(Rows)};
        {pgsql, C, timeout} ->
            {error, timeout};
        {'EXIT', C, _Reason} ->
            {error, closed}
    end.
