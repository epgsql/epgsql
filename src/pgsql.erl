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
    %% TODO connect timeout
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
    Ref = pgsql_sock:squery(C, Sql),
    case receive_results(C, Ref, []) of
        [Result] -> Result;
        Results  -> Results
    end.

equery(C, Sql) ->
    equery(C, Sql, []).

equery(C, Sql, Parameters) ->
    case parse(C, Sql) of
        {ok, #statement{types = Types} = S} ->
            Typed_Parameters = lists:zip(Types, Parameters),
            Ref = pgsql_sock:equery(C, S, Typed_Parameters),
            receive_result(C, Ref, undefined);
        Error ->
            Error
    end.

%% parse

parse(C, Sql) ->
    parse(C, "", Sql, []).

parse(C, Sql, Types) ->
    parse(C, "", Sql, Types).

parse(C, Name, Sql, Types) ->
    Ref = pgsql_sock:parse(C, Name, Sql, Types),
    receive_describe(C, Ref, #statement{name = Name}).

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
    Ref = pgsql_sock:execute(C, S, PortalName, N),
    receive_extended_result(C, Ref).

%% statement/portal functions

describe(C, Statement = #statement{name = Name}) ->
    Ref = pgsql_sock:describe(C, statement, Name),
    receive_describe(C, Ref, Statement).

describe(C, statement, Name) ->
    Ref = pgsql_sock:describe(C, statement, Name),
    receive_describe(C, Ref, #statement{name = Name});

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

receive_result(C, Ref, Result) ->
    try receive_result(C, Ref, [], []) of
        done    -> Result;
        R       -> receive_result(C, Ref, R)
    catch
        throw:E -> E
    end.

receive_results(C, Ref, Results) ->
    try receive_result(C, Ref, [], []) of
        done    -> lists:reverse(Results);
        R       -> receive_results(C, Ref, [R | Results])
    catch
        throw:E -> E
    end.

receive_result(C, Ref, Cols, Rows) ->
    receive
        {Ref, {columns, Cols2}} ->
            receive_result(C, Ref, Cols2, Rows);
        {Ref, {data, Row}} ->
            receive_result(C, Ref, Cols, [Row | Rows]);
        {Ref, {error, _E} = Error} ->
            Error;
        {Ref, {complete, {_Type, Count}}} ->
            case Rows of
                [] -> {ok, Count};
                _L -> {ok, Count, Cols, lists:reverse(Rows)}
            end;
        {Ref, {complete, _Type}} ->
            {ok, Cols, lists:reverse(Rows)};
        {Ref, done} ->
            done;
        {Ref, timeout} ->
            throw({error, timeout});
        {'EXIT', C, _Reason} ->
            throw({error, closed})
    end.

receive_extended_result(C, Ref)->
    receive_extended_result(C, Ref, []).

receive_extended_result(C, Ref, Rows) ->
    receive
        {Ref, {data, Row}} ->
            receive_extended_result(C, Ref, [Row | Rows]);
        {Ref, {error, _E} = Error} ->
            Error;
        {Ref, suspended} ->
            {partial, lists:reverse(Rows)};
        {Ref, {complete, {_Type, Count}}} ->
            case Rows of
                [] -> {ok, Count};
                _L -> {ok, Count, lists:reverse(Rows)}
            end;
        {Ref, {complete, _Type}} ->
            {ok, lists:reverse(Rows)};
        {Ref, timeout} ->
            {error, timeout};
        {'EXIT', C, _Reason} ->
            {error, closed}
    end.

receive_describe(C, Ref, Statement = #statement{}) ->
    receive
        {Ref, {types, Types}} ->
            receive_describe(C, Ref, Statement#statement{types = Types});
        {Ref, {columns, Columns}} ->
            Statement#statement{columns = Columns};
        {Ref, no_data} ->
            Statement#statement{columns = []};
        {Ref, Error = {error, _}} ->
            Error;
        {'EXIT', C, _Reason} ->
            {error, closed}
    end.
