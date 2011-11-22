%%% Copyright (C) 2008 - Will Glozer.  All rights reserved.
%%% Copyright (C) 2011 - Anton Lebedevich.  All rights reserved.
%%%
%%% Emulates original epgsql API over ipgsql for original tests

-module(pgsql_incremental).

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
    {ok, C} = ipgsql:start_link(),
    Ref = ipgsql:connect(C, Host, Username, Password, Opts),
    receive
        {C, Ref, connected} ->
            {ok, C};
        {C, Ref, Error = {error, _}} ->
            Error;
        {'EXIT', C, _Reason} ->
            {error, closed}
    end.

close(C) ->
    ipgsql:close(C).

get_parameter(C, Name) ->
    ipgsql:get_parameter(C, Name).

squery(C, Sql) ->
    Ref = ipgsql:squery(C, Sql),
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
            Ref = ipgsql:equery(C, S, Typed_Parameters),
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
    Ref = ipgsql:parse(C, Name, Sql, Types),
    sync_on_error(C, receive_describe(C, Ref, #statement{name = Name})).

%% bind

bind(C, Statement, Parameters) ->
    bind(C, Statement, "", Parameters).

bind(C, Statement, PortalName, Parameters) ->
    Ref = ipgsql:bind(C, Statement, PortalName, Parameters),
    sync_on_error(C, receive_atom(C, Ref, ok, ok)).

%% execute

execute(C, S) ->
    execute(C, S, "", 0).

execute(C, S, N) ->
    execute(C, S, "", N).

execute(C, S, PortalName, N) ->
    Ref = ipgsql:execute(C, S, PortalName, N),
    receive_extended_result(C, Ref).

%% statement/portal functions

describe(C, #statement{name = Name}) ->
    describe(C, statement, Name).

describe(C, statement, Name) ->
    Ref = ipgsql:describe(C, statement, Name),
    sync_on_error(C, receive_describe(C, Ref, #statement{name = Name}));

describe(C, Type, Name) ->
    %% TODO unknown result format of Describe portal
    ipgsql:describe(C, Type, Name).

close(C, #statement{name = Name}) ->
    close(C, statement, Name).

close(C, Type, Name) ->
    Ref = ipgsql:close(C, Type, Name),
    receive_atom(C, Ref, ok, ok).

sync(C) ->
    Ref = ipgsql:sync(C),
    receive_atom(C, Ref, ok, ok).

%% misc helper functions
with_transaction(C, F) ->
    try {ok, [], []} = squery(C, "BEGIN"),
        R = F(C),
        {ok, [], []} = squery(C, "COMMIT"),
        R
    catch
        _:Why ->
            squery(C, "ROLLBACK"),
            %% TODO hides error stacktrace
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
        {C, Ref, {columns, Cols2}} ->
            receive_result(C, Ref, Cols2, Rows);
        {C, Ref, {data, Row}} ->
            receive_result(C, Ref, Cols, [Row | Rows]);
        {C, Ref, {error, _E} = Error} ->
            Error;
        {C, Ref, {complete, {_Type, Count}}} ->
            case Rows of
                [] -> {ok, Count};
                _L -> {ok, Count, Cols, lists:reverse(Rows)}
            end;
        {C, Ref, {complete, _Type}} ->
            {ok, Cols, lists:reverse(Rows)};
        {C, Ref, done} ->
            done;
        {'EXIT', C, _Reason} ->
            throw({error, closed})
    end.

receive_extended_result(C, Ref)->
    receive_extended_result(C, Ref, []).

receive_extended_result(C, Ref, Rows) ->
    receive
        {C, Ref, {data, Row}} ->
            receive_extended_result(C, Ref, [Row | Rows]);
        {C, Ref, {error, _E} = Error} ->
            Error;
        {C, Ref, suspended} ->
            {partial, lists:reverse(Rows)};
        {C, Ref, {complete, {_Type, Count}}} ->
            case Rows of
                [] -> {ok, Count};
                _L -> {ok, Count, lists:reverse(Rows)}
            end;
        {C, Ref, {complete, _Type}} ->
            {ok, lists:reverse(Rows)};
        {'EXIT', C, _Reason} ->
            {error, closed}
    end.

receive_describe(C, Ref, Statement = #statement{}) ->
    receive
        {C, Ref, {types, Types}} ->
            receive_describe(C, Ref, Statement#statement{types = Types});
        {C, Ref, {columns, Columns}} ->
            Columns2 = [Col#column{format = pgsql_wire:format(Col#column.type)} || Col <- Columns],
            {ok, Statement#statement{columns = Columns2}};
        {C, Ref, no_data} ->
            {ok, Statement#statement{columns = []}};
        {C, Ref, Error = {error, _}} ->
            Error;
        {'EXIT', C, _Reason} ->
            {error, closed}
    end.

receive_atom(C, Ref, Receive, Return) ->
    receive
        {C, Ref, Receive} ->
            Return;
        {C, Ref, Error = {error, _}} ->
            Error;
        {'EXIT', C, _Reason} ->
            {error, closed}
    end.

sync_on_error(C, Error = {error, _}) ->
    Ref = ipgsql:sync(C),
    receive_atom(C, Ref, ok, ok),
    Error;

sync_on_error(_C, R) ->
    R.

