%%% Copyright (C) 2008 - Will Glozer.  All rights reserved.
%%% Copyright (C) 2011 - Anton Lebedevich.  All rights reserved.
%%%
%%% Emulates original epgsql API over epgsqli for original tests

-module(epgsql_incremental).

-export([connect/1, connect/2, connect/3, connect/4, close/1]).
-export([get_parameter/2, set_notice_receiver/2, get_cmd_status/1, get_os_pid/1,
         squery/2, equery/2, equery/3]).
-export([prepared_query/3]).
-export([parse/2, parse/3, parse/4, describe/2, describe/3]).
-export([bind/3, bind/4, execute/2, execute/3, execute/4, execute_batch/2, execute_batch/3]).
-export([close/2, close/3, sync/1]).

-include("epgsql.hrl").

%% -- client interface --

connect(Opts) ->
    Ref = epgsqli:connect(Opts),
    await_connect(Ref, Opts).

connect(Host, Opts) ->
    Ref = epgsqli:connect(Host, Opts),
    await_connect(Ref, Opts).

connect(Host, Username, Opts) ->
    Ref = epgsqli:connect(Host, Username, Opts),
    await_connect(Ref, Opts).

connect(Host, Username, Password, Opts) ->
    Ref = epgsqli:connect(Host, Username, Password, Opts),
    await_connect(Ref, Opts).

await_connect(Ref, Opts0) ->
    Opts = epgsql:to_map(Opts0),
    Timeout = maps:get(timeout, Opts, 5000),
    receive
        {C, Ref, connected} ->
            {ok, C};
        {_C, Ref, Error = {error, _}} ->
            Error
    after Timeout ->
            error(timeout)
    end.

close(C) ->
    epgsqli:close(C).

get_parameter(C, Name) ->
    epgsqli:get_parameter(C, Name).

set_notice_receiver(C, PidOrName) ->
    epgsqli:set_notice_receiver(C, PidOrName).

get_cmd_status(C) ->
    epgsqli:get_cmd_status(C).

get_os_pid(C) ->
    epgsqli:get_os_pid(C).

squery(C, Sql) ->
    Ref = epgsqli:squery(C, Sql),
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
            Ref = epgsqli:equery(C, S, Typed_Parameters),
            receive_result(C, Ref, undefined);
        Error ->
            Error
    end.

prepared_query(C, #statement{types = Types} = Stmt, Parameters) ->
    TypedParameters = lists:zip(Types, Parameters),
    Ref = epgsqli:prepared_query(C, Stmt, TypedParameters),
    receive_result(C, Ref, undefined);
prepared_query(C, Name, Parameters) ->
    case describe(C, statement, Name) of
        {ok, S} ->
            prepared_query(C, S, Parameters);
        Error ->
            Error
    end.


%% parse

parse(C, Sql) ->
    parse(C, "", Sql, []).

parse(C, Sql, Types) ->
    parse(C, "", Sql, Types).

parse(C, Name, Sql, Types) ->
    Ref = epgsqli:parse(C, Name, Sql, Types),
    sync_on_error(C, receive_describe(C, Ref, #statement{name = Name})).

%% bind

bind(C, Statement, Parameters) ->
    bind(C, Statement, "", Parameters).

bind(C, Statement, PortalName, Parameters) ->
    Ref = epgsqli:bind(C, Statement, PortalName, Parameters),
    sync_on_error(C, receive_atom(C, Ref, ok, ok)).

%% execute

execute(C, S) ->
    execute(C, S, "", 0).

execute(C, S, N) ->
    execute(C, S, "", N).

execute(C, S, PortalName, N) ->
    Ref = epgsqli:execute(C, S, PortalName, N),
    receive_extended_result(C, Ref, []).

execute_batch(C, Batch) ->
    Ref = epgsqli:execute_batch(C, Batch),
    receive_extended_results(C, Ref, []).

execute_batch(C, #statement{columns = Cols} = Stmt, Batch) ->
    Ref = epgsqli:execute_batch(C, Stmt, Batch),
    {Cols, receive_extended_results(C, Ref, [])};
execute_batch(C, Sql, Batch) ->
    case parse(C, Sql) of
        {ok, #statement{} = S} ->
            execute_batch(C, S, Batch);
        Error ->
            Error
    end.

%% statement/portal functions

describe(C, #statement{name = Name}) ->
    describe(C, statement, Name).

describe(C, statement, Name) ->
    Ref = epgsqli:describe(C, statement, Name),
    sync_on_error(C, receive_describe(C, Ref, #statement{name = Name}));

describe(C, portal, Name) ->
    Ref = epgsqli:describe(C, portal, Name),
    sync_on_error(C, receive_describe_portal(C, Ref)).

close(C, #statement{name = Name}) ->
    close(C, statement, Name).

close(C, Type, Name) ->
    Ref = epgsqli:close(C, Type, Name),
    receive_atom(C, Ref, ok, ok).

sync(C) ->
    Ref = epgsqli:sync(C),
    receive_atom(C, Ref, ok, ok).

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
            case Cols of
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

receive_extended_results(C, Ref, Results) ->
    try receive_extended_result(C, Ref, []) of
        done    -> lists:reverse(Results);
        R       -> receive_extended_results(C, Ref, [R | Results])
    catch
        throw:E -> E
    end.

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
        {C, Ref, done} ->
            done;
        {'EXIT', C, _Reason} ->
            {error, closed}
    end.

receive_describe(C, Ref, Statement = #statement{}) ->
    receive
        {C, Ref, {types, Types}} ->
            receive_describe(C, Ref, Statement#statement{types = Types});
        {C, Ref, {columns, Columns}} ->
            {ok, Statement#statement{columns = Columns}};
        {C, Ref, no_data} ->
            {ok, Statement#statement{columns = []}};
        {C, Ref, Error = {error, _}} ->
            Error;
        {'EXIT', C, _Reason} ->
            {error, closed}
    end.

receive_describe_portal(C, Ref) ->
    receive
        {C, Ref, {columns, Columns}} ->
            {ok, Columns};
        {C, Ref, no_data} ->
            {ok, []};
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
    Ref = epgsqli:sync(C),
    receive_atom(C, Ref, ok, ok),
    Error;

sync_on_error(_C, R) ->
    R.

