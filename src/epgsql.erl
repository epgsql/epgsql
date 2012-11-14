%%% Copyright (C) 2008 - Will Glozer.  All rights reserved.

-module(epgsql).

-import(proplists, [get_value/2,
                    get_value/3]).

-export([connect/2]).

-export([select/2, select/3, select/4,
         insert/3, insert/4,
         update/4,
         delete/3]).

-export([get_parameter/2,
         squery/2,
         equery/2, equery/3]).

-export([parse/2, parse/3, parse/4,
         describe/2, describe/3]).

-export([bind/3, bind/4,
         execute/2, execute/3, execute/4]).

-export([close/2, close/3,
         sync/1]).

-export([with_transaction/2]).

-export([escape/1,
         escape_like/1]).

-include("pgsql.hrl").

%========================
% connect and add to pool
%========================
connect(Pool, Opts) ->
    {ok, C} = pgsql_conn:start_link(),
    Host = get_value(host, Opts, "localhost"),
    Username = get_value(username, Opts),
    Password = get_value(password, Opts),
    PgOpts = [{database, get_value(database, Opts)},
            {timeout, get_value(timeout, Opts, 10000)},
            {port, get_value(port, Opts, 5432)}],
    pgsql_conn:connect(C, Host, Username, Password, PgOpts),
    epgsql_pool:add_conn(Pool, C),
    {ok, C}.

%========================
% api 
%========================
select(Pool, Table) when is_atom(Pool) ->
    squery(Pool, epgsql_builder:select(Table)).

select(Pool, Table, Fields) when is_list(Fields) ->
    squery(Pool, epgsql_builder:select(Table, Fields));

select(Pool, Table, Where) when is_tuple(Where) ->
    squery(Pool, epgsql_builder:select(Table, Where)).

select(Pool, Table, Fields, Where) 
    when is_list(Fields) and is_tuple(Where) ->
    squery(Pool, epgsql_builder:select(Table, Fields, Where)).

insert(Pool, Table, Record) ->
    squery(Pool, epgsql_builder:insert(Table, Record)).

insert(Pool, Table, Fields, Values) ->
    squery(Pool, epgsql_builder:insert(Table, Fields, Values)).

update(Pool, Table, Record, Where) ->
    squery(Pool, epgsql_builder:update(Table, Record, Where)).

delete(Pool, Table, Where) ->
    squery(Pool, epgsql_builder:delete(Table, Where)).

squery(Pool, Sql) ->
    with_conn(Pool, fun(C) ->
        ok = pgsql_conn:squery(C, Sql),
        case receive_results(C, []) of
        [Result] -> Result;
        Results  -> Results
        end
    end).

equery(Pool, Sql) ->
    equery(Pool, Sql, []).

equery(Pool, Sql, Parameters) ->
    with_conn(Pool, fun(C) ->
        case pgsql_conn:parse(C, "", Sql, []) of
            {ok, #statement{types = Types} = S} ->
                Typed_Parameters = lists:zip(Types, Parameters),
                ok = pgsql_conn:equery(C, S, Typed_Parameters),
                receive_result(C, undefined);
            Error ->
                Error
        end
    end).

with_conn(Pool, Fun) ->
    Fun(epgsql_pool:get_conn(Pool)).

% api with connection

get_parameter(C, Name) ->
    pgsql_conn:get_parameter(C, Name).

parse(C, Sql) ->
    parse(C, "", Sql, []).

parse(C, Sql, Types) ->
    parse(C, "", Sql, Types).

parse(C, Name, Sql, Types) ->
    pgsql_conn:parse(C, Name, Sql, Types).

%% bind

bind(C, Statement, Parameters) ->
    bind(C, Statement, "", Parameters).

bind(C, Statement, PortalName, Parameters) ->
    pgsql_conn:bind(C, Statement, PortalName, Parameters).

%% execute

execute(C, S) ->
    execute(C, S, "", 0).

execute(C, S, N) ->
    execute(C, S, "", N).

execute(C, S, PortalName, N) ->
    pgsql_conn:execute(C, S, PortalName, N),
    receive_extended_result(C).

%% statement/portal functions

describe(C, #statement{name = Name}) ->
    pgsql_conn:describe(C, statement, Name).

describe(C, Type, Name) ->
    pgsql_conn:describe(C, Type, Name).

close(C, #statement{name = Name}) ->
    pgsql_conn:close(C, statement, Name).

close(C, Type, Name) ->
    pgsql_conn:close(C, Type, Name).

sync(C) ->
    pgsql_conn:sync(C).

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

%% -- utility functions --

escape_like(S) when is_list(S) ->
    [escape_like(C) || C <- S];
escape_like($%) -> "\\%";
escape_like($_) -> "\\_";
escape_like(C)  -> escape(C).

%% Escape character that will confuse an SQL engine
escape(S) when is_list(S) ->
	[escape(C) || C <- S];
%% Characters to escape
escape($\0) -> "\\0";
escape($\n) -> "\\n";
escape($\t) -> "\\t";
escape($\b) -> "\\b";
escape($\r) -> "\\r";
escape($')  -> "\\'";
escape($")  -> "\\\"";
escape($\\) -> "\\\\";
escape(C)   -> C.

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
