%%% Copyright (C) 2008 - Will Glozer.  All rights reserved.
%%% Copyright (C) 2011 - Anton Lebedevich.  All rights reserved.
%%%
%%% Emulates original epgsql API over epgsqla for original tests

-module(epgsql_cast).

-export([connect/2, connect/3, connect/4, close/1]).
-export([get_parameter/2, set_notice_receiver/2, squery/2, equery/2, equery/3]).
-export([prepared_query/3]).
-export([parse/2, parse/3, parse/4, describe/2, describe/3]).
-export([bind/3, bind/4, execute/2, execute/3, execute/4, execute_batch/2]).
-export([close/2, close/3, sync/1]).
-export([with_transaction/2]).
-export([receive_result/2, sync_on_error/2]).

-include("epgsql.hrl").

%% -- client interface --

connect(Host, Opts) ->
    connect(Host, os:getenv("USER"), "", Opts).

connect(Host, Username, Opts) ->
    connect(Host, Username, "", Opts).

connect(Host, Username, Password, Opts) ->
    {ok, C} = epgsql_sock:start_link(),
    Ref = epgsqla:connect(C, Host, Username, Password, Opts),
    %% TODO connect timeout
    receive
        {C, Ref, connected} ->
            {ok, C};
        {C, Ref, Error = {error, _}} ->
            Error;
        {'EXIT', C, _Reason} ->
            {error, closed}
    end.

close(C) ->
    epgsqla:close(C).

get_parameter(C, Name) ->
    epgsqla:get_parameter(C, Name).

set_notice_receiver(C, PidOrName) ->
    epgsqla:set_notice_receiver(C, PidOrName).

squery(C, Sql) ->
    Ref = epgsqla:squery(C, Sql),
    receive_result(C, Ref).

equery(C, Sql) ->
    equery(C, Sql, []).

%% TODO add fast_equery command that doesn't need parsed statement
equery(C, Sql, Parameters) ->
    case parse(C, Sql) of
        {ok, #statement{types = Types} = S} ->
            Typed_Parameters = lists:zip(Types, Parameters),
            Ref = epgsqla:equery(C, S, Typed_Parameters),
            receive_result(C, Ref);
        Error ->
            Error
    end.

prepared_query(C, Name, Parameters) ->
    case describe(C, statement, Name) of
        {ok, #statement{types = Types} = S} ->
            Typed_Parameters = lists:zip(Types, Parameters),
            Ref = epgsqla:prepared_query(C, S, Typed_Parameters),
            receive_result(C, Ref);
        Error ->
            Error
    end.

%% parse

parse(C, Sql) ->
    parse(C, "", Sql, []).

parse(C, Sql, Types) ->
    parse(C, "", Sql, Types).

parse(C, Name, Sql, Types) ->
    Ref = epgsqla:parse(C, Name, Sql, Types),
    sync_on_error(C, receive_result(C, Ref)).

%% bind

bind(C, Statement, Parameters) ->
    bind(C, Statement, "", Parameters).

bind(C, Statement, PortalName, Parameters) ->
    Ref = epgsqla:bind(C, Statement, PortalName, Parameters),
    sync_on_error(C, receive_result(C, Ref)).

%% execute

execute(C, S) ->
    execute(C, S, "", 0).

execute(C, S, N) ->
    execute(C, S, "", N).

execute(C, S, PortalName, N) ->
    Ref = epgsqla:execute(C, S, PortalName, N),
    receive_result(C, Ref).

execute_batch(C, Batch) ->
    Ref = epgsqla:execute_batch(C, Batch),
    receive_result(C, Ref).

%% statement/portal functions

describe(C, #statement{name = Name}) ->
    describe(C, statement, Name).

describe(C, Type, Name) ->
    Ref = epgsqla:describe(C, Type, Name),
    %% TODO unknown result format of Describe portal
    sync_on_error(C, receive_result(C, Ref)).

close(C, #statement{name = Name}) ->
    close(C, statement, Name).

close(C, Type, Name) ->
    Ref = epgsqla:close(C, Type, Name),
    receive_result(C, Ref).

sync(C) ->
    Ref = epgsqla:sync(C),
    receive_result(C, Ref).

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

receive_result(C, Ref) ->
    %% TODO timeout
    receive
        {C, Ref, Result} ->
            Result;
        %% TODO no 'EXIT' for not linked processes
        {'EXIT', C, _Reason} ->
            {error, closed}
    end.

sync_on_error(C, Error = {error, _}) ->
    Ref = epgsqla:sync(C),
    receive_result(C, Ref),
    Error;

sync_on_error(_C, R) ->
    R.

