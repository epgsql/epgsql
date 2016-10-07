%%% Copyright (C) 2011 - Anton Lebedevich.  All rights reserved.

-module(epgsqla).

-export([start_link/0,
         connect/2, connect/3, connect/4, connect/5,
         close/1,
         get_parameter/2,
         set_notice_receiver/2,
         squery/2,
         equery/2, equery/3,
         prepared_query/3,
         parse/2, parse/3, parse/4,
         describe/2, describe/3,
         bind/3, bind/4,
         execute/2, execute/3, execute/4,
         execute_batch/2,
         close/2, close/3,
         sync/1,
         cancel/1,
         complete_connect/2]).

-include("epgsql.hrl").

%% -- client interface --
-spec start_link() -> {ok, pid()}.
start_link() ->
    epgsql_sock:start_link().

connect(Host, Opts) ->
    connect(Host, os:getenv("USER"), "", Opts).

connect(Host, Username, Opts) ->
    connect(Host, Username, "", Opts).

connect(Host, Username, Password, Opts) ->
    {ok, C} = epgsql_sock:start_link(),
    connect(C, Host, Username, Password, Opts).

-spec connect(epgsql:connection(), inet:ip_address() | inet:hostname(),
              string(), string(), [epgsql:connect_option()]) -> reference().
connect(C, Host, Username, Password, Opts) ->
    complete_connect(C, cast(C, {connect, Host, Username, Password, Opts})).

-spec close(epgsql:connection()) -> ok.
close(C) ->
    epgsql_sock:close(C).

-spec get_parameter(epgsql:connection(), binary()) -> binary() | undefined.
get_parameter(C, Name) ->
    epgsql_sock:get_parameter(C, Name).

-spec set_notice_receiver(epgsql:connection(), undefined | pid() | atom()) ->
                                 {ok, Previous :: pid() | atom()}.
set_notice_receiver(C, PidOrName) ->
    epgsql_sock:set_notice_receiver(C, PidOrName).

-spec squery(epgsql:connection(), string()) -> reference().
squery(C, Sql) ->
    cast(C, {squery, Sql}).

equery(C, Sql) ->
    equery(C, Sql, []).

-spec equery(epgsql:connection(), #statement{}, [epgsql:typed_param()]) -> reference().
equery(C, Statement, TypedParameters) ->
    cast(C, {equery, Statement, TypedParameters}).

-spec prepared_query(epgsql:connection(), #statement{}, [epgsql:typed_param()]) -> reference().
prepared_query(C, Statement, TypedParameters) ->
    cast(C, {prepared_query, Statement, TypedParameters}).

parse(C, Sql) ->
    parse(C, "", Sql, []).

parse(C, Sql, Types) ->
    parse(C, "", Sql, Types).

-spec parse(epgsql:connection(), iolist(), string(), [epgsql_type()]) -> reference().
parse(C, Name, Sql, Types) ->
    cast(C, {parse, Name, Sql, Types}).

bind(C, Statement, Parameters) ->
    bind(C, Statement, "", Parameters).

-spec bind(epgsql:connection(), #statement{}, string(), [epgsql:bind_param()]) -> reference().
bind(C, Statement, PortalName, Parameters) ->
    cast(C, {bind, Statement, PortalName, Parameters}).

execute(C, S) ->
    execute(C, S, "", 0).

execute(C, S, N) ->
    execute(C, S, "", N).

-spec execute(epgsql:connection(), #statement{}, string(), non_neg_integer()) -> reference().
execute(C, Statement, PortalName, MaxRows) ->
    cast(C, {execute, Statement, PortalName, MaxRows}).

-spec execute_batch(epgsql:connection(), [{#statement{}, [epgsql:bind_param()]}]) -> reference().
execute_batch(C, Batch) ->
    cast(C, {execute_batch, Batch}).

describe(C, #statement{name = Name}) ->
    describe(C, statement, Name).

describe(C, statement, Name) ->
    cast(C, {describe_statement, Name});

describe(C, portal, Name) ->
    cast(C, {describe_portal, Name}).

close(C, #statement{name = Name}) ->
    close(C, statement, Name).

close(C, Type, Name) ->
    cast(C, {close, Type, Name}).

sync(C) ->
    cast(C, sync).

-spec cancel(epgsql:connection()) -> ok.
cancel(C) ->
    epgsql_sock:cancel(C).

%% -- internal functions --

cast(C, Command) ->
    Ref = make_ref(),
    gen_server:cast(C, {{cast, self(), Ref}, Command}),
    Ref.

complete_connect(C, Ref) ->
    receive
        %% If we connect, then try and update the type cache.  When
        %% all is said and done, pass the result along as a message.
        {C, Ref, Msg} ->
            Retval =
                case Msg of
                    connected ->
                        ok = epgsql:update_type_cache(C),
                        {C, Ref, connected};
                    {error, Error} ->
                        {C, Ref, {error, Error}}
                end,
            self() ! Retval;
        {'EXIT', C, Reason} ->
            self() ! {'EXIT', C, Reason}
    end,
    Ref.
