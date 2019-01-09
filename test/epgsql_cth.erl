-module(epgsql_cth).

-export([
         init/2,
         terminate/1,
         pre_init_per_suite/3
        ]).

-include_lib("common_test/include/ct.hrl").
-include("epgsql_tests.hrl").

init(_Id, State) ->
    Start = os:timestamp(),
    PgConfig = start_postgres(),
    ok = create_testdbs(PgConfig),
    ct:pal(info, "postgres started in ~p ms\n",
        [timer:now_diff(os:timestamp(), Start) / 1000]),
    [{pg_config, PgConfig}|State].

pre_init_per_suite(_SuiteName, Config, State) ->
    {Config ++ State, State}.

terminate(State) ->
    ok = stop_postgres(?config(pg_config, State)).

create_testdbs(Config) ->
    PgHost = ?config(host, Config),
    PgPort = ?config(port, Config),
    PgUser = ?config(user, Config),
    Utils = ?config(utils, Config),
    Psql = ?config(psql, Utils),
    CreateDB = ?config(createdb, Utils),

    Opts = lists:concat([" -h ", PgHost, " -p ", PgPort, " "]),
    Cmds = [
        [CreateDB, Opts, PgUser],
        [Psql, Opts, "template1 < ", filename:join(?TEST_DATA_DIR, "test_schema.sql")]
    ],
    lists:foreach(fun(Cmd) ->
        {ok, []} = exec:run(lists:flatten(Cmd), [sync])
    end, Cmds).

%% =============================================================================
%% start postgresql
%% =============================================================================

-define(PG_TIMEOUT, 30000).

start_postgres() ->
    ok = application:start(erlexec),
    pipe([
        fun find_utils/1,
        fun init_database/1,
        fun get_version/1,
        fun write_postgresql_config/1,
        fun copy_certs/1,
        fun write_pg_hba_config/1,
        fun start_postgresql/1
    ], []).

stop_postgres(Config) ->
    PgProc = ?config(proc, Config),

    PgProc ! stop,
    ok.

find_utils(State) ->
    Utils = [initdb, createdb, postgres, psql],
    UtilsConfig = lists:foldl(fun(U, Acc) ->
        UList = atom_to_list(U),
        Path = case os:find_executable(UList) of
            false ->
                case filelib:wildcard("/usr/lib/postgresql/**/bin/" ++ UList) of
                    [] ->
                        ct:pal(error, "~s not found", [U]),
                        throw({util_no_found, U});
                    List -> lists:last(lists:sort(List))
                end;
            P -> P
        end,
        [{U, Path}|Acc]
    end, [], Utils),
    [{utils, UtilsConfig}|State].

start_postgresql(Config) ->
    PgDataDir = ?config(datadir, Config),
    Utils = ?config(utils, Config),
    Postgres = ?config(postgres, Utils),

    PgHost = "localhost",
    PgPort = get_free_port(),
    SocketDir = "/tmp",
    Command = lists:concat(
                [Postgres,
                 " -k ", SocketDir,
                 " -D ", PgDataDir,
                 " -h ", PgHost,
                 " -p ", PgPort]),
    ct:pal(info, ?HI_IMPORTANCE, "Starting Postgresql: `~s`", [Command]),
    Pid = proc_lib:spawn(fun() ->
        {ok, _, I} = exec:run_link(Command,
            [{stderr,
              fun(_, _, Msg) ->
                  ct:pal(info, "postgres: ~s", [Msg])
              end},
             {env, [{"LANGUAGE", "en"}]}]),
        loop(I)
    end),
    ConfigR = [
        {host, PgHost},
        {port, PgPort},
        {proc, Pid}
        | Config
    ],
    wait_postgresql_ready(SocketDir, ConfigR).

loop(I) ->
    receive
        stop -> exec:kill(I, 0);
        _ -> loop(I)
    end.

wait_postgresql_ready(SocketDir, Config) ->
    PgPort = ?config(port, Config),

    PgFile = lists:concat([".s.PGSQL.", PgPort]),
    Path = filename:join(SocketDir, PgFile),
    WaitUntil = ts_add(os:timestamp(), ?PG_TIMEOUT),
    case wait_(Path, WaitUntil) of
        true -> ok;
        false -> throw(<<"Postgresql init timeout">>)
    end,
    Config.

wait_(Path, Until) ->
    case file:read_file_info(Path) of
        {error, enoent} ->
            case os:timestamp() > Until of
                true -> false;
                _ ->
                    timer:sleep(300),
                    wait_(Path, Until)
            end;
        _ -> true
    end.

init_database(Config) ->
    Utils = ?config(utils, Config),
    Initdb = ?config(initdb, Utils),

    {ok, Cwd} = file:get_cwd(),
    PgDataDir = filename:append(Cwd, "datadir"),
    {ok, _} = exec:run(Initdb ++ " --locale en_US.UTF8 " ++ PgDataDir, [sync,stdout,stderr]),
    [{datadir, PgDataDir}|Config].

get_version(Config) ->
    Datadir = ?config(datadir, Config),
    VersionFile = filename:join(Datadir, "PG_VERSION"),
    {ok, VersionFileData} = file:read_file(VersionFile),
    VersionBin = list_to_binary(string:strip(binary_to_list(VersionFileData), both, $\n)),
    Version = lists:map(fun erlang:binary_to_integer/1,
                         binary:split(VersionBin, <<".">>, [global])),
    [{version, Version} | Config].

write_postgresql_config(Config) ->
    PgDataDir = ?config(datadir, Config),

    PGConfig = [
        "ssl = on\n",
        "ssl_ca_file = 'root.crt'\n",
        "lc_messages = 'en_US.UTF-8'\n",
        "fsync = off\n",
        "wal_level = 'logical'\n",
        "max_replication_slots = 15\n",
        "max_wal_senders = 15"
    ],
    FilePath = filename:join(PgDataDir, "postgresql.conf"),
    ok = file:write_file(FilePath, PGConfig),
    Config.

copy_certs(Config) ->
    PgDataDir = ?config(datadir, Config),

    Files = [
        {"epgsql.crt", "server.crt", 8#00660},
        {"epgsql.key", "server.key", 8#00600},
        {"root.crt", "root.crt", 8#00660},
        {"root.key", "root.key", 8#00660}
    ],
    lists:foreach(fun({From, To, Mode}) ->
        FromPath = filename:join(?TEST_DATA_DIR, From),
        ToPath = filename:join(PgDataDir, To),
        {ok, _} = file:copy(FromPath, ToPath),
        ok = file:change_mode(ToPath, Mode)
    end, Files),
    Config.

write_pg_hba_config(Config) ->
    PgDataDir = ?config(datadir, Config),
    Version = ?config(version, Config),

    User = os:getenv("USER"),
    PGConfig = [
        "local   all             ", User, "                              trust\n",
        "host    template1       ", User, "              127.0.0.1/32    trust\n",
        "host    ", User, "      ", User, "              127.0.0.1/32    trust\n",
        "hostssl postgres        ", User, "              127.0.0.1/32    trust\n",
        "host    epgsql_test_db1 ", User, "              127.0.0.1/32    trust\n",
        "host    epgsql_test_db1 epgsql_test             127.0.0.1/32    trust\n",
        "host    epgsql_test_db1 epgsql_test_md5         127.0.0.1/32    md5\n",
        "host    epgsql_test_db1 epgsql_test_cleartext   127.0.0.1/32    password\n",
        "hostssl epgsql_test_db1 epgsql_test_cert        127.0.0.1/32    cert clientcert=1\n",
        "host    template1       ", User, "              ::1/128    trust\n",
        "host    ", User, "      ", User, "              ::1/128    trust\n",
        "hostssl postgres        ", User, "              ::1/128    trust\n",
        "host    epgsql_test_db1 ", User, "              ::1/128    trust\n",
        "host    epgsql_test_db1 epgsql_test             ::1/128    trust\n",
        "host    epgsql_test_db1 epgsql_test_md5         ::1/128    md5\n",
        "host    epgsql_test_db1 epgsql_test_cleartext   ::1/128    password\n",
        "hostssl epgsql_test_db1 epgsql_test_cert        ::1/128    cert clientcert=1\n" |
        case Version >= [10] of
            true ->
                %% See
                %% https://www.postgresql.org/docs/10/static/release-10.html
                %% "Change how logical replication uses pg_hba.conf"
                ["host    epgsql_test_db1 epgsql_test_replication 127.0.0.1/32    trust\n",
                 %% scram auth method only available on PG >= 10
                 "host    epgsql_test_db1 epgsql_test_scram       127.0.0.1/32    scram-sha-256\n"];
            false ->
                ["host    replication     epgsql_test_replication 127.0.0.1/32    trust\n"]
        end
    ],
    FilePath = filename:join(PgDataDir, "pg_hba.conf"),
    ok = file:write_file(FilePath, PGConfig),
    [{user, User}|Config].

%% =============================================================================
%% Internal functions
%% =============================================================================

get_free_port() ->
    {ok, Listen} = gen_tcp:listen(0, []),
    {ok, Port} = inet:port(Listen),
    ok = gen_tcp:close(Listen),
    Port.

pipe(Funs, Config) ->
    lists:foldl(fun(F, S) -> F(S) end, Config, Funs).

ts_add({Mega, Sec, Micro}, Timeout) ->
    V = (Mega * 1000000 + Sec)*1000000 + Micro + Timeout * 1000,
    {V div 1000000000000,
     V div 1000000 rem 1000000,
     V rem 1000000}.
