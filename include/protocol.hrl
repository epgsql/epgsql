-define(int16, 1/big-signed-unit:16).
-define(int32, 1/big-signed-unit:32).
-define(int64, 1/big-signed-unit:64).

%% Commands defined as per this page:
%% https://www.postgresql.org/docs/current/static/protocol-message-formats.html

%% Commands
-define(BIND, $B).
-define(CLOSE, $C).
-define(DESCRIBE, $D).
-define(EXECUTE, $E).
-define(FLUSH, $H).
-define(PASSWORD, $p).
-define(PARSE, $P).
-define(SIMPLEQUERY, $Q).
-define(AUTHENTICATION_REQUEST, $R).
-define(SYNC, $S).
-define(SASL_ANY_RESPONSE, $p).

%% Parameters

-define(PREPARED_STATEMENT, $S).
-define(PORTAL, $P).

%% Responses

-define(PARSE_COMPLETE, $1).
-define(BIND_COMPLETE, $2).
-define(CLOSE_COMPLETE, $3).
-define(NOTIFICATION, $A).
-define(COMMAND_COMPLETE, $C).
-define(DATA_ROW, $D).
-define(ERROR, $E).
-define(EMPTY_QUERY, $I).
-define(CANCELLATION_KEY, $K).
-define(NO_DATA, $n).
-define(NOTICE, $N).
-define(PORTAL_SUSPENDED, $s).
-define(PARAMETER_STATUS, $S).
-define(PARAMETER_DESCRIPTION, $t).
-define(ROW_DESCRIPTION, $T).
-define(READY_FOR_QUERY, $Z).
-define(COPY_BOTH_RESPONSE, $W).
-define(COPY_DATA, $d).
-define(TERMINATE, $X).

% CopyData replication messages
-define(X_LOG_DATA, $w).
-define(PRIMARY_KEEPALIVE_MESSAGE, $k).
-define(STANDBY_STATUS_UPDATE, $r).
