-type epgsql_type() :: atom() | {array, atom()} | {unknown_oid, integer()}.

-record(column,    {name :: binary(),
                    type :: epgsql_type(),
                    size :: -1 | pos_integer(),
                    modifier :: -1 | pos_integer(),
                    format :: integer()}).

-record(statement, {name :: string(),
                    columns :: [#column{}],
                    types :: [epgsql_type()]}).

-record(error,  {severity :: fatal | error | atom(), %TODO: concretize
                 code :: binary(),
                 message :: binary(),
                 extra :: [{detail, binary()}
                           | {hint, binary()}
                           | {position, binary()}]}).

-type connection() :: pid().

-type connect_option() :: {database, string()}
                          | {port, inet:port_number()}
                          | {ssl, boolean() | required}
                          | {ssl_opts, list()} % ssl:option(), see OTP ssl_api.hrl
                          | {timeout, timeout()}
                          | {async, pid()}.
-type connect_error() :: #error{}.
-type query_error() :: #error{}.

-type bind_param() ::
        null
        | boolean()
        | string()
        | binary()
        | integer()
        | float()
        | calendar:date()
        | calendar:time()                       %actualy, `Seconds' may be float()
        | calendar:datetime()
        | {calendar:time(), Days::non_neg_integer(), Months::non_neg_integer()}
        | [bind_param()].                       %array (maybe nested)

-type squery_row() :: {binary()}.
-type equery_row() :: {bind_param()}.
-type ok_reply(RowType) :: {ok, [#column{}], [RowType]}  % SELECT
                         | {ok, non_neg_integer()}   % UPDATE / INSERT
                         | {ok, non_neg_integer(), [#column{}], [RowType]}. % UPDATE / INSERT + RETURNING
