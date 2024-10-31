-record(copy,
        {
         %% pid of the process that started the COPY. It is used to receive asynchronous error
         %% messages when some error in data stream was detected
         initiator :: pid(),
         last_error :: undefined | epgsql:query_error(),
         format :: binary | text,
         binary_encoder :: epgsql_wire:copy_row_encoder() | undefined
        }).
