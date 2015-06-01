#!/usr/bin/env awk -f
BEGIN {
    print "-module(epgsql_errcodes)."
    print "-export([to_name/1])."
    print
}
NF == 4 && \
$1 ~ /[^\s]{5}/ && \
$2 ~ /[EWS]/ \
{
    printf("to_name(<<\"%s\">>) -> %s;\n", $1, $4)
}
END {
    print "to_name(_) -> undefined."
}
