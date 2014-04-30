PROJECT = epgsql

# Options
ERLC_OPTS  = +debug_info +nowarn_shadow_vars +warnings_as_errors
EUNIT_OPTS = [verbose]

# Standard targets
include erlang.mk

.PHONY: create_testdbs test
create_testdbs:
	psql template1 < ./test_data/test_schema.sql

test: eunit

# eof
