PROJECT = epgsql

# Options
ERLC_OPTS  = +debug_info +nowarn_shadow_vars +warnings_as_errors
PLT_APPS   = ssl # 'erts kernel stdlib' included by default by erlang.mk
EUNIT_OPTS = [verbose]

# Standard targets
include erlang.mk

.PHONY: create_testdbs test
create_testdbs:
	psql template1 < ./test_data/test_schema.sql

test: eunit

# eof
