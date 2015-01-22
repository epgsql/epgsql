REBAR = rebar
LASTVERSION = $(shell git rev-parse HEAD )

all: compile

compile:
	@$(REBAR) compile

clean:
	@$(REBAR) clean

# The INSERT is used to make sure the schema_version matches the tests
# being run.
create_testdbs:
	# Uses the test environment set up with setup_test_db.sh
	echo "CREATE DATABASE davidw;" | psql -h localhost -p 10432 template1
	psql -h localhost -p 10432 template1 < ./test_data/test_schema.sql
	psql -h localhost -p 10432 epgsql_test_db1 -c "INSERT INTO schema_version (version) VALUES ('${LASTVERSION}');"

test:
	@$(REBAR) eunit

performance_test: compile
	erlc ./test/epgsql_perf_tests.erl
	erl -noshell -pa ./ebin -eval "eunit:test(epgsql_perf_tests, [verbose])" -run init stop

dialyzer: build.plt compile
	dialyzer --plt $< ebin

build.plt:
	dialyzer -q --build_plt --apps erts kernel stdlib ssl --output_plt $@

.PHONY: all compile release clean create_testdbs performance_test test dialyzer
