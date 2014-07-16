REBAR = rebar

all: compile

compile:
	@$(REBAR) compile

clean:
	@$(REBAR) clean

create_testdbs:
	psql template1 < ./test_data/test_schema.sql

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
