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

dialyzer: build.plt compile
	dialyzer --plt $< ebin

build.plt:
	dialyzer -q --build_plt --apps kernel stdlib ssl --output_plt $@

.PHONY: all compile release clean create_testdbs test dialyzer build.plt
