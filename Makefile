REBAR = rebar3
MINIMAL_COVERAGE = 55

all: compile

compile: src/epgsql_errcodes.erl
	@$(REBAR) compile

clean: $(REBAR) clean_doc
	@$(REBAR) clean

clean_doc:
	@rm -f doc/*.html
	@rm -f doc/erlang.png
	@rm -f doc/stylesheet.css
	@rm -f doc/edoc-info

src/epgsql_errcodes.erl:
	./generate_errcodes_src.sh > src/epgsql_errcodes.erl

common-test:
	$(REBAR) ct --readable true -c

eunit:
	$(REBAR) eunit -c

# Fail the build if coverage falls below 55%
cover:
	$(REBAR) cover -v --min_coverage $(MINIMAL_COVERAGE)

test: compile eunit common-test cover

dialyzer: compile
	@$(REBAR) dialyzer

elvis:
	@$(REBAR) as lint lint

edoc:
	@$(REBAR) edoc

.PHONY: all compile clean common-test eunit cover test dialyzer elvis
