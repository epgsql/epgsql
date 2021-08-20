REBAR = ./rebar3
MINIMAL_COVERAGE = 55

all: compile

$(REBAR):
	wget https://github.com/erlang/rebar3/releases/download/3.15.2/rebar3
	chmod +x rebar3

compile: src/epgsql_errcodes.erl $(REBAR)
	@$(REBAR) compile

clean: $(REBAR)
	@$(REBAR) clean
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

elvis: $(REBAR)
	@$(REBAR) as lint lint

edoc: $(REBAR)
	@$(REBAR) edoc

.PHONY: all compile clean common-test eunit cover test dialyzer elvis
