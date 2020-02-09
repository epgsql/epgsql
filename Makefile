REBAR = ./rebar3
MINIMAL_COVERAGE = 55

all: compile

$(REBAR):
	wget https://s3.amazonaws.com/rebar3/rebar3
	chmod +x rebar3

compile: src/epgsql_errcodes.erl $(REBAR)
	@$(REBAR) compile

clean: $(REBAR)
	@$(REBAR) clean

src/epgsql_errcodes.erl:
	./generate_errcodes_src.sh > src/epgsql_errcodes.erl

common-test:
	$(REBAR) ct -v -c

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
