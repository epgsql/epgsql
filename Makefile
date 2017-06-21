REBAR = rebar3

all: compile

compile: src/epgsql_errcodes.erl
	@$(REBAR) compile

clean:
	@$(REBAR) clean

src/epgsql_errcodes.erl:
	./generate_errcodes_src.sh > src/epgsql_errcodes.erl

test: compile
	@$(REBAR) do ct

dialyzer: compile
	@$(REBAR) dialyzer

.PHONY: all compile clean test dialyzer
