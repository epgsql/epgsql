REBAR = ./rebar3

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

test: compile
	@$(REBAR) do ct -v -c
	@$(REBAR) cover -v -m 55

dialyzer: compile
	@$(REBAR) dialyzer

elvis: $(REBAR)
	@$(REBAR) as lint lint

.PHONY: all compile clean test dialyzer elvis
