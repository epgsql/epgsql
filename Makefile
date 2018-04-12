REBAR = rebar3
ERL_VSN = $(shell erl -eval 'erlang:display(erlang:system_info(otp_release)), halt().'  -noshell)

all: compile

compile: src/epgsql_errcodes.erl
	@$(REBAR) compile

clean:
	@$(REBAR) clean

src/epgsql_errcodes.erl:
	./generate_errcodes_src.sh > src/epgsql_errcodes.erl

test: compile
	@$(REBAR) do ct -v

dialyzer: compile
	@$(REBAR) dialyzer

elvis:
	@case "$(ERL_VSN)" in\
		"R16"*)\
			echo "Elvis is disabled on erl 16"\
			;;\
		*)\
			$(REBAR) as lint lint\
			;;\
	esac

.PHONY: all compile clean test dialyzer elvis
