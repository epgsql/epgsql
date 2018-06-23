REBAR = ./rebar3
ERL_VSN = $(shell erl -eval 'erlang:display(erlang:system_info(otp_release)), halt().'  -noshell)

all: compile

$(REBAR):
	@case "$(ERL_VSN)" in\
		"R16"*)\
			wget https://github.com/erlang/rebar3/releases/download/3.5.2/rebar3 \
			;;\
		*)\
			wget https://s3.amazonaws.com/rebar3/rebar3 \
			;;\
	esac
	chmod +x rebar3

compile: src/epgsql_errcodes.erl $(REBAR)
	@$(REBAR) compile

clean: $(REBAR)
	@$(REBAR) clean

src/epgsql_errcodes.erl:
	./generate_errcodes_src.sh > src/epgsql_errcodes.erl

test: compile
	@$(REBAR) do ct -v

dialyzer: compile
	@$(REBAR) dialyzer

elvis: $(REBAR)
	@case "$(ERL_VSN)" in\
		"R16"*)\
			echo "Elvis is disabled on erl 16"\
			;;\
		*)\
			$(REBAR) as lint lint\
			;;\
	esac

.PHONY: all compile clean test dialyzer elvis
