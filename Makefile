NAME		:= epgsql
VERSION		:= $(shell git describe --always --tags)

ERL         := erl
ERLC        := erlc

# ------------------------------------------------------------------------

ERLC_FLAGS  := -Wall -I include +debug_info

TESTS       := $(wildcard test_src/*.erl)

all: compile ebin/$(NAME).app

compile:
	rebar compile

ebin/%.qpp: src/%.app.src
	sed -e s/git/\"$(VERSION)\"/g $< > $@

clean:
	rebar clean

test: $(TESTS:test_src/%.erl=test_ebin/%.beam) compile
	@dialyzer -n --src -c src
	$(ERL) -config etc/epgsql_test.config -pa ebin/ -pa test_ebin/ -noshell -s pgsql_tests run_tests -s init stop

# ------------------------------------------------------------------------

.SUFFIXES: .erl .beam
.PHONY:    all compile clean test

test_ebin/%.beam : test_src/%.erl
	$(ERLC) $(ERLC_FLAGS) -o $(dir $@) $<
