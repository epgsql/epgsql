NAME		:= epgsql
VERSION		:= $(shell git describe --always --tags)

ERL  		:= erl
ERLC 		:= erlc

# ------------------------------------------------------------------------

ERLC_FLAGS	:= -Wall -I include +debug_info

SRC			:= $(wildcard src/*.erl)
TESTS 		:= $(wildcard test/*.erl)
RELEASE		:= $(NAME)-$(VERSION).tar.gz

APPDIR		:= $(NAME)-$(VERSION)
BEAMS		:= $(SRC:src/%.erl=ebin/%.beam) 

compile: $(BEAMS) ebin/$(NAME).app

app: compile
	@mkdir -p $(APPDIR)/ebin
	@cp -r ebin/* $(APPDIR)/ebin/
	@cp -r include $(APPDIR)

release: app
	@tar czvf $(RELEASE) $(APPDIR)

clean:
	@rm -f ebin/*.beam
	@rm -f ebin/$(NAME).app
	@rm -f test/*.beam
	@rm -rf $(NAME)-$(VERSION) $(NAME)-*.tar.gz

create_testdbs:
	psql template1 < ./test_data/test_schema.sql

test: $(TESTS:test/%.erl=test/%.beam) compile
	$(ERL) -pa ebin/ -pa deps/uuid/ebin -pa test/ -noshell -s pgsql_tests run_tests -s init stop

# ------------------------------------------------------------------------

.SUFFIXES: .erl .beam
.PHONY:    app compile clean test

ebin/%.beam : src/%.erl
	$(ERLC) $(ERLC_FLAGS) -o $(dir $@) $<

ebin/%.app : src/%.app.src Makefile
	sed -e 's|git|\"$(VERSION)\"|g' $< > $@

test/%.beam : test/%.erl
	$(ERLC) $(ERLC_FLAGS) -o $(dir $@) $<

dialyzer: build.plt compile
	dialyzer --plt $< ebin

build.plt:
	dialyzer -q --build_plt --apps kernel stdlib ssl --output_plt $@
