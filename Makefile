NAME		:= epgsql
VERSION		:= $(shell git describe --always --tags)

all: compile ebin/$(NAME).app

compile:
	rebar compile

ebin/%.qpp: src/%.app.src
	sed -e s/git/\"$(VERSION)\"/g $< > $@

clean:
	rebar clean
