REBAR=./rebar3

all: deps compile

deps: get-deps

compile:
	$(REBAR) compile

get-deps:
	$(REBAR) get-deps

clean:
	$(REBAR) clean

console:
	erl -pa deps/*/ebin -pa deps/*/include -pa ebin

eunit:
	$(REBAR) eunit skip_deps=true

ct:
	$(REBAR) ct skip_deps=true
