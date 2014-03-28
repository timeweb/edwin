-module(edwin_app).

-behaviour(application).

-export([start/2]).
-export([stop/1]).


start(_StartType, _StartArgs) ->
    edwin_sup:start_link().


stop(_State) ->
    ok.
