-module(edwin_st).

-behaviour(gen_server).

-export([start_link/0]).
-export([init/1]).
-export([handle_call/3]).
-export([handle_cast/2]).
-export([handle_info/2]).
-export([terminate/2]).
-export([code_change/3]).
-export([get_stmt/1]).
-export([set_stmt/2]).


start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, #{}, []).


get_stmt(SQL) ->
    gen_server:call(?MODULE, {get_stmt, SQL}).


set_stmt(Stmt, SQL) ->
    gen_server:call(?MODULE, {set_stmt, Stmt, SQL}).


init(State) ->
    ets:new(?MODULE, [set, private, named_table]),
    {ok, State}.


handle_call({get_stmt, SQL}, _From, State) ->
    Stmt = case ets:member(?MODULE, SQL) of
               true ->
                   ets:lookup_element(?MODULE, SQL, 2);
               false ->
                   null
           end,
    {reply, Stmt, State};
handle_call({set_stmt, Name, SQL}, _From, State) ->
    ets:insert(?MODULE, {SQL, Name}),
    {reply, ok, State};
handle_call(_Request, _From, State) ->
    {reply, null, State}.


handle_cast(_Request, State) ->
    {noreply, State}.


handle_info(_Info, State) ->
    {noreply, State}.


terminate(_Reason, _State) ->
    ok.


code_change(_OldVsn, State, _Extra) ->
    {ok, State}.
