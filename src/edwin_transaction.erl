-module(edwin_transaction).

-behaviour(gen_server).

-include_lib("emysql/include/emysql.hrl").

%% API
-export([
  start_link/0,
  start/2,
  match/2,
  rollback/2,
  commit/2
  ]).

%% gen_server callbacks
-export([init/1,
  handle_call/3,
  handle_cast/2,
  handle_info/2,
  terminate/2,
  code_change/3]).

-define(SERVER, ?MODULE).
-define(TRANSACTION_POOL_LIFETIME, 60000).

-record(state, {transactions = [] :: list()}).

%%%===================================================================
%%% API
%%%===================================================================

start(Owner, ParentPool) ->
  {ok, PoolEnt} = get_pool(ParentPool),
  NewPoolId = list_to_atom(atom_to_list(ParentPool) ++ "_" ++ edwin:md5(crypto:rand_bytes(32))),
  put({transaction_id, ParentPool}, NewPoolId),
  emysql:add_pool(
    NewPoolId,
    1,
    PoolEnt#pool.user,
    PoolEnt#pool.password,
    PoolEnt#pool.host,
    PoolEnt#pool.port,
    PoolEnt#pool.database,
    PoolEnt#pool.encoding
  ),
  {ok, 0} = edwin:execute(NewPoolId, "START TRANSACTION"),
  gen_server:cast(?SERVER, {add, {Owner, ParentPool, NewPoolId}}),
{ok, NewPoolId}.

match(Owner, ParentPool) ->
  case gen_server:call(?SERVER, {get, {Owner, ParentPool}}) of
    undefined -> ParentPool;
    TransactionPool -> TransactionPool
  end.

rollback(Owner, ParentPool) ->
  edwin:execute(ParentPool, "ROLLBACK"),
  gen_server:cast(?SERVER, {clean, {Owner, ParentPool}}),
  ok.

commit(Owner, ParentPool) ->
  edwin:execute(ParentPool, "COMMIT"),
  gen_server:cast(?SERVER, {clean, {Owner, ParentPool}}),
  ok.

-spec(start_link() ->
  {ok, Pid :: pid()} | ignore | {error, Reason :: term()}).
start_link() ->
  gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

-spec(init(Args :: term()) ->
  {ok, State :: #state{}} | {ok, State :: #state{}, timeout() | hibernate} |
  {stop, Reason :: term()} | ignore).
init([]) ->
  {ok, #state{}}.

-spec(handle_call(Request :: term(), From :: {pid(), Tag :: term()},
    State :: #state{}) ->
  {reply, Reply :: term(), NewState :: #state{}} |
  {reply, Reply :: term(), NewState :: #state{}, timeout() | hibernate} |
  {noreply, NewState :: #state{}} |
  {noreply, NewState :: #state{}, timeout() | hibernate} |
  {stop, Reason :: term(), Reply :: term(), NewState :: #state{}} |
  {stop, Reason :: term(), NewState :: #state{}}).
handle_call({get, {Owner, ParentPool}}, _From, #state{transactions = TransactionsList} = State) ->
  {reply, proplists:get_value({Owner, ParentPool}, TransactionsList), State};
handle_call(_Request, _From, State) ->
  {reply, ok, State}.

-spec(handle_cast(Request :: term(), State :: #state{}) ->
  {noreply, NewState :: #state{}} |
  {noreply, NewState :: #state{}, timeout() | hibernate} |
  {stop, Reason :: term(), NewState :: #state{}}).
handle_cast({add, {Owner, ParentPool, NewPoolId}}, #state{transactions = TransactionsList0} = State) ->
  TransactionsList = proplists:delete({Owner, ParentPool}, TransactionsList0),
  timer:send_after(?TRANSACTION_POOL_LIFETIME, self(), {clean, {Owner, ParentPool}}),
  {noreply, State#state{transactions = [{{Owner, ParentPool}, NewPoolId} | TransactionsList]}};
handle_cast({clean, {Owner, ParentPool}}, #state{transactions = TransactionsList0} = State) ->
  TransactionsList = clean(Owner, ParentPool, TransactionsList0),
  {noreply, State#state{transactions = TransactionsList}};
handle_cast(_Request, State) ->
  {noreply, State}.

-spec(handle_info(Info :: timeout() | term(), State :: #state{}) ->
  {noreply, NewState :: #state{}} |
  {noreply, NewState :: #state{}, timeout() | hibernate} |
  {stop, Reason :: term(), NewState :: #state{}}).
handle_info({clean, {Owner, ParentPool}}, #state{transactions = TransactionsList0} = State) ->
  TransactionsList = clean(Owner, ParentPool, TransactionsList0),
  {noreply, State#state{transactions = TransactionsList}};
handle_info(_Info, State) ->
  {noreply, State}.

-spec(terminate(Reason :: (normal | shutdown | {shutdown, term()} | term()),
    State :: #state{}) -> term()).
terminate(_Reason, _State) ->
  ok.

-spec(code_change(OldVsn :: term() | {down, term()}, State :: #state{},
    Extra :: term()) ->
  {ok, NewState :: #state{}} | {error, Reason :: term()}).
code_change(_OldVsn, State, _Extra) ->
  {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

get_pool(Pool) ->
  Pools = emysql_conn_mgr:pools(),
  case lists:filter(fun (#pool{pool_id = PoolId}) -> Pool =:= PoolId end,Pools) of
    [] -> {error, noent};
    [Res] -> {ok, Res}
  end.

clean(Owner, ParentPool, TransactionsList0) ->
  case proplists:get_value({Owner, ParentPool}, TransactionsList0) of
     undefined ->
       TransactionsList0;
     TransactionPoolId ->
       case emysql_conn_mgr:has_pool(TransactionPoolId) of
         false -> ok;
         true -> emysql_conn_mgr:remove_pool(TransactionPoolId)
       end,
       proplists:delete({Owner, ParentPool}, TransactionsList0)
   end.
