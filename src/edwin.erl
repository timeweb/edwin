-module(edwin).

-include_lib("emysql/include/emysql.hrl").

-export([select/2]).
-export([select/3, select_list/3]).
-export([select/4, select_list/4]).
-export([select/5, select_list/5]).
-export([select_last/4]).
-export([select_last/5]).
-export([update/3]).
-export([update/4]).
-export([update/5]).
-export([join_update/5]).
-export([replace/3]).
-export([insert/3]).
-export([delete/2]).
-export([delete/3]).
-export([multipleDelete/3]).
-export([ex/3]).
-export([ex/2]).
-export([execute/2]).
-export([call/3]).
-export([fn/3]).
-export([escape/1]).
-export([start_transaction/1]).
-export([rollback_transaction/1]).
-export([commit_transaction/1]).
-export([md5/1]).

-define(REPLACE_MAP, [
  {$1, "m"},
  {$2, "n"},
  {$3, "o"},
  {$4, "p"},
  {$5, "r"},
  {$6, "s"},
  {$7, "t"},
  {$8, "u"},
  {$9, "v"},
  {$0, "w"}
]).

select(Pool, Table) ->
  select(Pool, Table, []).

select(Pool, Table, Columns) when is_list(Columns) ->
  select(Pool, Table, Columns, #{});
select(Pool, Table, Where) when is_integer(Where) ->
  select(Pool, Table, [], Where).

select(Pool, Table, Columns, Where) when is_integer(Where) ->
  select(Pool, Table, Columns, #{id => Where});
select(Pool, Table, Columns, Where) when is_map(Where) ->
  select(Pool, Table, Columns, Where, #{}).

select(Pool, Table, Columns, Where, Opts) when is_map(Where), is_map(Opts) ->
  {SQL, Data} = edwin_sql:select(Table, Columns, maps:to_list(Where), maps:to_list(Opts)),
  ex(Pool, SQL, Data).

select_list(Pool, Table, Columns) ->
  select_list(select(Pool, Table, Columns)).

select_list(Pool, Table, Columns, Where) ->
  select_list(select(Pool, Table, Columns, Where)).

select_list(Pool, Table, Columns, Where, Opts) ->
  select_list(select(Pool, Table, Columns, Where, Opts)).

select_list(Data) when is_map(Data) ->
  [Data];
select_list(Data) ->
  Data.

select_last(Pool, Table, Columns, Where) ->
  select_last(Pool, Table, Columns, Where, id).

select_last(Pool, Table, Columns, Where, PK) ->
  Opts = #{limit => 1},
  select(Pool, Table, Columns, Where, maps:put(order, {PK, desc}, Opts)).

update(Pool, Table, Args) ->
  update(Pool, Table, Args, #{}).

update(Pool, Table, Args, Where) when is_integer(Where) ->
  update(Pool, Table, Args, #{ id => Where });
update(Pool, Table, Args, Where) ->
  update(Pool, Table, Args, Where, #{}).

update(Pool, Table, Args, Where, Opts) when is_atom(Table), is_map(Where), is_map(Args), is_map(Opts) ->
  {SQL, Data} = edwin_sql:update(Table, maps:to_list(Args), maps:to_list(Where), maps:to_list(Opts)),
  ex(Pool, SQL, Data).

join_update(Pool, Table, Join, Args, Where) when is_map(Join), is_map(Where), is_map(Args) ->
  {SQL, Data} = edwin_sql:join_update(Table, maps:to_list(Join), maps:to_list(Args), maps:to_list(Where)),
  ex(Pool, SQL, Data).

replace(Pool, Table, Values) ->
  {SQL, Data} = edwin_sql:replace(Table, maps:to_list(Values)),
  ex(Pool, SQL, Data).

insert(Pool, Table, Values) when is_atom(Table), is_map(Values) ->
  {SQL, Data} = edwin_sql:insert(Table, maps:to_list(Values)),
  ex(Pool, SQL, Data).


delete(Pool, Table) ->
  delete(Pool, Table, #{}).
delete(Pool, Table, Where) when is_integer(Where) ->
  delete(Pool, Table, #{ id => Where });
delete(Pool, Table, Where) when is_map(Where) ->
  {SQL, Data} = edwin_sql:delete(Table, maps:to_list(Where)),
  ex(Pool, SQL, Data).

multipleDelete(Pool, Tables, Where) ->
  {SQL, Data} = edwin_sql:multiple_delete(Tables, maps:to_list(Where)),
  ex(Pool, SQL, Data).

start_transaction(Pool) ->
  edwin_transaction:start(self(), Pool).

rollback_transaction(Pool) ->
  edwin_transaction:rollback(self(), Pool).

commit_transaction(Pool) ->
  edwin_transaction:commit(self(), Pool).

ex(Pool, SQL) ->
  ex(Pool, SQL, []).
ex(Pool, SQL, Data) when is_atom(Pool)->
  StmtName = sql_to_stmt(SQL),
  case emysql_statements:fetch(StmtName) of
    undefined ->
      emysql:prepare(StmtName, SQL);
    _  ->
      ok
  end,
  TransactionPool = edwin_transaction:match(self(), Pool),
  try
    ex(emysql:execute(TransactionPool, StmtName, Data))
  catch
    exit:{{Status, Msg}, _} ->
      erlang:error({edwin_error, #{status => Status, msg => Msg}});
    exit:pool_not_found ->
      erlang:error({error, #{msg => lists:flatten(io_lib:format("unknown pool `~s`.", [TransactionPool]))}})
  end.

ex(#ok_packet{insert_id = 0, affected_rows = AffectedRows}) ->
  {ok, AffectedRows};
ex(#ok_packet{insert_id = Id}) ->
  {ok, Id};
ex(#result_packet{} = Result) ->
  result(emysql_util:as_json(Result));
ex(#error_packet{} = Reason) ->
  erlang:error({edwin_error, #{status => Reason#error_packet.status,
    msg => Reason#error_packet.msg}});
ex(Result) ->
  Result.


call(Pool, Proc, Args) ->
  SQL = edwin_sql:call(Proc, Args),
  execute(Pool, SQL).


fn(Pool, Fun, Args) ->
  SQL = edwin_sql:fn(Fun, Args),
  ex(Pool, SQL, Args).


execute(Pool, SQL) ->
  TransactionPool = edwin_transaction:match(self(), Pool),
  ex(emysql:execute(TransactionPool, SQL)).


result(List) when length(List) =:= 1 ->
  maps:from_list([{binary_to_atom(K, utf8), V} || {K, V} <- lists:flatten(List)]);
result(List) ->
  [maps:from_list([{binary_to_atom(K, utf8), V} || {K, V} <- P]) || P <- List].

sql_to_stmt(SQL) ->
  Hash = md5(SQL),
  Stmt = lists:map(fun (E) -> proplists:get_value(E, ?REPLACE_MAP, E) end, Hash),
  list_to_atom(lists:flatten(Stmt)).

md5(Data) ->
  Binary16Base = crypto:hash(md5, Data),
  lists:flatten([io_lib:format("~2.16.0b", [B]) || <<B>> <= Binary16Base]).

escape(Str) ->
  binary:replace(Str, [<<"'">>], <<"\\">>, [global, {insert_replaced, 1}]).