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

ex(Pool, SQL) ->
  ex(Pool, SQL, []).
ex(Pool, SQL, Data) when is_atom(Pool)->
  StmtName = case edwin_st:get_stmt(SQL) of
               null ->
                 StmtNew = random_atom(10),
                 emysql:prepare(StmtNew, SQL),
                 edwin_st:set_stmt(StmtNew, SQL),
                 StmtNew;
               Stmt when is_atom(Stmt) ->
                 Stmt
             end,
  try
    ex(emysql:execute(Pool, StmtName, Data))
  catch
    exit:{{Status, Msg}, _} ->
      erlang:error({edwin_error, #{status => Status, msg => Msg}});
    exit:pool_not_found ->
      erlang:error({error, #{msg => lists:flatten(io_lib:format("unknown pool `~s`.", [Pool]))}})
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
  ex(emysql:execute(Pool, SQL)).


result(List) when length(List) =:= 1 ->
  maps:from_list([{binary_to_atom(K, utf8), V} || {K, V} <- lists:flatten(List)]);
result(List) ->
  [maps:from_list([{binary_to_atom(K, utf8), V} || {K, V} <- P]) || P <- List].


random_atom(Len) ->
  Chrs = list_to_tuple("abcdefghijklmnopqrstuvwxyz"),
  ChrsSize = size(Chrs),
  F = fun(_, R) ->
    [element(crypto:rand_uniform(Len, ChrsSize), Chrs) | R] end,
  list_to_atom(lists:foldl(F, "", lists:seq(1, Len))).
