-module(edwin).

-include_lib("emysql/include/emysql.hrl").

-export([select/3]).
-export([select/2]).
-export([select/4]).
-export([update/3]).
-export([update/4]).
-export([insert/3]).
-export([delete/2]).
-export([delete/3]).
-export([ex/3]).
-export([ex/2]).
-export([call/3]).
-export([fn/3]).

select(Pool, Table) ->
    select(Pool, Table, []).

select(Pool, Table, Columns) when is_list(Columns) ->
    select(Pool, Table, Columns, #{});
select(Pool, Table, Columns) when is_integer(Columns) ->
    select(Pool, Table, [], Columns).

select(Pool, Table, Columns, Where) when is_integer(Where) ->
    select(Pool, Table, Columns, #{ id => Where });

select(Pool, Table, Columns, Where) when is_map(Where) ->
    {SQL, Data} = edwin_sql:select(Table, Columns, maps:to_list(Where)),
    ex(Pool, SQL, Data).

update(Pool, Table, Args) ->
    update(Pool, Table, Args, #{}).
update(Pool, Table, Args, Where) when is_integer(Where) ->
    update(Pool, Table, Args, #{ id => Where });
update(Pool, Table, Args, Where) when is_map(Where), is_map(Args) ->
    {SQL, Data} = edwin_sql:update(Table, maps:to_list(Args), maps:to_list(Where)),
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

ex(Pool, SQL) ->
    ex(Pool, SQL, []).
ex(Pool, SQL, Data) when is_atom(Pool)->
    StmtName = case edwin_st:get_stmt(SQL) of
                   null ->
                       StmtNew = random_atom(5),
                       emysql:prepare(StmtNew, SQL),
                       edwin_st:set_stmt(StmtNew, SQL),
                       StmtNew;
                   Stmt when is_atom(Stmt) ->
                       Stmt
               end,
    try
        ex(emysql:execute(Pool, StmtName, Data))
    catch
        exit:{Reason, _} ->
            {error, Reason}
    end.

ex(#ok_packet{insert_id = 0, affected_rows = AffectedRows}) ->
    {ok, AffectedRows};
ex(#ok_packet{insert_id = Id}) ->
    {ok, Id};
ex(#result_packet{} = Result) ->
    result(emysql_util:as_json(Result));
ex(#error_packet{} = Reason) ->
    {error, Reason};
ex(Result) ->
    Result.

call(Pool, Proc, Args) ->
    SQL = edwin_sql:call(Proc, Args),
    execute(Pool, SQL).

fn(Pool, Fun, Args) ->
    SQL = edwin_sql:fn(Fun, Args),
    ex(Pool, SQL).

execute(Pool, SQL) ->
    emysql:execute(Pool, SQL).

result(List) when length(List) =:= 1 ->
    maps:from_list(lists:flatten(List));
result(List) ->
    [maps:from_list(P) || P <- List].

random_atom(Len) ->
    Chrs = list_to_tuple("abcdefghijklmnopqrstuvwxyz"),
    ChrsSize = size(Chrs),
    F = fun(_, R) ->
                [element(crypto:rand_uniform(Len, ChrsSize), Chrs) | R] end,
    list_to_atom(lists:foldl(F, "", lists:seq(1, Len))).
