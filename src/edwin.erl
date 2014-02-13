-module(edwin).

-export([select/3]).
-export([select/2]).
-export([select/4]).
-export([update/3]).
-export([insert/3]).
-export([delete/2]).
-export([delete/3]).
-export([ex/3]).
-export([ex/2]).

select(Pool, Table) ->
    select(Pool, Table, []).
select(Pool, Table, Columns) ->
    select(Pool, Table, Columns, []).
select(Pool, Table, Columns, Where) when is_atom(Table) ->
    {SQL, Data} = edwin_sql:select(Table, Columns, Where),
    case emysql_util:as_json(ex(Pool, SQL, Data)) of
        Result when length(Result) =:= 1 ->
            lists:flatten(Result);
        Result -> Result
    end.

update(Pool, Table, Args) ->
    update(Pool, Table, Args, []).
update(Pool, Table, Args, Where) when is_atom(Table) ->
    {SQL, Data} = edwin_sql:update(Table, Args, Where),
    ex(Pool, SQL, Data).

insert(Pool, Table, [{_,_} | _] = Query) when is_atom(Table)->
    {SQL, Data} = edwin_sql:insert(Table, Query),
    ex(Pool, SQL, Data).

delete(Pool, Table) ->
    delete(Pool, Table, []).
delete(Pool, Table, Where) when is_atom(Table) ->
    {SQL, Data} = edwin_sql:delete(Table, Where),
    ex(Pool, SQL, Data).

ex(Pool, SQL) ->
    ex(Pool, SQL, []).
ex(Pool, SQL, Data) when is_atom(Pool)->
    STM = random_atom(5),
    emysql:prepare(STM, SQL),
    emysql:execute(Pool, STM, Data).

random_atom(Len) ->
    Chrs = list_to_tuple("abcdefghijklmnopqrstuvwxyz"),
    ChrsSize = size(Chrs),
    F = fun(_, R) ->
                [element(crypto:rand_uniform(Len, ChrsSize), Chrs) | R] end,
    list_to_atom(lists:foldl(F, "", lists:seq(1, Len))).
