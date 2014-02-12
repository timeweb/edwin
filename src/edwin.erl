-module(edwin).

-export([select/3]).
-export([select/2]).
-export([select/1]).
-export([update/2]).
-export([insert/2]).
-export([delete/1]).
-export([delete/2]).
-export([ex/1]).
-export([ex/2]).

select(Table) ->
    select(Table, []).
select(Table, Columns) ->
    select(Table, Columns, []).
select(Table, Columns, Where) ->
    {SQL, Data} = edwin_sql:select(Table, Columns, Where),
    emysql:execute(SQL, Data).

update(Table, Args) ->
    update(Table, Args, []).
update(Table, Args, Where) ->
    {SQL, Data} = edwin_sql:update(Table, Args, Where),
    emysql:execute(SQL, Data).

insert(Table, [{_,_} | _] = Query) when is_atom(Table)->
    {SQL, Data} = edwin_sql:insert(Table, Query),
    emysql:execute(SQL, Data).

delete(Table) ->
    delete(Table, []).
delete(Table, Where) ->
    {SQL, Data} = edwin_sql:delete(Table, Where),
    emysql:execute(SQL, Data).

ex(SQL) ->
    ex(SQL, []).
ex(SQL, Data) ->
    STM = random_atom(5),
    emysql:prepare(STM, SQL),
    emysql:execute(pool0, STM, Data).

random_atom(Len) ->
    Chrs = list_to_tuple("abcdefghijklmnopqrstuvwxyz"),
    ChrsSize = size(Chrs),
    F = fun(_, R) ->
                [element(crypto:rand_uniform(Len, ChrsSize), Chrs) | R] end,
    list_to_atom(lists:foldl(F, "", lists:seq(1, Len))).
