-module(edwin).

-export([select/3]).
-export([select/2]).
-export([select/1]).
-export([update/2]).
-export([insert/2]).
-export([ex/1]).
-export([ex/2]).

select(Table) ->
    select(Table, []).
select(Table, Columns) ->
    select(Table, Columns, []).
select(Table, Columns, Where) ->
    edwin_sql:select(Table, Columns, Where).

update(Table, Args) ->
    update(Table, Args, []).
update(Table, Args, Where) ->
    edwin_sql:update(Table, Args, Where).

insert(Table, [{_,_} | _] = Query) when is_atom(Table)->
    edwin_sql:insert(Table, Query).

ex(SQL) ->
    ex(SQL, []).
ex(SQL, Args) ->
    STM = random_atom(5),
    emysql:prepare(STM, SQL),
    emysql:execute(pool0, STM, Args).

random_atom(Len) ->
    Chrs = list_to_tuple("abcdefghijklmnopqrstuvwxyz"),
    ChrsSize = size(Chrs),
    F = fun(_, R) ->
                [element(crypto:rand_uniform(Len, ChrsSize), Chrs) | R] end,
    list_to_atom(lists:foldl(F, "", lists:seq(1, Len))).
