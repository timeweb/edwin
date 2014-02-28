-module(edwin).

-include_lib("emysql/include/emysql.hrl").

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
    ex(Pool, SQL, Data).

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
    case emysql:execute(Pool, STM, Data) of
        #ok_packet{insert_id = ID} ->
            {ok, ID};
        #result_packet{} = Result ->
            as_p(emysql_util:as_json(Result));
        Result when length(Result) =:= 1 ->
            lists:flatten(Result);
        Result -> Result
    end.

call(Pool, Proc, Args) ->
    edwin_sql:call(Proc, Args).

as_p(P) ->
    case P of
        Proplist when length(Proplist) =:= 1 ->
            lists:flatten(Proplist);
        Proplist -> Proplist
    end.

random_atom(Len) ->
    Chrs = list_to_tuple("abcdefghijklmnopqrstuvwxyz"),
    ChrsSize = size(Chrs),
    F = fun(_, R) ->
                [element(crypto:rand_uniform(Len, ChrsSize), Chrs) | R] end,
    list_to_atom(lists:foldl(F, "", lists:seq(1, Len))).
