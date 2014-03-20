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
    STM = random_atom(5),
    emysql:prepare(STM, SQL),
    case emysql:execute(Pool, STM, Data) of
        #ok_packet{insert_id = ID} ->
            {ok, ID};
        #result_packet{} = Result ->
            result(emysql_util:as_json(Result));
        Result ->
            Result
    end.

call(Pool, Proc, Args) ->
    SQL = edwin_sql:call(Proc, Args),
    execute(Pool, SQL).

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
