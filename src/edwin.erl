-module(edwin).

-export([select/3]).
-export([select/2]).
-export([select/1]).
-export([update/1]).
-export([update/2]).
-export([insert/2]).
-export([ex/1]).
-export([ex/2]).

select(Table) ->
    select(Table, []).
select(Table, Columns) ->
    select(Table, Columns, []).
select(Table, Columns, Args) ->
    Columns1 = case Columns of
                   Cols when Cols =:= []; Cols =:= "*" ->
                       "*";
                   Cols ->
                       string:join([atom_to_list(L1) || L1 <- Cols], ", ")
               end,
    SQL1 = "SELECT " ++ Columns1 ++ " FROM " ++ atom_to_list(Table) ++ where_conditions(Args),
    Args1 = prepare_args(Args),
    {SQL1, Args1}.

prepare_args(Args) ->
    [V1 || V1 <- lists:flatten([V || {_K, V} <- Args]), V1 =/= notin].

update(SQL) ->
    update(SQL, []).
update(SQL, Args) ->
    Result = ex(SQL, Args),
    Rows = emysql_util:affected_rows(Result),
    {ok, Rows}.

insert(Table, [{_,_} | _] = Row) when is_atom(Table)->
    Row1 = [{K, V} || {K, V} <- Row, V =/= undefined],
    Columns = string:join([to_l(K) || {K, _} <- Row], ", "),
    Defs = string:join(["?" || _ <- lists:seq(1, length(Row1))], ", "),
    SQL = "INSERT INTO " ++ atom_to_list(Table) ++ " (" ++ Columns ++ ") VALUES (" ++ Defs ++ ")",
    Args = [V || {_,V} <- Row1],
    Result = ex(SQL, Args),
    ID = emysql_util:insert_id(Result),
    {ok, ID}.

to_l(A) when is_atom(A) -> atom_to_list(A);
to_l(B) when is_binary(B) -> binary_to_list(B);
to_l(L) when is_list(L) -> L.

where_conditions([]) ->
    "";
where_conditions([{_,_} | _] = Selector) ->
    ReqCols = lists:zipwith(fun({K, V}, _I) ->
                                    if
                                        is_list(V) ->
                                            [F | _] = V,
                                            R = if
                                                    F =:= notin -> " NOT IN ";
                                                    true -> " IN "
                                                end,
                                            atom_to_list(K) ++ R ++ "(" ++ string:join(["?" || _ <- lists:seq(1, length(V))], ", ")  ++")";
                                        true ->
                                            atom_to_list(K) ++ " = ?"
                                    end
                            end, Selector, lists:seq(1, length(Selector))),
    io:format("REQLOCS: ~p~n", [ReqCols]),
    " WHERE " ++ string:join(ReqCols, " AND ").

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
