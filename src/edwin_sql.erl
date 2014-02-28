-module(edwin_sql).

-export([select/3]).
-export([update/3]).
-export([delete/2]).
-export([insert/2]).
-export([call/2]).

-define(COMMA, ", ").
-define(ARG, " = ?").
-define(Q, "?").
-define(SELECT, "SELECT ").
-define(UPDATE, "UPDATE ").
-define(INSERT, "INSERT INTO ").
-define(FROM, " FROM ").
-define(DELETE, "DELETE FROM ").
-define(SET, " SET ").
-define(VALS, ") VALUES (").
-define(BKTLS, " (").
-define(BKTRS, " )").
-define(BKTL, "(").
-define(BKTR, ")").
-define(STAR, "*").
-define(WHERE, " WHERE ").
-define(AND, " AND ").
-define(IN(H), if H =:= notin -> " NOT IN "; true -> " IN " end).
-define(DATA(P), [V || {_,V} <- P]).
-define(CALL, "CALL ").

select(Table, Columns, Where) ->
    SQL= ?SELECT ++ columns(Columns) ++ ?FROM ++ to_l(Table) ++ where_conditions(Where),
    {SQL, prepare_args(Where)}.

update(Table, Args, Where) ->
    SQL = ?UPDATE ++ to_l(Table) ++ ?SET ++ cols(Args) ++ where_conditions(Where),
    {SQL, ?DATA(Args) ++ ?DATA(Where)}.

delete(Table, Where) ->
    SQL = ?DELETE ++ to_l(Table) ++ where_conditions(Where),
    {SQL, ?DATA(Where)}.

insert(Table, Args) ->
    Args1 = [{K, V} || {K, V} <- Args, V =/= undefined],
    SQL = ?INSERT ++ to_l(Table) ++ ?BKTLS ++ columns(Args) ++ ?VALS ++ defs(Args) ++ ?BKTR,
    {SQL, ?DATA(Args1)}.

call(Proc, Args) when is_atom(Proc) ->
    ?CALL ++ to_l(Proc) ++ ?BKTL ++ args(Args) ++ ?BKTR.

columns(C) when C =:= []; C =:= ?STAR ->
    ?STAR;
columns([{_,_} | _] = C) ->
    string:join([to_l(K) || {K, _} <- C], ?COMMA);
columns(C) ->
    string:join([to_l(S) || S <- C], ?COMMA).

defs(Columns) ->
    string:join([?Q || _ <- lists:seq(1, length(Columns))], ?COMMA).

args(Args) ->
    string:join([to_l(A) || A <- Args], ?COMMA).

to_l(A) when is_atom(A) -> atom_to_list(A);
to_l(B) when is_binary(B) -> binary_to_list(B);
to_l(I) when is_integer(I) -> integer_to_list(I);
to_l(L) when is_list(L) -> L.

prepare_args(Args) ->
    [V1 || V1 <- lists:flatten([V || {_, V} <- Args]), V1 =/= notin].

cols([{_,_} | _] = Query) ->
    string:join(lists:zipwith(fun({K, _}, _I) -> to_l(K) ++ ?ARG end, Query, lists:seq(1, length(Query))), ?COMMA).

where_conditions([]) ->
    "";
where_conditions([{_,_} | _] = Selector) ->
    ReqCols = lists:zipwith(fun({K, V}, _I) ->
                                    if
                                        is_list(V) ->
                                            [H | _] = V,
                                            to_l(K) ++ ?IN(H) ++ ?BKTL ++ defs(V) ++ ?BKTR;
                                        true ->
                                            to_l(K) ++ ?ARG
                                    end
                            end, Selector, lists:seq(1, length(Selector))),
    ?WHERE ++ string:join(ReqCols, ?AND).
