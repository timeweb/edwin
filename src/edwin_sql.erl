-module(edwin_sql).

-export([select/3]).
-export([update/3]).
-export([delete/2]).
-export([insert/2]).
-export([call/2]).
-export([fn/2]).

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
-define(IN, " IN ").
-define(DATA(P), [V || {_,V} <- P]).
-define(CALL, "CALL ").
-define(AS, " AS ").
-define(SPC, " ").
-define(EQ, " = ").
-define(BT, "`").
-define(JOIN, " JOIN ").
-define(ON, " ON ").
-define(DOT, ".").
-define(CMA, ",").
-define(CMAS, ", ").

select(Table, Columns, Where) ->
    {Conditions, Args} = parse_conditions(Where, Table),
    SQL= ?SELECT ++ columns_as(Columns, Table) ++ ?FROM ++ bt(Table) ++ Conditions,
    {SQL, Args}.


update(Table, Args, Where) ->
    {Conditions, WhereArgs} = parse_conditions(Where, Table),
    SQL = ?UPDATE ++ bt(Table) ++ ?SET ++ cols(Args) ++ Conditions,
    {SQL, ?DATA(Args) ++ WhereArgs}.


delete(Table, Where) ->
    {Conditions, Args} = parse_conditions(Where, Table),
    SQL = ?DELETE ++ bt(Table) ++ Conditions,
    {SQL, Args}.


insert(Table, Args) ->
    Args1 = [{K, V} || {K, V} <- Args, V =/= undefined],
    SQL = ?INSERT ++ to_l(Table) ++ ?BKTLS ++ columns(Args) ++ ?VALS ++ defs(Args) ++ ?BKTR,
    {SQL, ?DATA(Args1)}.


call(Proc, Args) when is_atom(Proc) ->
    ?CALL ++ to_l(Proc) ++ ?BKTL ++ args_with_type(Args) ++ ?BKTR.


fn(Fun, Args) when is_atom(Fun), is_list(Args) ->
    ?SELECT ++ to_l(Fun) ++ ?BKTL ++ defs(Args) ++ ?BKTR ++ ?AS ++ "result".


columns(C) when C =:= []; C =:= ?STAR ->
    ?STAR;
columns([{_,_} | _] = C) ->
    string:join([bt(K) || {K, _} <- C], ?COMMA);
columns(C) ->
    string:join([bt(S) || S <- C], ?COMMA).


columns_as(C, _Table) when C =:= []; C =:= ?STAR ->
    ?STAR;
columns_as(C, Table) ->
    string:join([as(S, Table) || S <- C], ?COMMA).
        

defs(Columns) ->
    string:join([?Q || _ <- lists:seq(1, length(Columns))], ?COMMA).

args_with_type(Args) ->
    args_with_type(Args, []).

args_with_type([], Acc) ->
    Acc;
args_with_type([Arg | Args], [] = Acc) when is_list(Arg); is_binary(Arg) ->
    args_with_type(Args, Acc ++ "'" ++ to_l(Arg) ++ "'");
args_with_type([Arg | Args], Acc) when is_list(Arg); is_binary(Arg) ->
    args_with_type(Args, Acc ++ ?COMMA ++ "'" ++ to_l(Arg) ++ "'");
args_with_type([Arg | Args], [] = Acc) ->
    args_with_type(Args, Acc  ++ to_l(Arg));
args_with_type([Arg | Args], Acc) ->
    args_with_type(Args, Acc ++ ?COMMA ++ to_l(Arg)).


to_l(A) when is_atom(A) -> atom_to_list(A);
to_l(B) when is_binary(B) -> binary_to_list(B);
to_l(I) when is_integer(I) -> integer_to_list(I);
to_l(L) when is_list(L) -> L.


cols([{_,_} | _] = Query) ->
    string:join(lists:zipwith(fun({K, _}, _I) -> bt(K) ++ ?ARG end, Query, lists:seq(1, length(Query))), ?COMMA).


parse_conditions(Conditions, TableName) ->
    {Join, Where} = parse_conditions(Conditions, [], []),
    CompiledJoin = compile_join(Join, TableName),
    {CompiledWhere, Values} = compile_where(Where, TableName),
    {CompiledJoin ++ CompiledWhere, Values}.
parse_conditions([], Join, Where) ->
    {Join, Where};
parse_conditions([{Key, {Type, Joiner}}|Rest], Join, Where) when is_atom(Joiner) ->
    parse_conditions(Rest, [{Key, Type, Joiner}|Join], Where);
parse_conditions([{Key, {'NOT IN', Values}}|Rest], Join, Where) when is_list(Values) ->
    parse_conditions(Rest, Join, [{Key, 'NOT IN', Values}|Where]);
parse_conditions([{Key, {Operator, Value}}|Rest], Join, Where) ->
    parse_conditions(Rest, Join, [{Key, Operator, Value}|Where]);
parse_conditions([{Key, true}|Rest], Join, Where) ->
    parse_conditions(Rest, Join, [{Key, '=', 1}|Where]);
parse_conditions([{Key, false}|Rest], Join, Where) ->
    parse_conditions(Rest, Join, [{Key, '=', 0}|Where]);
parse_conditions([{Key, Joiner}|Rest], Join, Where) when is_atom(Joiner) ->
    parse_conditions(Rest, [{Key, '', Joiner}|Join], Where);
parse_conditions([{Key, Values}|Rest], Join, Where) when is_list(Values) ->
    parse_conditions(Rest, Join, [{Key, 'IN', Values}|Where]);
parse_conditions([{Key, Value}|Rest], Join, Where) ->
    parse_conditions(Rest, Join, [{Key, '=', Value}|Where]).
    
compile_join(Stack, TableName) ->
    compile_join(Stack, [], TableName).
compile_join([], Compiled, _TableName) ->
    string:join(Compiled,?SPC);
compile_join([{Key, Type, Joiner}|Rest], Compiled, TableName) ->
    [Table, Column] = string:tokens(to_l(Joiner),?DOT),
    Join = to_l(Type) ++ ?JOIN ++ bt(Table) ++ ?ON ++ bt(TableName) ++ ?DOT ++ bt(Key) ++ ?EQ ++ bt(Table) ++ ?DOT ++ bt(Column),
    compile_join(Rest, [Join|Compiled], TableName).

compile_where(Stack, TableName) ->
    compile_where(Stack, {[], []}, TableName).
compile_where([], {[], []}, _TableName) ->
    {"", []};
compile_where([], {Compiled,Values}, _TableName) ->
    {?WHERE ++ string:join(Compiled, ?AND), lists:reverse(Values)};
compile_where([{Key, Op, Value}|Rest], {Compiled, Values}, TableName) ->
    {Args, AddValues} = case Value of
        Value when is_list(Value) -> {?BKTL ++ string:join([?Q || _ <- Value],?CMAS) ++ ?BKTR, Value};
        Value -> {?Q, [Value]}
    end,
    compile_where(Rest, {[clmn(Key, TableName) ++ spc(Op) ++ Args|Compiled], Values ++ AddValues}, TableName).


bt(Word) ->
    ?BT ++ to_l(Word) ++ ?BT.

spc(Word) ->
    ?SPC ++ to_l(Word) ++ ?SPC.

clmn(Clmn, DefaultTable) when is_atom(Clmn) ->
    case string:tokens(to_l(Clmn),?DOT) of
        [Column] -> bt(DefaultTable) ++ ?DOT ++ bt(Column);
        [Table, Column] -> bt(Table) ++ ?DOT ++ bt(Column)
    end;
clmn(AsIs, _DefaultTable) ->
    AsIs.   

as({Column, As}, Table) ->
    clmn(Column, Table) ++ ?AS ++ bt(As);
as(Column, Table) ->
    clmn(Column, Table).
