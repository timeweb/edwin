-module(edwin_sql).

-export([select/4]).
-export([update/4]).
-export([replace/2]).
-export([delete/2]).
-export([multipleDelete/2]).
-export([insert/2]).
-export([call/2]).
-export([fn/2]).

-define(COMMA, ", ").
-define(ARG, " = ?").
-define(Q, "?").
-define(SELECT, "SELECT ").
-define(UPDATE, "UPDATE ").
-define(REPLACE, "REPLACE ").
-define(INSERT, "INSERT INTO ").
-define(FROM, " FROM ").
-define(DELETE, "DELETE FROM ").
-define(MULTIPLE_DELETE, "DELETE ").
-define(SET, " SET ").
-define(VALS, ") VALUES (").
-define(BKTLS, " (").
-define(BKTRS, " )").
-define(BKTL, "(").
-define(BKTR, ")").
-define(STAR, "*").
-define(WHERE, " WHERE ").
-define(AND, " AND ").
-define(OR, " OR ").
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
-define(ORDER, " ORDER BY ").
-define(DESC, " DESC ").
-define(ASC, " ASC ").
-define(GROUP, " GROUP BY ").
-define(LIMIT, " LIMIT ").
-define(OFFSET, " OFFSET ").

select(Table, Columns, Where, Opts) ->
    {Conditions, Args} = parse_conditions(Where, Table),
    Options = make_opts(Opts, Table),
    SQL= ?SELECT ++ columns_as(Columns, Table) ++ ?FROM ++ bt(Table) ++ Conditions ++ Options,
    {SQL, Args}.


update(Table, Args, Where, Opts) ->
    {Conditions, WhereArgs} = parse_conditions(Where, Table),
    {Cols, UpdateArgs} = cols(Args, Table),
    Options = make_opts(Opts, Table), 
    SQL = ?UPDATE ++ bt(Table) ++ ?SET ++ Cols ++ Conditions ++ Options,
    {SQL, UpdateArgs ++ WhereArgs}.

replace(Table, Args) ->
    Args1 = [{K, V} || {K, V} <- Args, V =/= undefined],
    SQL = ?REPLACE ++ to_l(Table) ++ ?BKTLS ++ columns(Args) ++ ?VALS ++ defs(Args) ++ ?BKTR,
    {SQL, ?DATA(Args1)}.

delete(Table, Where) ->
    {Conditions, Args} = parse_conditions(Where, Table),
    SQL = ?DELETE ++ bt(Table) ++ Conditions,
    {SQL, Args}.

multipleDelete([Table | _JoinedTables] = Tables, Where) ->
    {Conditions, Args} = parse_conditions(Where, Table),
    TableList = [bt(Tbl) || Tbl <- Tables],
    SQL = io_lib:format(<<"~s~s~s~s~s">>, [?MULTIPLE_DELETE, string:join(TableList, ?COMMA), ?FROM, Table, Conditions]),
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


cols(Query, TableName) ->
   cols(Query, {[], []}, TableName).
cols([], {Compiled, Values}, _TableName) ->
   {string:join(lists:reverse(Compiled), ?COMMA), lists:map(fun v/1, lists:reverse(Values))};
cols([{Key, {{AsIs, Params}}}|Rest], {Compiled, Values}, TableName) when is_list(Params) ->
    cols(Rest, {[clmn(Key, TableName) ++ ?EQ ++ to_l(AsIs)|Compiled], lists:reverse(Params) ++ Values}, TableName);
cols([{Key, {{AsIs, Param}}}|Rest], {Compiled, Values}, TableName) ->
    cols(Rest, {[clmn(Key, TableName) ++ ?EQ ++ to_l(AsIs)|Compiled], [Param|Values]}, TableName);
cols([{Key, {AsIs}}|Rest], {Compiled, Values}, TableName) ->
    cols(Rest, {[clmn(Key, TableName) ++ ?EQ ++ to_l(AsIs)|Compiled], Values}, TableName);
cols([{Key, Value}|Rest], {Compiled, Values}, TableName) ->
    cols(Rest, {[clmn(Key, TableName) ++ ?ARG|Compiled], [Value|Values]}, TableName).
    

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
parse_conditions([{'any equal', {Keys, Value}}|Rest], Join, Where) when is_list(Keys) ->
    ConditionList = [{atom_to_list(Key), Value} || Key <- Keys],
    parse_conditions(Rest, Join, [{'any equal', ConditionList}|Where]);
parse_conditions([{Key, {Operator, Value}}|Rest], Join, Where) ->
    parse_conditions(Rest, Join, [{Key, Operator, Value}|Where]);
parse_conditions([{Key, {between, Values}}|Rest], Join, Where) when is_list(Values) ->
  parse_conditions(Rest, Join, [{Key, between, Values}|Where]);
parse_conditions([{Key, true}|Rest], Join, Where) ->
    parse_conditions(Rest, Join, [{Key, '=', 1}|Where]);
parse_conditions([{Key, false}|Rest], Join, Where) ->
    parse_conditions(Rest, Join, [{Key, '=', 0}|Where]);
parse_conditions([{Key, null}|Rest], Join, Where) ->
    parse_conditions(Rest, Join, [{Key, 'IS', {<<"NULL">>}}|Where]);
parse_conditions([{Key, not_null}|Rest], Join, Where) ->
    parse_conditions(Rest, Join, [{Key, 'IS', {<<"NOT NULL">>}}|Where]);
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
    case string:tokens(to_l(Joiner),?DOT) of
        [Table, Column] ->
            Join = to_l(Type) ++ ?JOIN ++ bt(Table) ++ ?ON ++ clmn(Key, TableName) ++ ?EQ ++ bt(Table) ++ ?DOT ++ bt(Column);
        [DB, Table, Column] ->
            Join = to_l(Type) ++ ?JOIN ++ bt(DB) ++ ?DOT ++ bt(Table) ++ ?ON ++ clmn(Key, TableName) ++ ?EQ ++ bt(Table) ++ ?DOT ++ bt(Column)
    end,
    compile_join(Rest, [Join|Compiled], TableName).

compile_where(Stack, TableName) ->
    compile_where(Stack, {[], []}, TableName).
compile_where([], {[], []}, _TableName) ->
    {"", []};
compile_where([], {Compiled,Values}, _TableName) ->
    {?WHERE ++ string:join(Compiled, ?AND), lists:reverse(Values)};
compile_where([{Operation, Statements}|Rest], {Compiled, Values}, TableName) ->
    {FieldStatement, ValueList} = case Operation of
        'any equal' ->
            FieldList = [Field ++ ?EQ ++ ?Q || {Field, _Value} <- Statements],
            ValueList = [Value || {_Field, Value} <- Statements],
            {?BKTL ++ string:join(FieldList, ?OR) ++ ?BKTR, ValueList}
    end,
    compile_where(Rest, {[FieldStatement|Compiled], lists:append(Values, ValueList)}, TableName);
compile_where([{Key, 'OR', Statements}|Rest], {Compiled, Values}, TableName) ->
    CondList = [clmn(Key, TableName) ++ Op ++ ?Q || {Op, _Value} <- Statements],
    ValueList = lists:reverse([Value || {_Op, Value} <- Statements]),
    FieldStatement = ?BKTL ++ string:join(CondList, ?OR) ++ ?BKTR,
    compile_where(Rest, {[FieldStatement|Compiled], lists:append(Values, ValueList)}, TableName);
compile_where([{Key, Op, Value}|Rest], {Compiled, Values}, TableName) ->
    {Args, AddValues} = case Value of
        {{Inline, Params}}  when is_list(Params) -> {to_l(Inline), lists:reverse(Params)};
        {{Inline, Param}} -> {to_l(Inline), [Param]};
        {Inline}  -> {to_l(Inline), []};
        Value when is_list(Value), Op =:= between -> {?Q ++ ?AND ++ ?Q, lists:reverse(Value)};
        Value when is_list(Value) -> {?BKTL ++ string:join([?Q || _ <- Value],?CMAS) ++ ?BKTR, Value};
        Value -> {?Q, [Value]}
    end,
    compile_where(Rest, {[clmn(Key, TableName) ++ spc(Op) ++ Args|Compiled], Values ++ AddValues}, TableName).

make_opts(Opts, DefaultTable) ->
    Result = case proplists:get_value(group, Opts) of
        undefined -> "";
        Group -> ?GROUP ++ clmn(Group, DefaultTable)
    end,
    Result1 = case proplists:get_value(order, Opts) of
        undefined -> Result;
        {TableName, Order, Direction} ->
            make_opts(maps:to_list(#{order => {Order, Direction}}), TableName);
        {Order, Direction} ->
            case Direction of
                asc -> Result ++ ?ORDER ++ clmn(Order, DefaultTable) ++ " ASC";
                desc -> Result ++ ?ORDER ++ clmn(Order, DefaultTable) ++ " DESC"
            end;
        Order -> Result ++ ?ORDER ++ clmn(Order, DefaultTable)
    end,
    Result2 = case proplists:get_value(limit, Opts) of
        undefined -> Result1;
        Limit -> Result1 ++ ?LIMIT ++ integer_to_list(Limit)
    end,
    Result3 = case proplists:get_value(offset, Opts) of
        undefined -> Result2;
        Offset -> Result2 ++ ?OFFSET ++ integer_to_list(Offset)
    end,
    Result3.


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
    to_l(AsIs).   

as({Column, As}, Table) ->
    clmn(Column, Table) ++ ?AS ++ bt(As);
as(Column, Table) ->
    clmn(Column, Table).

v(true) ->
    1;
v(false) ->
    0;
v(null) ->
    <<"NULL">>;
v(Value) ->
    Value.
