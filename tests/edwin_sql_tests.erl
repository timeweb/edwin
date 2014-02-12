-module(edwin_sql_tests).

-include_lib("eunit/include/eunit.hrl").

select_test() ->
    R1 = edwin_sql:select(table, [id, name, status], [{type, <<"test">>}, {id, 5}]),
    ?assertEqual({"SELECT id, name, status FROM table WHERE type = ? AND id = ?", [<<"test">>, 5]}, R1),
    R2 = edwin_sql:select(table, [], []),
    ?assertEqual({"SELECT * FROM table", []}, R2),
    R3 = edwin_sql:select(table, [id, name], []),
    ?assertEqual({"SELECT id, name FROM table", []}, R3),
    R4 = edwin_sql:select(table, [id, name, status], [{type, <<"test">>}, {id, [1, 2, 3]}]),
    ?assertEqual({"SELECT id, name, status FROM table WHERE type = ? AND id IN (?, ?, ?)", [<<"test">>, 1, 2, 3]}, R4).

update_test() ->
    R1 = edwin_sql:update(table, [{name, <<"test">>}, {type, 2}], [{id, 4}]),
    ?assertEqual({"UPDATE table SET name = ?, type = ? WHERE id = ?", [<<"test">>, 2, 4]}, R1),
    R2 = edwin_sql:update(table, [{name, <<"test">>}, {type, 2}], []),
    ?assertEqual({"UPDATE table SET name = ?, type = ?", [<<"test">>, 2]}, R2).

delete_test() ->
    R1 = edwin_sql:delete(table, []),
    ?assertEqual({"DELETE FROM table", []}, R1),
    R2 = edwin_sql:delete(table, [{id, 3}]),
    ?assertEqual({"DELETE FROM table WHERE id = ?", [3]}, R2).

insert_test() ->
    R1 = edwin_sql:insert(table, [{name, <<"sql">>}, {type, 55}, {status, <<"off">>}]),
    ?assertEqual({"INSERT INTO table (name, type, status) VALUES (?, ?, ?)", [<<"sql">>, 55, <<"off">>]}, R1).
