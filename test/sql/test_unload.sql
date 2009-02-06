BEGIN;

DROP TABLE test_table;

DROP FUNCTION test();
DROP FUNCTION test(int);
DROP FUNCTION test_get(int);
DROP FUNCTION update_row(int, text);


COMMIT;
