CREATE TABLE test_table (
    id int,
    value text
);

CREATE FUNCTION test () RETURNS setof test_table AS 
$body$
    SELECT * FROM test_table;
$body$ language sql;

CREATE FUNCTION test (int) RETURNS setof test_table AS 
$body$
    SELECT * FROM test_table where id = $1;
$body$ language sql;

CREATE FUNCTION test_get (int) RETURNS setof test_table AS 
$body$
    SELECT * FROM test_table where id = $1;
$body$ language sql;

INSERT INTO test_table values (1, 'one');
INSERT INTO test_table values (2, 'two');
INSERT INTO test_table values (3, 'three');

CREATE FUNCTION update_row (int, text) RETURNS boolean AS
$body$
    UPDATE test_table SET value =  $2 WHERE id = $1;
    SELECT TRUE;
$body$ LANGUAGE sql;
