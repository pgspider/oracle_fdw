-- ===================================================================
-- create FDW objects
-- ===================================================================
--Testcase 1:
SET client_min_messages = WARNING;
--Testcase 2:
CREATE EXTENSION oracle_fdw;

--Testcase 3:
CREATE SERVER oracle_srv FOREIGN DATA WRAPPER oracle_fdw OPTIONS (dbserver '', isolation_level 'read_committed', nchar 'true');
--Testcase 4:
CREATE SERVER oracle_srv2 FOREIGN DATA WRAPPER oracle_fdw OPTIONS (dbserver '', isolation_level 'read_committed', nchar 'true');
--Testcase 5:
CREATE SERVER oracle_srv3 FOREIGN DATA WRAPPER oracle_fdw OPTIONS (dbserver '', isolation_level 'read_committed', nchar 'true');

--Testcase 6:
CREATE USER MAPPING FOR CURRENT_USER SERVER oracle_srv OPTIONS (user 'test', password 'test');
--Testcase 7:
CREATE USER MAPPING FOR CURRENT_USER SERVER oracle_srv2 OPTIONS (user 'test', password 'test');
--Testcase 8:
CREATE USER MAPPING FOR CURRENT_USER SERVER oracle_srv3 OPTIONS (user 'test', password 'test');

-- ===================================================================
-- create objects used through FDW oracle_srv server
-- ===================================================================
--Testcase 9:
CREATE TYPE user_enum AS ENUM ('foo', 'bar', 'buz');

DO
$$BEGIN
--Testcase 10:
   SELECT oracle_execute('oracle_srv', 'DROP TABLE test."T 1" PURGE');
EXCEPTION
   WHEN OTHERS THEN
      NULL;
END;$$;

DO
$$BEGIN
--Testcase 11:
   SELECT oracle_execute('oracle_srv', 'DROP TABLE test."T 2" PURGE');
EXCEPTION
   WHEN OTHERS THEN
      NULL;
END;$$;

DO
$$BEGIN
--Testcase 12:
   SELECT oracle_execute('oracle_srv', 'DROP TABLE test."T 3" PURGE');
EXCEPTION
   WHEN OTHERS THEN
      NULL;
END;$$;

DO
$$BEGIN
--Testcase 13:
   SELECT oracle_execute('oracle_srv', 'DROP TABLE test."T 4" PURGE');
EXCEPTION
   WHEN OTHERS THEN
      NULL;
END;$$;

--Testcase 14:
SELECT oracle_execute(
          'oracle_srv',
          E'CREATE TABLE test."T 1" (\n'
          '   "C 1"   NUMBER(5) PRIMARY KEY,\n'
          '   c2   NUMBER(5),\n'
          '   c3   CLOB,\n'
          '   c4   TIMESTAMP WITH TIME ZONE,\n'
          '   c5   TIMESTAMP,\n'
          '   c6   VARCHAR(10),\n'
          '   c7   CHAR(10),\n'
          '   c8   CLOB\n'
          ') SEGMENT CREATION IMMEDIATE'
       );

--Testcase 15:
SELECT oracle_execute(
          'oracle_srv',
          E'CREATE TABLE test."T 2" (\n'
          '   c1  NUMBER(5) PRIMARY KEY,\n'
          '   c2   CLOB\n'
          ') SEGMENT CREATION IMMEDIATE'
       );

--Testcase 16:
SELECT oracle_execute(
          'oracle_srv',
          E'CREATE TABLE test."T 3" (\n'
          '   c1  NUMBER(5) PRIMARY KEY,\n'
          '   c2  NUMBER(5) ,\n'
          '   c3    CLOB\n'
          ') SEGMENT CREATION IMMEDIATE'
       );

--Testcase 17:
SELECT oracle_execute(
          'oracle_srv',
          E'CREATE TABLE test."T 4" (\n'
          '   c1  NUMBER(5) PRIMARY KEY,\n'
          '   c2  NUMBER(5) ,\n'
          '   c3    CLOB\n'
          ') SEGMENT CREATION IMMEDIATE'
       );

--Testcase 18:
CREATE SCHEMA "S 1";
-- table name will be set to lower case, e.g. "T 1" -> "t 1" if using case 'smart'
IMPORT FOREIGN SCHEMA "TEST" FROM SERVER oracle_srv INTO "S 1" OPTIONS (case 'smart');

-- check attributes of foreign table which was created by IMPORT FOREIGN SCHEMA
--Testcase 19:
\dS+ "S 1"."t 1";

-- Disable autovacuum for these tables to avoid unexpected effects of that
-- ALTER TABLE "S 1"."T 1" SET (autovacuum_enabled = 'false');
-- ALTER TABLE "S 1"."T 2" SET (autovacuum_enabled = 'false');
-- ALTER TABLE "S 1"."T 3" SET (autovacuum_enabled = 'false');
-- ALTER TABLE "S 1"."T 4" SET (autovacuum_enabled = 'false');

--Testcase 20:
INSERT INTO "S 1"."t 1"
	SELECT id,
	       id % 10,
	       to_char(id, 'FM00000'),
	       '1970-01-01'::timestamptz + ((id % 100) || ' days')::interval,
	       '1970-01-01'::timestamp + ((id % 100) || ' days')::interval,
	       id % 10,
	       id % 10,
	       'foo'::user_enum
	FROM generate_series(1, 1000) id;

--Testcase 21:
INSERT INTO "S 1"."t 2"
	SELECT id,
	       'AAA' || to_char(id, 'FM000')
	FROM generate_series(1, 100) id;

--Testcase 22:
INSERT INTO "S 1"."t 3"
	SELECT id,
	       id + 1,
	       'AAA' || to_char(id, 'FM000')
	FROM generate_series(1, 100) id;

--Testcase 23:
DELETE FROM "S 1"."t 3" WHERE c1 % 2 != 0;	-- delete for outer join tests

--Testcase 24:
INSERT INTO "S 1"."t 4"
	SELECT id,
	       id + 1,
	       'AAA' || to_char(id, 'FM000')
	FROM generate_series(1, 100) id;
--Testcase 25:
DELETE FROM "S 1"."t 4" WHERE c1 % 3 != 0;	-- delete for outer join tests

ANALYZE "S 1"."t 1";
ANALYZE "S 1"."t 2";
ANALYZE "S 1"."t 3";
ANALYZE "S 1"."t 4";

-- ===================================================================
-- create foreign tables
-- ===================================================================
--Testcase 26:
CREATE FOREIGN TABLE ft1 (
	c0 int,
	c1 int OPTIONS (key 'yes') NOT NULL ,
	c2 int NOT NULL,
	c3 text,
	c4 timestamptz,
	c5 timestamp,
	c6 varchar(10),
	c7 char(10) default 'ft1',
	c8 text
) SERVER oracle_srv OPTIONS (table 'T 1');;
--Testcase 27:
ALTER FOREIGN TABLE ft1 DROP COLUMN c0;

--Testcase 28:
CREATE FOREIGN TABLE ft2 (
	c1 int OPTIONS (key 'yes') NOT NULL ,
	c2 int NOT NULL,
	cx int,
	c3 text,
	c4 timestamptz,
	c5 timestamp,
	c6 varchar(10),
	c7 char(10) default 'ft2',
	c8 text
) SERVER oracle_srv OPTIONS (table 'T 1');;
--Testcase 29:
ALTER FOREIGN TABLE ft2 DROP COLUMN cx;

--Testcase 30:
CREATE FOREIGN TABLE ft4 (
	c1 int OPTIONS (key 'yes') NOT NULL ,
	c2 int NOT NULL,
	c3 text
) SERVER oracle_srv OPTIONS (table 'T 3');

--Testcase 31:
CREATE FOREIGN TABLE ft5 (
	c1 int OPTIONS (key 'yes') NOT NULL ,
	c2 int NOT NULL,
	c3 text
) SERVER oracle_srv OPTIONS (table 'T 4');

--Testcase 32:
CREATE FOREIGN TABLE ft6 (
	c1 int OPTIONS (key 'yes') NOT NULL ,
	c2 int NOT NULL,
	c3 text
) SERVER oracle_srv2 OPTIONS (table 'T 4');

--Testcase 33:
CREATE FOREIGN TABLE ft7 (
	c1 int OPTIONS (key 'yes') NOT NULL ,
	c2 int NOT NULL,
	c3 text
) SERVER oracle_srv3 OPTIONS (table 'T 4');

-- ===================================================================
-- tests for validator
-- ===================================================================
-- requiressl and some other parameters are omitted because
-- valid values for them depend on configure options
-- ALTER SERVER testserver1 OPTIONS (
-- 	use_remote_estimate 'false',
-- 	updatable 'true',
-- 	fdw_startup_cost '123.456',
-- 	fdw_tuple_cost '0.123',
-- 	service 'value',
-- 	connect_timeout 'value',
-- 	dbname 'value',
-- 	host 'value',
-- 	hostaddr 'value',
-- 	port 'value',
-- 	--client_encoding 'value',
-- 	application_name 'value',
-- 	--fallback_application_name 'value',
-- 	keepalives 'value',
-- 	keepalives_idle 'value',
-- 	keepalives_interval 'value',
-- 	tcp_user_timeout 'value',
-- 	-- requiressl 'value',
-- 	sslcompression 'value',
-- 	sslmode 'value',
-- 	sslcert 'value',
-- 	sslkey 'value',
-- 	sslrootcert 'value',
-- 	sslcrl 'value',
-- 	--requirepeer 'value',
-- 	krbsrvname 'value',
-- 	gsslib 'value'
-- 	--replication 'value'
-- );

-- -- Error, invalid list syntax
-- ALTER SERVER testserver1 OPTIONS (ADD extensions 'foo; bar');

-- -- OK but gets a warning
-- ALTER SERVER testserver1 OPTIONS (ADD extensions 'foo, bar');
-- ALTER SERVER testserver1 OPTIONS (DROP extensions);

-- ALTER USER MAPPING FOR public SERVER testserver1
-- 	OPTIONS (DROP user, DROP password);

-- -- Attempt to add a valid option that's not allowed in a user mapping
-- ALTER USER MAPPING FOR public SERVER testserver1
-- 	OPTIONS (ADD sslmode 'require');

-- -- But we can add valid ones fine
-- ALTER USER MAPPING FOR public SERVER testserver1
-- 	OPTIONS (ADD sslpassword 'dummy');

-- -- Ensure valid options we haven't used in a user mapping yet are
-- -- permitted to check validation.
-- ALTER USER MAPPING FOR public SERVER testserver1
-- 	OPTIONS (ADD sslkey 'value', ADD sslcert 'value');

-- ALTER FOREIGN TABLE ft1 OPTIONS (schema_name 'S 1', table 'T 1');
-- ALTER FOREIGN TABLE ft2 OPTIONS (schema_name 'S 1', table 'T 1');
--Testcase 34:
ALTER FOREIGN TABLE ft1 ALTER COLUMN c1 OPTIONS (column_name 'C 1');
--Testcase 35:
ALTER FOREIGN TABLE ft2 ALTER COLUMN c1 OPTIONS (column_name 'C 1');
--Testcase 36:
\det+

-- oracle_fdw does not support dbname option
-- Test that alteration of server options causes reconnection
-- Remote's errors might be non-English, so hide them to ensure stable results
-- \set VERBOSITY terse
-- SELECT c3, c4 FROM ft1 ORDER BY c3, c1 LIMIT 1;  -- should work
-- ALTER SERVER oracle_srv OPTIONS (SET dbname 'no such database');
-- SELECT c3, c4 FROM ft1 ORDER BY c3, c1 LIMIT 1;  -- should fail
-- DO $d$
--     BEGIN
--         EXECUTE $$ALTER SERVER oracle_srv
--             OPTIONS (SET dbname '$$||current_database()||$$')$$;
--     END;
-- $d$;
-- SELECT c3, c4 FROM ft1 ORDER BY c3, c1 LIMIT 1;  -- should work again

-- oracle_fdw does not support add user option
-- -- Test that alteration of user mapping options causes reconnection
-- ALTER USER MAPPING FOR CURRENT_USER SERVER oracle_srv
--   OPTIONS (ADD user 'no such user');
-- SELECT c3, c4 FROM ft1 ORDER BY c3, c1 LIMIT 1;  -- should fail
-- ALTER USER MAPPING FOR CURRENT_USER SERVER oracle_srv
--   OPTIONS (DROP user);
-- SELECT c3, c4 FROM ft1 ORDER BY c3, c1 LIMIT 1;  -- should work again
-- \set VERBOSITY default

-- oracle_fdw does not support use_remote_estimate option
-- -- Now we should be able to run ANALYZE.
-- -- To exercise multiple code paths, we use local stats on ft1
-- -- and remote-estimate mode on ft2.
-- ANALYZE ft1;
-- ALTER FOREIGN TABLE ft2 OPTIONS (use_remote_estimate 'true');

-- ===================================================================
-- simple queries
-- ===================================================================
-- single table without alias
-- According to the oracle specification, we cannot specify LOB columns in the ORDER BY clause of a query,
-- the GROUP BY clause of a query, or an aggregate function. c3 is represented as LOB data,
-- so it is not pushed down.
--Testcase 37:
EXPLAIN (COSTS OFF) SELECT * FROM ft1 ORDER BY c3, c1 OFFSET 100 LIMIT 10;
--Testcase 38:
SELECT * FROM ft1 ORDER BY c3, c1 OFFSET 100 LIMIT 10;
-- single table with alias - also test that tableoid sort is not pushed to remote side
--Testcase 39:
EXPLAIN (COSTS OFF) SELECT * FROM ft1 t1 ORDER BY t1.c3, t1.c1, t1.tableoid OFFSET 100 LIMIT 10;
--Testcase 40:
SELECT * FROM ft1 t1 ORDER BY t1.c3, t1.c1, t1.tableoid OFFSET 100 LIMIT 10;
-- whole-row reference
--Testcase 41:
EXPLAIN (COSTS OFF) SELECT t1 FROM ft1 t1 ORDER BY t1.c3, t1.c1 OFFSET 100 LIMIT 10;
--Testcase 42:
SELECT t1 FROM ft1 t1 ORDER BY t1.c3, t1.c1 OFFSET 100 LIMIT 10;
-- empty result
--Testcase 43:
SELECT * FROM ft1 WHERE false;
-- with WHERE clause
--Testcase 44:
EXPLAIN (COSTS OFF) SELECT * FROM ft1 t1 WHERE t1.c1 = 101 AND t1.c6 = '1' AND t1.c7 >= '1';
--Testcase 45:
SELECT * FROM ft1 t1 WHERE t1.c1 = 101 AND t1.c6 = '1' AND t1.c7 >= '1';
-- with FOR UPDATE/SHARE
--Testcase 46:
EXPLAIN (COSTS OFF) SELECT * FROM ft1 t1 WHERE c1 = 101 FOR UPDATE;
--Testcase 47:
SELECT * FROM ft1 t1 WHERE c1 = 101 FOR UPDATE;
--Testcase 48:
EXPLAIN (COSTS OFF) SELECT * FROM ft1 t1 WHERE c1 = 102 FOR SHARE;
--Testcase 49:
SELECT * FROM ft1 t1 WHERE c1 = 102 FOR SHARE;

-- aggregate
--Testcase 50:
SELECT COUNT(*) FROM ft1 t1;
-- subquery
--Testcase 51:
SELECT * FROM ft1 t1 WHERE t1.c3 IN (SELECT c3 FROM ft2 t2 WHERE c1 <= 10) ORDER BY c1;
-- subquery+MAX
--Testcase 52:
SELECT * FROM ft1 t1 WHERE t1.c3 = (SELECT MAX(c3) FROM ft2 t2) ORDER BY c1;
-- used in CTE
--Testcase 53:
WITH t1 AS (SELECT * FROM ft1 WHERE c1 <= 10) SELECT t2.c1, t2.c2, t2.c3, t2.c4 FROM t1, ft2 t2 WHERE t1.c1 = t2.c1 ORDER BY t1.c1;
-- fixed values
--Testcase 54:
SELECT 'fixed', NULL FROM ft1 t1 WHERE c1 = 1;
-- Test forcing the remote server to produce sorted data for a merge join.

--Testcase 55:
SET enable_hashjoin TO false;
--Testcase 56:
SET enable_nestloop TO false;
-- inner join; expressions in the clauses appear in the equivalence class list
--Testcase 57:
EXPLAIN (COSTS OFF)
	SELECT t1.c1, t2."c 1" FROM ft2 t1 JOIN "S 1"."t 1" t2 ON (t1.c1 = t2."c 1") ORDER BY t1.c1, t2."c 1" OFFSET 100 LIMIT 10;
--Testcase 58:
SELECT t1.c1, t2."c 1" FROM ft2 t1 JOIN "S 1"."t 1" t2 ON (t1.c1 = t2."c 1") ORDER BY t1.c1, t2."c 1" OFFSET 100 LIMIT 10;
-- outer join; expressions in the clauses do not appear in equivalence class
-- list but no output change as compared to the previous query
--Testcase 59:
EXPLAIN (COSTS OFF)
	SELECT t1.c1, t2."c 1" FROM ft2 t1 LEFT JOIN "S 1"."t 1" t2 ON (t1.c1 = t2."c 1") ORDER BY t1.c1 OFFSET 100 LIMIT 10;
--Testcase 60:
SELECT t1.c1, t2."c 1" FROM ft2 t1 LEFT JOIN "S 1"."t 1" t2 ON (t1.c1 = t2."c 1") ORDER BY t1.c1 OFFSET 100 LIMIT 10;
-- A join between local table and foreign join. ORDER BY clause is added to the
-- foreign join so that the local table can be joined using merge join strategy.
-- oracle fdw does not support three table join
--Testcase 61:
EXPLAIN (COSTS OFF)
	SELECT t1."c 1" FROM "S 1"."t 1" t1 left join ft1 t2 join ft2 t3 on (t2.c1 = t3.c1) on (t3.c1 = t1."c 1") OFFSET 100 LIMIT 10;
--Testcase 62:
SELECT t1."c 1" FROM "S 1"."t 1" t1 left join ft1 t2 join ft2 t3 on (t2.c1 = t3.c1) on (t3.c1 = t1."c 1") OFFSET 100 LIMIT 10;
-- Test similar to above, except that the full join prevents any equivalence
-- classes from being merged. This produces single relation equivalence classes
-- included in join restrictions.
--Testcase 63:
EXPLAIN (COSTS OFF)
	SELECT t1."c 1", t2.c1, t3.c1 FROM "S 1"."t 1" t1 left join ft1 t2 full join ft2 t3 on (t2.c1 = t3.c1) on (t3.c1 = t1."c 1") OFFSET 100 LIMIT 10;
--Testcase 64:
SELECT t1."c 1", t2.c1, t3.c1 FROM "S 1"."t 1" t1 left join ft1 t2 full join ft2 t3 on (t2.c1 = t3.c1) on (t3.c1 = t1."c 1") OFFSET 100 LIMIT 10;
-- Test similar to above with all full outer joins
--Testcase 65:
EXPLAIN (COSTS OFF)
	SELECT t1."c 1", t2.c1, t3.c1 FROM "S 1"."t 1" t1 full join ft1 t2 full join ft2 t3 on (t2.c1 = t3.c1) on (t3.c1 = t1."c 1") OFFSET 100 LIMIT 10;
--Testcase 66:
SELECT t1."c 1", t2.c1, t3.c1 FROM "S 1"."t 1" t1 full join ft1 t2 full join ft2 t3 on (t2.c1 = t3.c1) on (t3.c1 = t1."c 1") OFFSET 100 LIMIT 10;
--Testcase 67:
RESET enable_hashjoin;
--Testcase 68:
RESET enable_nestloop;

-- Test executing assertion in estimate_path_cost_size() that makes sure that
-- retrieved_rows for foreign rel re-used to cost pre-sorted foreign paths is
-- a sensible value even when the rel has tuples=0
--Testcase 69:
SELECT oracle_execute(
           'oracle_srv',
           E'CREATE TABLE test.loct_empty (\n'
           '   c1  NUMBER(5) PRIMARY KEY,\n'
           '   c2   CLOB\n'
           ') SEGMENT CREATION IMMEDIATE'
        );

--Testcase 70:
CREATE FOREIGN TABLE ft_empty (c1 int options (key 'yes') NOT NULL, c2 text)
   SERVER oracle_srv OPTIONS (table 'LOCT_EMPTY');

--Testcase 71:
INSERT INTO ft_empty
   SELECT id, 'AAA' || to_char(id, 'FM000') FROM generate_series(1, 100) id;
--Testcase 72:
DELETE FROM ft_empty;
ANALYZE ft_empty;
--Testcase 73:
EXPLAIN (COSTS OFF) SELECT * FROM ft_empty ORDER BY c1;

--Testcase 74:
SELECT oracle_execute('oracle_srv', 'DROP TABLE test.loct_empty PURGE');

-- ===================================================================
-- WHERE with remotely-executable conditions
-- ===================================================================
--Testcase 75:
EXPLAIN (COSTS OFF) SELECT * FROM ft1 t1 WHERE t1.c1 = 1;         -- Var, OpExpr(b), Const
--Testcase 76:
EXPLAIN (COSTS OFF) SELECT * FROM ft1 t1 WHERE t1.c1 = 100 AND t1.c2 = 0; -- BoolExpr
--Testcase 77:
EXPLAIN (COSTS OFF) SELECT * FROM ft1 t1 WHERE c1 IS NULL;        -- NullTest
--Testcase 78:
EXPLAIN (COSTS OFF) SELECT * FROM ft1 t1 WHERE c1 IS NOT NULL;    -- NullTest
--Testcase 79:
EXPLAIN (COSTS OFF) SELECT * FROM ft1 t1 WHERE round(abs(c1), 0) = 1; -- FuncExpr
--Testcase 80:
EXPLAIN (COSTS OFF) SELECT * FROM ft1 t1 WHERE c1 = -c1;          -- OpExpr(l)
--Testcase 81:
EXPLAIN (COSTS OFF) SELECT * FROM ft1 t1 WHERE (c1 IS NOT NULL) IS DISTINCT FROM (c1 IS NOT NULL); -- DistinctExpr
--Testcase 82:
EXPLAIN (COSTS OFF) SELECT * FROM ft1 t1 WHERE c1 = ANY(ARRAY[c2, 1, c1 + 0]); -- ScalarArrayOpExpr
--Testcase 83:
EXPLAIN (COSTS OFF) SELECT * FROM ft1 t1 WHERE c1 = (ARRAY[c1,c2,3])[1]; -- SubscriptingRef
--Testcase 84:
EXPLAIN (COSTS OFF) SELECT * FROM ft1 t1 WHERE c6 = E'foo''s\\bar';  -- check special chars
--Testcase 85:
EXPLAIN (COSTS OFF) SELECT * FROM ft1 t1 WHERE c8 = 'foo';  -- can't be sent to remote
-- parameterized remote path for foreign table
--Testcase 86:
EXPLAIN (COSTS OFF)
  SELECT * FROM "S 1"."t 1" a, ft2 b WHERE a."c 1" = 47 AND b.c1 = a.c2;
--Testcase 87:
SELECT * FROM ft2 a, ft2 b WHERE a.c1 = 47 AND b.c1 = a.c2;

-- check both safe and unsafe join conditions
--Testcase 88:
EXPLAIN (COSTS OFF)
  SELECT * FROM ft2 a, ft2 b
  WHERE a.c2 = 6 AND b.c1 = a.c1 AND a.c8 = 'foo' AND b.c7 = upper(a.c7);
--Testcase 89:
SELECT * FROM ft2 a, ft2 b
WHERE a.c2 = 6 AND b.c1 = a.c1 AND a.c8 = 'foo' AND b.c7 = upper(a.c7);
-- bug before 9.3.5 due to sloppy handling of remote-estimate parameters
--Testcase 90:
SELECT * FROM ft1 WHERE c1 = ANY (ARRAY(SELECT c1 FROM ft2 WHERE c1 < 5));
--Testcase 91:
SELECT * FROM ft2 WHERE c1 = ANY (ARRAY(SELECT c1 FROM ft1 WHERE c1 < 5));
-- we should not push order by clause with volatile expressions or unsafe
-- collations
--Testcase 92:
EXPLAIN (COSTS OFF)
	SELECT * FROM ft2 ORDER BY ft2.c1, random();
--Testcase 93:
EXPLAIN (COSTS OFF)
	SELECT * FROM ft2 ORDER BY ft2.c1, ft2.c3 collate "C";

-- user-defined operator/function
--Testcase 94:
CREATE FUNCTION oracle_fdw_abs(int) RETURNS int AS $$
BEGIN
RETURN abs($1);
END
$$ LANGUAGE plpgsql IMMUTABLE;
--Testcase 95:
CREATE OPERATOR === (
    LEFTARG = int,
    RIGHTARG = int,
    PROCEDURE = int4eq,
    COMMUTATOR = ===
);

-- built-in operators and functions can be shipped for remote execution
-- according to oracle spec, do not pushdown TEXT/CLOB in aggregation function
--Testcase 96:
EXPLAIN (COSTS OFF)
  SELECT count(c3) FROM ft1 t1 WHERE t1.c1 = abs(t1.c2);
--Testcase 97:
SELECT count(c3) FROM ft1 t1 WHERE t1.c1 = abs(t1.c2);
--Testcase 98:
EXPLAIN (COSTS OFF)
  SELECT count(c3) FROM ft1 t1 WHERE t1.c1 = t1.c2;
--Testcase 99:
SELECT count(c3) FROM ft1 t1 WHERE t1.c1 = t1.c2;

-- by default, user-defined ones cannot
--Testcase 100:
EXPLAIN (COSTS OFF)
  SELECT count(c3) FROM ft1 t1 WHERE t1.c1 = oracle_fdw_abs(t1.c2);
--Testcase 101:
SELECT count(c3) FROM ft1 t1 WHERE t1.c1 = oracle_fdw_abs(t1.c2);
--Testcase 102:
EXPLAIN (COSTS OFF)
  SELECT count(c3) FROM ft1 t1 WHERE t1.c1 === t1.c2;
--Testcase 103:
SELECT count(c3) FROM ft1 t1 WHERE t1.c1 === t1.c2;

-- ORDER BY can be shipped, though
--Testcase 104:
EXPLAIN (COSTS OFF)
  SELECT * FROM ft1 t1 WHERE t1.c1 === t1.c2 order by t1.c2 limit 1;
--Testcase 105:
SELECT * FROM ft1 t1 WHERE t1.c1 === t1.c2 order by t1.c2 limit 1;

-- but let's put them in an extension ...
--Testcase 106:
ALTER EXTENSION oracle_fdw ADD FUNCTION oracle_fdw_abs(int);
--Testcase 107:
ALTER EXTENSION oracle_fdw ADD OPERATOR === (int, int);
-- oracle_fdw does not support 'extentions' option, the user-defined function 
-- cannot be shipped.
--ALTER SERVER oracle_srv OPTIONS (ADD extensions 'oracle_fdw');

-- ... now they can be shipped
--Testcase 108:
EXPLAIN (COSTS OFF)
  SELECT count(c3) FROM ft1 t1 WHERE t1.c1 = oracle_fdw_abs(t1.c2);
--Testcase 109:
SELECT count(c3) FROM ft1 t1 WHERE t1.c1 = oracle_fdw_abs(t1.c2);
--Testcase 110:
EXPLAIN (COSTS OFF)
  SELECT count(c3) FROM ft1 t1 WHERE t1.c1 === t1.c2;
--Testcase 111:
SELECT count(c3) FROM ft1 t1 WHERE t1.c1 === t1.c2;

-- and both ORDER BY and LIMIT can be shipped
--Testcase 112:
EXPLAIN (COSTS OFF)
  SELECT * FROM ft1 t1 WHERE t1.c1 === t1.c2 order by t1.c2 limit 1;
--Testcase 113:
SELECT * FROM ft1 t1 WHERE t1.c1 === t1.c2 order by t1.c2 limit 1;

-- ===================================================================
-- JOIN queries
-- ===================================================================
-- Analyze ft4 and ft5 so that we have better statistics. These tables do not
-- have use_remote_estimate set.
ANALYZE ft4;
ANALYZE ft5;

-- join two tables
--Testcase 114:
EXPLAIN (COSTS OFF)
SELECT t1.c1, t2.c1 FROM ft1 t1 JOIN ft2 t2 ON (t1.c1 = t2.c1) ORDER BY t1.c3, t1.c1 OFFSET 100 LIMIT 10;
--Testcase 115:
SELECT t1.c1, t2.c1 FROM ft1 t1 JOIN ft2 t2 ON (t1.c1 = t2.c1) ORDER BY t1.c3, t1.c1 OFFSET 100 LIMIT 10;
-- join three tables
--Testcase 116:
EXPLAIN (COSTS OFF)
SELECT t1.c1, t2.c2, t3.c3 FROM ft1 t1 JOIN ft2 t2 ON (t1.c1 = t2.c1) JOIN ft4 t3 ON (t3.c1 = t1.c1) ORDER BY t1.c3, t1.c1 OFFSET 10 LIMIT 10;
--Testcase 117:
SELECT t1.c1, t2.c2, t3.c3 FROM ft1 t1 JOIN ft2 t2 ON (t1.c1 = t2.c1) JOIN ft4 t3 ON (t3.c1 = t1.c1) ORDER BY t1.c3, t1.c1 OFFSET 10 LIMIT 10;
-- left outer join
--Testcase 118:
EXPLAIN (COSTS OFF)
SELECT t1.c1, t2.c1 FROM ft4 t1 LEFT JOIN ft5 t2 ON (t1.c1 = t2.c1) ORDER BY t1.c1, t2.c1 OFFSET 10 LIMIT 10;
--Testcase 119:
SELECT t1.c1, t2.c1 FROM ft4 t1 LEFT JOIN ft5 t2 ON (t1.c1 = t2.c1) ORDER BY t1.c1, t2.c1 OFFSET 10 LIMIT 10;
-- left outer join three tables
--Testcase 120:
EXPLAIN (COSTS OFF)
SELECT t1.c1, t2.c2, t3.c3 FROM ft2 t1 LEFT JOIN ft2 t2 ON (t1.c1 = t2.c1) LEFT JOIN ft4 t3 ON (t2.c1 = t3.c1) ORDER BY t1.c1 OFFSET 10 LIMIT 10;
--Testcase 121:
SELECT t1.c1, t2.c2, t3.c3 FROM ft2 t1 LEFT JOIN ft2 t2 ON (t1.c1 = t2.c1) LEFT JOIN ft4 t3 ON (t2.c1 = t3.c1) ORDER BY t1.c1 OFFSET 10 LIMIT 10;
-- left outer join + placement of clauses.
-- clauses within the nullable side are not pulled up, but top level clause on
-- non-nullable side is pushed into non-nullable side
--Testcase 122:
EXPLAIN (COSTS OFF)
SELECT t1.c1, t1.c2, t2.c1, t2.c2 FROM ft4 t1 LEFT JOIN (SELECT * FROM ft5 WHERE c1 < 10) t2 ON (t1.c1 = t2.c1) WHERE t1.c1 < 10;
--Testcase 123:
SELECT t1.c1, t1.c2, t2.c1, t2.c2 FROM ft4 t1 LEFT JOIN (SELECT * FROM ft5 WHERE c1 < 10) t2 ON (t1.c1 = t2.c1) WHERE t1.c1 < 10;
-- clauses within the nullable side are not pulled up, but the top level clause
-- on nullable side is not pushed down into nullable side
--Testcase 124:
EXPLAIN (COSTS OFF)
SELECT t1.c1, t1.c2, t2.c1, t2.c2 FROM ft4 t1 LEFT JOIN (SELECT * FROM ft5 WHERE c1 < 10) t2 ON (t1.c1 = t2.c1)
			WHERE (t2.c1 < 10 OR t2.c1 IS NULL) AND t1.c1 < 10;
--Testcase 125:
SELECT t1.c1, t1.c2, t2.c1, t2.c2 FROM ft4 t1 LEFT JOIN (SELECT * FROM ft5 WHERE c1 < 10) t2 ON (t1.c1 = t2.c1)
			WHERE (t2.c1 < 10 OR t2.c1 IS NULL) AND t1.c1 < 10;
-- right outer join
--Testcase 126:
EXPLAIN (COSTS OFF)
SELECT t1.c1, t2.c1 FROM ft5 t1 RIGHT JOIN ft4 t2 ON (t1.c1 = t2.c1) ORDER BY t2.c1, t1.c1 OFFSET 10 LIMIT 10;
--Testcase 127:
SELECT t1.c1, t2.c1 FROM ft5 t1 RIGHT JOIN ft4 t2 ON (t1.c1 = t2.c1) ORDER BY t2.c1, t1.c1 OFFSET 10 LIMIT 10;
-- right outer join three tables
--Testcase 128:
EXPLAIN (COSTS OFF)
SELECT t1.c1, t2.c2, t3.c3 FROM ft2 t1 RIGHT JOIN ft2 t2 ON (t1.c1 = t2.c1) RIGHT JOIN ft4 t3 ON (t2.c1 = t3.c1) OFFSET 10 LIMIT 10;
--Testcase 129:
SELECT t1.c1, t2.c2, t3.c3 FROM ft2 t1 RIGHT JOIN ft2 t2 ON (t1.c1 = t2.c1) RIGHT JOIN ft4 t3 ON (t2.c1 = t3.c1) OFFSET 10 LIMIT 10;
-- full outer join
--Testcase 130:
EXPLAIN (COSTS OFF)
SELECT t1.c1, t2.c1 FROM ft4 t1 FULL JOIN ft5 t2 ON (t1.c1 = t2.c1) ORDER BY t1.c1, t2.c1 OFFSET 45 LIMIT 10;
--Testcase 131:
SELECT t1.c1, t2.c1 FROM ft4 t1 FULL JOIN ft5 t2 ON (t1.c1 = t2.c1) ORDER BY t1.c1, t2.c1 OFFSET 45 LIMIT 10;
-- full outer join with restrictions on the joining relations
-- a. the joining relations are both base relations
--Testcase 132:
EXPLAIN (COSTS OFF)
SELECT t1.c1, t2.c1 FROM (SELECT c1 FROM ft4 WHERE c1 between 50 and 60) t1 FULL JOIN (SELECT c1 FROM ft5 WHERE c1 between 50 and 60) t2 ON (t1.c1 = t2.c1) ORDER BY t1.c1, t2.c1;
--Testcase 133:
SELECT t1.c1, t2.c1 FROM (SELECT c1 FROM ft4 WHERE c1 between 50 and 60) t1 FULL JOIN (SELECT c1 FROM ft5 WHERE c1 between 50 and 60) t2 ON (t1.c1 = t2.c1) ORDER BY t1.c1, t2.c1;
--Testcase 134:
EXPLAIN (COSTS OFF)
SELECT 1 FROM (SELECT c1 FROM ft4 WHERE c1 between 50 and 60) t1 FULL JOIN (SELECT c1 FROM ft5 WHERE c1 between 50 and 60) t2 ON (TRUE) OFFSET 10 LIMIT 10;
--Testcase 135:
SELECT 1 FROM (SELECT c1 FROM ft4 WHERE c1 between 50 and 60) t1 FULL JOIN (SELECT c1 FROM ft5 WHERE c1 between 50 and 60) t2 ON (TRUE) OFFSET 10 LIMIT 10;
-- b. one of the joining relations is a base relation and the other is a join
-- relation
--Testcase 136:
EXPLAIN (COSTS OFF)
SELECT t1.c1, ss.a, ss.b FROM (SELECT c1 FROM ft4 WHERE c1 between 50 and 60) t1 FULL JOIN (SELECT t2.c1, t3.c1 FROM ft4 t2 LEFT JOIN ft5 t3 ON (t2.c1 = t3.c1) WHERE (t2.c1 between 50 and 60)) ss(a, b) ON (t1.c1 = ss.a) ORDER BY t1.c1, ss.a, ss.b;
--Testcase 137:
SELECT t1.c1, ss.a, ss.b FROM (SELECT c1 FROM ft4 WHERE c1 between 50 and 60) t1 FULL JOIN (SELECT t2.c1, t3.c1 FROM ft4 t2 LEFT JOIN ft5 t3 ON (t2.c1 = t3.c1) WHERE (t2.c1 between 50 and 60)) ss(a, b) ON (t1.c1 = ss.a) ORDER BY t1.c1, ss.a, ss.b;
-- c. test deparsing the remote query as nested subqueries
--Testcase 138:
EXPLAIN (COSTS OFF)
SELECT t1.c1, ss.a, ss.b FROM (SELECT c1 FROM ft4 WHERE c1 between 50 and 60) t1 FULL JOIN (SELECT t2.c1, t3.c1 FROM (SELECT c1 FROM ft4 WHERE c1 between 50 and 60) t2 FULL JOIN (SELECT c1 FROM ft5 WHERE c1 between 50 and 60) t3 ON (t2.c1 = t3.c1) WHERE t2.c1 IS NULL OR t2.c1 IS NOT NULL) ss(a, b) ON (t1.c1 = ss.a) ORDER BY t1.c1, ss.a, ss.b;
--Testcase 139:
SELECT t1.c1, ss.a, ss.b FROM (SELECT c1 FROM ft4 WHERE c1 between 50 and 60) t1 FULL JOIN (SELECT t2.c1, t3.c1 FROM (SELECT c1 FROM ft4 WHERE c1 between 50 and 60) t2 FULL JOIN (SELECT c1 FROM ft5 WHERE c1 between 50 and 60) t3 ON (t2.c1 = t3.c1) WHERE t2.c1 IS NULL OR t2.c1 IS NOT NULL) ss(a, b) ON (t1.c1 = ss.a) ORDER BY t1.c1, ss.a, ss.b;
-- d. test deparsing rowmarked relations as subqueries
--Testcase 140:
EXPLAIN (COSTS OFF)
SELECT t1.c1, ss.a, ss.b FROM (SELECT c1 FROM "S 1"."t 3" WHERE c1 = 50) t1 INNER JOIN (SELECT t2.c1, t3.c1 FROM (SELECT c1 FROM ft4 WHERE c1 between 50 and 60) t2 FULL JOIN (SELECT c1 FROM ft5 WHERE c1 between 50 and 60) t3 ON (t2.c1 = t3.c1) WHERE t2.c1 IS NULL OR t2.c1 IS NOT NULL) ss(a, b) ON (TRUE) ORDER BY t1.c1, ss.a, ss.b FOR UPDATE OF t1;
--Testcase 141:
SELECT t1.c1, ss.a, ss.b FROM (SELECT c1 FROM "S 1"."t 3" WHERE c1 = 50) t1 INNER JOIN (SELECT t2.c1, t3.c1 FROM (SELECT c1 FROM ft4 WHERE c1 between 50 and 60) t2 FULL JOIN (SELECT c1 FROM ft5 WHERE c1 between 50 and 60) t3 ON (t2.c1 = t3.c1) WHERE t2.c1 IS NULL OR t2.c1 IS NOT NULL) ss(a, b) ON (TRUE) ORDER BY t1.c1, ss.a, ss.b FOR UPDATE OF t1;
-- full outer join + inner join
--Testcase 142:
EXPLAIN (COSTS OFF)
SELECT t1.c1, t2.c1, t3.c1 FROM ft4 t1 INNER JOIN ft5 t2 ON (t1.c1 = t2.c1 + 1 and t1.c1 between 50 and 60) FULL JOIN ft4 t3 ON (t2.c1 = t3.c1) ORDER BY t1.c1, t2.c1, t3.c1 LIMIT 10;
--Testcase 143:
SELECT t1.c1, t2.c1, t3.c1 FROM ft4 t1 INNER JOIN ft5 t2 ON (t1.c1 = t2.c1 + 1 and t1.c1 between 50 and 60) FULL JOIN ft4 t3 ON (t2.c1 = t3.c1) ORDER BY t1.c1, t2.c1, t3.c1 LIMIT 10;
-- full outer join three tables
--Testcase 144:
EXPLAIN (COSTS OFF)
SELECT t1.c1, t2.c2, t3.c3 FROM ft2 t1 FULL JOIN ft2 t2 ON (t1.c1 = t2.c1) FULL JOIN ft4 t3 ON (t2.c1 = t3.c1) ORDER BY t1.c1 OFFSET 10 LIMIT 10;
--Testcase 145:
SELECT t1.c1, t2.c2, t3.c3 FROM ft2 t1 FULL JOIN ft2 t2 ON (t1.c1 = t2.c1) FULL JOIN ft4 t3 ON (t2.c1 = t3.c1) ORDER BY t1.c1 OFFSET 10 LIMIT 10;
-- full outer join + right outer join
--Testcase 146:
EXPLAIN (COSTS OFF)
SELECT t1.c1, t2.c2, t3.c3 FROM ft2 t1 FULL JOIN ft2 t2 ON (t1.c1 = t2.c1) RIGHT JOIN ft4 t3 ON (t2.c1 = t3.c1) OFFSET 10 LIMIT 10;
--Testcase 147:
SELECT t1.c1, t2.c2, t3.c3 FROM ft2 t1 FULL JOIN ft2 t2 ON (t1.c1 = t2.c1) RIGHT JOIN ft4 t3 ON (t2.c1 = t3.c1) OFFSET 10 LIMIT 10;
-- right outer join + full outer join
--Testcase 148:
EXPLAIN (COSTS OFF)
SELECT t1.c1, t2.c2, t3.c3 FROM ft2 t1 RIGHT JOIN ft2 t2 ON (t1.c1 = t2.c1) FULL JOIN ft4 t3 ON (t2.c1 = t3.c1) ORDER BY t1.c1 OFFSET 10 LIMIT 10;
--Testcase 149:
SELECT t1.c1, t2.c2, t3.c3 FROM ft2 t1 RIGHT JOIN ft2 t2 ON (t1.c1 = t2.c1) FULL JOIN ft4 t3 ON (t2.c1 = t3.c1) ORDER BY t1.c1 OFFSET 10 LIMIT 10;
-- full outer join + left outer join
--Testcase 150:
EXPLAIN (COSTS OFF)
SELECT t1.c1, t2.c2, t3.c3 FROM ft2 t1 FULL JOIN ft2 t2 ON (t1.c1 = t2.c1) LEFT JOIN ft4 t3 ON (t2.c1 = t3.c1) ORDER BY t1.c1 OFFSET 10 LIMIT 10;
--Testcase 151:
SELECT t1.c1, t2.c2, t3.c3 FROM ft2 t1 FULL JOIN ft2 t2 ON (t1.c1 = t2.c1) LEFT JOIN ft4 t3 ON (t2.c1 = t3.c1) ORDER BY t1.c1 OFFSET 10 LIMIT 10;
-- left outer join + full outer join
--Testcase 152:
EXPLAIN (COSTS OFF)
SELECT t1.c1, t2.c2, t3.c3 FROM ft2 t1 LEFT JOIN ft2 t2 ON (t1.c1 = t2.c1) FULL JOIN ft4 t3 ON (t2.c1 = t3.c1) ORDER BY t1.c1 OFFSET 10 LIMIT 10;
--Testcase 153:
SELECT t1.c1, t2.c2, t3.c3 FROM ft2 t1 LEFT JOIN ft2 t2 ON (t1.c1 = t2.c1) FULL JOIN ft4 t3 ON (t2.c1 = t3.c1) ORDER BY t1.c1 OFFSET 10 LIMIT 10;
--Testcase 154:
SET enable_memoize TO off;
-- right outer join + left outer join
--Testcase 155:
EXPLAIN (COSTS OFF)
SELECT t1.c1, t2.c2, t3.c3 FROM ft2 t1 RIGHT JOIN ft2 t2 ON (t1.c1 = t2.c1) LEFT JOIN ft4 t3 ON (t2.c1 = t3.c1) ORDER BY t1.c1 OFFSET 10 LIMIT 10;
--Testcase 156:
SELECT t1.c1, t2.c2, t3.c3 FROM ft2 t1 RIGHT JOIN ft2 t2 ON (t1.c1 = t2.c1) LEFT JOIN ft4 t3 ON (t2.c1 = t3.c1) ORDER BY t1.c1 OFFSET 10 LIMIT 10;
--Testcase 157:
RESET enable_memoize;
-- left outer join + right outer join
--Testcase 158:
EXPLAIN (COSTS OFF)
SELECT t1.c1, t2.c2, t3.c3 FROM ft2 t1 LEFT JOIN ft2 t2 ON (t1.c1 = t2.c1) RIGHT JOIN ft4 t3 ON (t2.c1 = t3.c1) ORDER BY t1.c1 OFFSET 10 LIMIT 10;
--Testcase 159:
SELECT t1.c1, t2.c2, t3.c3 FROM ft2 t1 LEFT JOIN ft2 t2 ON (t1.c1 = t2.c1) RIGHT JOIN ft4 t3 ON (t2.c1 = t3.c1) ORDER BY t1.c1 OFFSET 10 LIMIT 10;
-- full outer join + WHERE clause, only matched rows
--Testcase 160:
EXPLAIN (COSTS OFF)
SELECT t1.c1, t2.c1 FROM ft4 t1 FULL JOIN ft5 t2 ON (t1.c1 = t2.c1) WHERE (t1.c1 = t2.c1 OR t1.c1 IS NULL) ORDER BY t1.c1, t2.c1 OFFSET 10 LIMIT 10;
--Testcase 161:
SELECT t1.c1, t2.c1 FROM ft4 t1 FULL JOIN ft5 t2 ON (t1.c1 = t2.c1) WHERE (t1.c1 = t2.c1 OR t1.c1 IS NULL) ORDER BY t1.c1, t2.c1 OFFSET 10 LIMIT 10;
-- full outer join + WHERE clause with shippable extensions set
--Testcase 162:
EXPLAIN (COSTS OFF)
SELECT t1.c1, t2.c2, t1.c3 FROM ft1 t1 FULL JOIN ft2 t2 ON (t1.c1 = t2.c1) WHERE oracle_fdw_abs(t1.c1) > 0 OFFSET 10 LIMIT 10;
--ALTER SERVER oracle_srv OPTIONS (DROP extensions);
-- full outer join + WHERE clause with shippable extensions not set
--Testcase 163:
EXPLAIN (COSTS OFF)
SELECT t1.c1, t2.c2, t1.c3 FROM ft1 t1 FULL JOIN ft2 t2 ON (t1.c1 = t2.c1) WHERE oracle_fdw_abs(t1.c1) > 0 OFFSET 10 LIMIT 10;
--ALTER SERVER oracle_srv OPTIONS (ADD extensions 'oracle_fdw');
-- join two tables with FOR UPDATE clause
-- tests whole-row reference for row marks
--Testcase 164:
EXPLAIN (COSTS OFF)
SELECT t1.c1, t2.c1 FROM ft1 t1 JOIN ft2 t2 ON (t1.c1 = t2.c1) ORDER BY t1.c3, t1.c1 OFFSET 100 LIMIT 10 FOR UPDATE OF t1;
--Testcase 165:
SELECT t1.c1, t2.c1 FROM ft1 t1 JOIN ft2 t2 ON (t1.c1 = t2.c1) ORDER BY t1.c3, t1.c1 OFFSET 100 LIMIT 10 FOR UPDATE OF t1;
--Testcase 166:
EXPLAIN (COSTS OFF)
SELECT t1.c1, t2.c1 FROM ft1 t1 JOIN ft2 t2 ON (t1.c1 = t2.c1) ORDER BY t1.c3, t1.c1 OFFSET 100 LIMIT 10 FOR UPDATE;
--Testcase 167:
SELECT t1.c1, t2.c1 FROM ft1 t1 JOIN ft2 t2 ON (t1.c1 = t2.c1) ORDER BY t1.c3, t1.c1 OFFSET 100 LIMIT 10 FOR UPDATE;
-- join two tables with FOR SHARE clause
--Testcase 168:
EXPLAIN (COSTS OFF)
SELECT t1.c1, t2.c1 FROM ft1 t1 JOIN ft2 t2 ON (t1.c1 = t2.c1) ORDER BY t1.c3, t1.c1 OFFSET 100 LIMIT 10 FOR SHARE OF t1;
--Testcase 169:
SELECT t1.c1, t2.c1 FROM ft1 t1 JOIN ft2 t2 ON (t1.c1 = t2.c1) ORDER BY t1.c3, t1.c1 OFFSET 100 LIMIT 10 FOR SHARE OF t1;
--Testcase 170:
EXPLAIN (COSTS OFF)
SELECT t1.c1, t2.c1 FROM ft1 t1 JOIN ft2 t2 ON (t1.c1 = t2.c1) ORDER BY t1.c3, t1.c1 OFFSET 100 LIMIT 10 FOR SHARE;
--Testcase 171:
SELECT t1.c1, t2.c1 FROM ft1 t1 JOIN ft2 t2 ON (t1.c1 = t2.c1) ORDER BY t1.c3, t1.c1 OFFSET 100 LIMIT 10 FOR SHARE;
-- join in CTE
--Testcase 172:
EXPLAIN (COSTS OFF)
WITH t (c1_1, c1_3, c2_1) AS MATERIALIZED (SELECT t1.c1, t1.c3, t2.c1 FROM ft1 t1 JOIN ft2 t2 ON (t1.c1 = t2.c1)) SELECT c1_1, c2_1 FROM t ORDER BY c1_3, c1_1 OFFSET 100 LIMIT 10;
--Testcase 173:
WITH t (c1_1, c1_3, c2_1) AS MATERIALIZED (SELECT t1.c1, t1.c3, t2.c1 FROM ft1 t1 JOIN ft2 t2 ON (t1.c1 = t2.c1)) SELECT c1_1, c2_1 FROM t ORDER BY c1_3, c1_1 OFFSET 100 LIMIT 10;
-- ctid with whole-row reference
--Testcase 174:
EXPLAIN (COSTS OFF)
SELECT t1.ctid, t1, t2, t1.c1 FROM ft1 t1 JOIN ft2 t2 ON (t1.c1 = t2.c1) ORDER BY t1.c3, t1.c1 OFFSET 100 LIMIT 10;
-- SEMI JOIN, not pushed down
--Testcase 175:
EXPLAIN (COSTS OFF)
SELECT t1.c1 FROM ft1 t1 WHERE EXISTS (SELECT 1 FROM ft2 t2 WHERE t1.c1 = t2.c1) ORDER BY t1.c1 OFFSET 100 LIMIT 10;
--Testcase 176:
SELECT t1.c1 FROM ft1 t1 WHERE EXISTS (SELECT 1 FROM ft2 t2 WHERE t1.c1 = t2.c1) ORDER BY t1.c1 OFFSET 100 LIMIT 10;
-- ANTI JOIN, not pushed down
--Testcase 177:
EXPLAIN (COSTS OFF)
SELECT t1.c1 FROM ft1 t1 WHERE NOT EXISTS (SELECT 1 FROM ft2 t2 WHERE t1.c1 = t2.c2) ORDER BY t1.c1 OFFSET 100 LIMIT 10;
--Testcase 178:
SELECT t1.c1 FROM ft1 t1 WHERE NOT EXISTS (SELECT 1 FROM ft2 t2 WHERE t1.c1 = t2.c2) ORDER BY t1.c1 OFFSET 100 LIMIT 10;
-- CROSS JOIN can be pushed down
--Testcase 179:
EXPLAIN (COSTS OFF)
SELECT t1.c1, t2.c1 FROM ft1 t1 CROSS JOIN ft2 t2 ORDER BY t1.c1, t2.c1 OFFSET 100 LIMIT 10;
--Testcase 180:
SELECT t1.c1, t2.c1 FROM ft1 t1 CROSS JOIN ft2 t2 ORDER BY t1.c1, t2.c1 OFFSET 100 LIMIT 10;
-- different server, not pushed down. No result expected.
--Testcase 181:
EXPLAIN (COSTS OFF)
SELECT t1.c1, t2.c1 FROM ft5 t1 JOIN ft6 t2 ON (t1.c1 = t2.c1) ORDER BY t1.c1, t2.c1 OFFSET 100 LIMIT 10;
--Testcase 182:
SELECT t1.c1, t2.c1 FROM ft5 t1 JOIN ft6 t2 ON (t1.c1 = t2.c1) ORDER BY t1.c1, t2.c1 OFFSET 100 LIMIT 10;
-- unsafe join conditions (c8 has a UDT), not pushed down. Practically a CROSS
-- JOIN since c8 in both tables has same value.
--Testcase 183:
EXPLAIN (COSTS OFF)
SELECT t1.c1, t2.c1 FROM ft1 t1 LEFT JOIN ft2 t2 ON (t1.c8 = t2.c8) ORDER BY t1.c1, t2.c1 OFFSET 100 LIMIT 10;
--Testcase 184:
SELECT t1.c1, t2.c1 FROM ft1 t1 LEFT JOIN ft2 t2 ON (t1.c8 = t2.c8) ORDER BY t1.c1, t2.c1 OFFSET 100 LIMIT 10;
-- unsafe conditions on one side (c8 has a UDT), not pushed down.
--Testcase 185:
EXPLAIN (COSTS OFF)
SELECT t1.c1, t2.c1 FROM ft1 t1 LEFT JOIN ft2 t2 ON (t1.c1 = t2.c1) WHERE t1.c8 = 'foo' ORDER BY t1.c3, t1.c1 OFFSET 100 LIMIT 10;
--Testcase 186:
SELECT t1.c1, t2.c1 FROM ft1 t1 LEFT JOIN ft2 t2 ON (t1.c1 = t2.c1) WHERE t1.c8 = 'foo' ORDER BY t1.c3, t1.c1 OFFSET 100 LIMIT 10;
-- join where unsafe to pushdown condition in WHERE clause has a column not
-- in the SELECT clause. In this test unsafe clause needs to have column
-- references from both joining sides so that the clause is not pushed down
-- into one of the joining sides.
--Testcase 187:
EXPLAIN (COSTS OFF)
SELECT t1.c1, t2.c1 FROM ft1 t1 JOIN ft2 t2 ON (t1.c1 = t2.c1) WHERE t1.c8 = t2.c8 ORDER BY t1.c3, t1.c1 OFFSET 100 LIMIT 10;
--Testcase 188:
SELECT t1.c1, t2.c1 FROM ft1 t1 JOIN ft2 t2 ON (t1.c1 = t2.c1) WHERE t1.c8 = t2.c8 ORDER BY t1.c3, t1.c1 OFFSET 100 LIMIT 10;
-- Aggregate after UNION, for testing setrefs
--Testcase 189:
EXPLAIN (COSTS OFF)
SELECT t1c1, avg(t1c1 + t2c1) FROM (SELECT t1.c1, t2.c1 FROM ft1 t1 JOIN ft2 t2 ON (t1.c1 = t2.c1) UNION SELECT t1.c1, t2.c1 FROM ft1 t1 JOIN ft2 t2 ON (t1.c1 = t2.c1)) AS t (t1c1, t2c1) GROUP BY t1c1 ORDER BY t1c1 OFFSET 100 LIMIT 10;
--Testcase 190:
SELECT t1c1, avg(t1c1 + t2c1) FROM (SELECT t1.c1, t2.c1 FROM ft1 t1 JOIN ft2 t2 ON (t1.c1 = t2.c1) UNION SELECT t1.c1, t2.c1 FROM ft1 t1 JOIN ft2 t2 ON (t1.c1 = t2.c1)) AS t (t1c1, t2c1) GROUP BY t1c1 ORDER BY t1c1 OFFSET 100 LIMIT 10;
-- join with lateral reference
--Testcase 191:
EXPLAIN (COSTS OFF)
SELECT t1."c 1" FROM "S 1"."t 1" t1, LATERAL (SELECT DISTINCT t2.c1, t3.c1 FROM ft1 t2, ft2 t3 WHERE t2.c1 = t3.c1 AND t2.c2 = t1.c2) q ORDER BY t1."c 1" OFFSET 10 LIMIT 10;
--Testcase 192:
SELECT t1."c 1" FROM "S 1"."t 1" t1, LATERAL (SELECT DISTINCT t2.c1, t3.c1 FROM ft1 t2, ft2 t3 WHERE t2.c1 = t3.c1 AND t2.c2 = t1.c2) q ORDER BY t1."c 1" OFFSET 10 LIMIT 10;

-- non-Var items in targetlist of the nullable rel of a join preventing
-- push-down in some cases
-- unable to push {ft1, ft2}
--Testcase 193:
EXPLAIN (COSTS OFF)
SELECT q.a, ft2.c1 FROM (SELECT 13 FROM ft1 WHERE c1 = 13) q(a) RIGHT JOIN ft2 ON (q.a = ft2.c1) WHERE ft2.c1 BETWEEN 10 AND 15;
--Testcase 194:
SELECT q.a, ft2.c1 FROM (SELECT 13 FROM ft1 WHERE c1 = 13) q(a) RIGHT JOIN ft2 ON (q.a = ft2.c1) WHERE ft2.c1 BETWEEN 10 AND 15;

-- ok to push {ft1, ft2} but not {ft1, ft2, ft4}
--Testcase 195:
EXPLAIN (COSTS OFF)
SELECT ft4.c1, q.* FROM ft4 LEFT JOIN (SELECT 13, ft1.c1, ft2.c1 FROM ft1 RIGHT JOIN ft2 ON (ft1.c1 = ft2.c1) WHERE ft1.c1 = 12) q(a, b, c) ON (ft4.c1 = q.b) WHERE ft4.c1 BETWEEN 10 AND 15 ORDER BY ft4.c1;
--Testcase 196:
SELECT ft4.c1, q.* FROM ft4 LEFT JOIN (SELECT 13, ft1.c1, ft2.c1 FROM ft1 RIGHT JOIN ft2 ON (ft1.c1 = ft2.c1) WHERE ft1.c1 = 12) q(a, b, c) ON (ft4.c1 = q.b) WHERE ft4.c1 BETWEEN 10 AND 15 ORDER BY ft4.c1;

-- join with nullable side with some columns with null values
--Testcase 197:
UPDATE ft5 SET c3 = null where c1 % 9 = 0;
--Testcase 198:
EXPLAIN (COSTS OFF)
SELECT ft5, ft5.c1, ft5.c2, ft5.c3, ft4.c1, ft4.c2 FROM ft5 left join ft4 on ft5.c1 = ft4.c1 WHERE ft4.c1 BETWEEN 10 and 30 ORDER BY ft5.c1, ft4.c1;
--Testcase 199:
SELECT ft5, ft5.c1, ft5.c2, ft5.c3, ft4.c1, ft4.c2 FROM ft5 left join ft4 on ft5.c1 = ft4.c1 WHERE ft4.c1 BETWEEN 10 and 30 ORDER BY ft5.c1, ft4.c1;

-- multi-way join involving multiple merge joins
-- (this case used to have EPQ-related planning problems)
--Testcase 200:
CREATE TABLE local_tbl (c1 int NOT NULL, c2 int NOT NULL, c3 text, CONSTRAINT local_tbl_pkey PRIMARY KEY (c1));
--Testcase 201:
INSERT INTO local_tbl SELECT id, id % 10, to_char(id, 'FM0000') FROM generate_series(1, 1000) id;
ANALYZE local_tbl;
--Testcase 202:
SET enable_nestloop TO false;
--Testcase 203:
SET enable_hashjoin TO false;
--Testcase 204:
EXPLAIN (COSTS OFF)
SELECT * FROM ft1, ft2, ft4, ft5, local_tbl WHERE ft1.c1 = ft2.c1 AND ft1.c2 = ft4.c1
    AND ft1.c2 = ft5.c1 AND ft1.c2 = local_tbl.c1 AND ft1.c1 < 100 AND ft2.c1 < 100 ORDER BY ft1.c1 FOR UPDATE;
--Testcase 205:
SELECT * FROM ft1, ft2, ft4, ft5, local_tbl WHERE ft1.c1 = ft2.c1 AND ft1.c2 = ft4.c1
    AND ft1.c2 = ft5.c1 AND ft1.c2 = local_tbl.c1 AND ft1.c1 < 100 AND ft2.c1 < 100 ORDER BY ft1.c1 FOR UPDATE;
--Testcase 206:
RESET enable_nestloop;
--Testcase 207:
RESET enable_hashjoin;
--Testcase 208:
DROP TABLE local_tbl;

-- -- check join pushdown in situations where multiple userids are involved
-- CREATE ROLE regress_view_owner SUPERUSER;
-- CREATE USER MAPPING FOR regress_view_owner SERVER oracle_srv;
-- GRANT SELECT ON ft4 TO regress_view_owner;
-- GRANT SELECT ON ft5 TO regress_view_owner;

-- CREATE VIEW v4 AS SELECT * FROM ft4;
-- CREATE VIEW v5 AS SELECT * FROM ft5;
-- ALTER VIEW v5 OWNER TO regress_view_owner;
-- EXPLAIN (COSTS OFF)
-- SELECT t1.c1, t2.c2 FROM v4 t1 LEFT JOIN v5 t2 ON (t1.c1 = t2.c1) ORDER BY t1.c1, t2.c1 OFFSET 10 LIMIT 10;  -- can't be pushed down, different view owners
-- SELECT t1.c1, t2.c2 FROM v4 t1 LEFT JOIN v5 t2 ON (t1.c1 = t2.c1) ORDER BY t1.c1, t2.c1 OFFSET 10 LIMIT 10;
-- ALTER VIEW v4 OWNER TO regress_view_owner;
-- EXPLAIN (COSTS OFF)
-- SELECT t1.c1, t2.c2 FROM v4 t1 LEFT JOIN v5 t2 ON (t1.c1 = t2.c1) ORDER BY t1.c1, t2.c1 OFFSET 10 LIMIT 10;  -- can be pushed down
-- SELECT t1.c1, t2.c2 FROM v4 t1 LEFT JOIN v5 t2 ON (t1.c1 = t2.c1) ORDER BY t1.c1, t2.c1 OFFSET 10 LIMIT 10;

-- EXPLAIN (COSTS OFF)
-- SELECT t1.c1, t2.c2 FROM v4 t1 LEFT JOIN ft5 t2 ON (t1.c1 = t2.c1) ORDER BY t1.c1, t2.c1 OFFSET 10 LIMIT 10;  -- can't be pushed down, view owner not current user
-- SELECT t1.c1, t2.c2 FROM v4 t1 LEFT JOIN ft5 t2 ON (t1.c1 = t2.c1) ORDER BY t1.c1, t2.c1 OFFSET 10 LIMIT 10;
-- ALTER VIEW v4 OWNER TO CURRENT_USER;
-- EXPLAIN (COSTS OFF)
-- SELECT t1.c1, t2.c2 FROM v4 t1 LEFT JOIN ft5 t2 ON (t1.c1 = t2.c1) ORDER BY t1.c1, t2.c1 OFFSET 10 LIMIT 10;  -- can be pushed down
-- SELECT t1.c1, t2.c2 FROM v4 t1 LEFT JOIN ft5 t2 ON (t1.c1 = t2.c1) ORDER BY t1.c1, t2.c1 OFFSET 10 LIMIT 10;
-- ALTER VIEW v4 OWNER TO regress_view_owner;

-- -- cleanup
-- DROP OWNED BY regress_view_owner;
-- DROP ROLE regress_view_owner;

-- ===================================================================
-- Aggregate and grouping queries
-- ===================================================================

-- Simple aggregates
--Testcase 209:
explain (costs off)
select count(c6), sum(c1), avg(c1), min(c2), max(c1), stddev(c2), sum(c1) * (random() <= 1)::int as sum2 from ft1 where c2 < 5 group by c2 order by 1, 2;
--Testcase 210:
select count(c6), sum(c1), avg(c1), min(c2), max(c1), stddev(c2), sum(c1) * (random() <= 1)::int as sum2 from ft1 where c2 < 5 group by c2 order by 1, 2;

--Testcase 211:
explain (costs off)
select count(c6), sum(c1), avg(c1), min(c2), max(c1), stddev(c2), sum(c1) * (random() <= 1)::int as sum2 from ft1 where c2 < 5 group by c2 order by 1, 2 limit 1;
--Testcase 212:
select count(c6), sum(c1), avg(c1), min(c2), max(c1), stddev(c2), sum(c1) * (random() <= 1)::int as sum2 from ft1 where c2 < 5 group by c2 order by 1, 2 limit 1;

-- Aggregate is not pushed down as aggregation contains random()
--Testcase 213:
explain (costs off)
select sum(c1 * (random() <= 1)::int) as sum, avg(c1) from ft1;

-- Aggregate over join query
--Testcase 214:
explain (costs off)
select count(*), sum(t1.c1), avg(t2.c1) from ft1 t1 inner join ft1 t2 on (t1.c2 = t2.c2) where t1.c2 = 6;
--Testcase 215:
select count(*), sum(t1.c1), avg(t2.c1) from ft1 t1 inner join ft1 t2 on (t1.c2 = t2.c2) where t1.c2 = 6;

-- Not pushed down due to local conditions present in underneath input rel
--Testcase 216:
explain (costs off)
select sum(t1.c1), count(t2.c1) from ft1 t1 inner join ft2 t2 on (t1.c1 = t2.c1) where ((t1.c1 * t2.c1)/(t1.c1 * t2.c1)) * random() <= 1;

-- GROUP BY clause having expressions
-- todo: support pushdown div operator
--Testcase 217:
explain (costs off)
select c2/2, sum(c2) * (c2/2) from ft1 group by c2/2 order by c2/2;
--Testcase 218:
select c2/2, sum(c2) * (c2/2) from ft1 group by c2/2 order by c2/2;

-- Aggregates in subquery are pushed down.
--Testcase 219:
explain (costs off)
select count(x.a), sum(x.a) from (select c2 a, sum(c1) b from ft1 group by c2, sqrt(c1) order by 1, 2) x;
--Testcase 220:
select count(x.a), sum(x.a) from (select c2 a, sum(c1) b from ft1 group by c2, sqrt(c1) order by 1, 2) x;

-- Aggregate is not pushed down by taking unshippable expression out
-- oracle does not support random()
--Testcase 221:
explain (costs off)
select c2 * (random() <= 1)::int as sum1, sum(c1) * c2 as sum2 from ft1 group by c2 order by 1, 2;
--Testcase 222:
select c2 * (random() <= 1)::int as sum1, sum(c1) * c2 as sum2 from ft1 group by c2 order by 1, 2;

-- Aggregate with unshippable GROUP BY clause are not pushed
--Testcase 223:
explain (costs off)
select c2 * (random() <= 1)::int as c2 from ft2 group by c2 * (random() <= 1)::int order by 1;

-- GROUP BY clause in various forms, cardinal, alias and constant expression
--Testcase 224:
explain (costs off)
select count(c2) w, c2 x, 5 y, 7.0 z from ft1 group by 2, y, 9.0::int order by 2;
--Testcase 225:
select count(c2) w, c2 x, 5 y, 7.0 z from ft1 group by 2, y, 9.0::int order by 2;

-- GROUP BY clause referring to same column multiple times
-- Also, ORDER BY contains an aggregate function
--Testcase 226:
explain (costs off)
select c2, c2 from ft1 where c2 > 6 group by 1, 2 order by sum(c1);
--Testcase 227:
select c2, c2 from ft1 where c2 > 6 group by 1, 2 order by sum(c1);

-- Testing HAVING clause shippability
--Testcase 228:
explain (costs off)
select c2, sum(c1) from ft2 group by c2 having avg(c1) < 500 and sum(c1) < 49800 order by c2;
--Testcase 229:
select c2, sum(c1) from ft2 group by c2 having avg(c1) < 500 and sum(c1) < 49800 order by c2;

-- Unshippable HAVING clause will be evaluated locally, and other qual in HAVING clause is pushed down
--Testcase 230:
explain (costs off)
select count(*) from (select c5, count(c1) from ft1 group by c5, sqrt(c2) having (avg(c1) / avg(c1)) * random() <= 1 and avg(c1) < 500) x;
--Testcase 231:
select count(*) from (select c5, count(c1) from ft1 group by c5, sqrt(c2) having (avg(c1) / avg(c1)) * random() <= 1 and avg(c1) < 500) x;

-- Aggregate in HAVING clause is not pushable, and thus aggregation is not pushed down
--Testcase 232:
explain (costs off)
select sum(c1) from ft1 group by c2 having avg(c1 * (random() <= 1)::int) > 100 order by 1;

-- Remote aggregate in combination with a local Param (for the output
-- of an initplan) can be trouble, per bug #15781
--Testcase 233:
explain (costs off)
select exists(select 1 from pg_enum), sum(c1) from ft1;
--Testcase 234:
select exists(select 1 from pg_enum), sum(c1) from ft1;

--Testcase 235:
explain (costs off)
select exists(select 1 from pg_enum), sum(c1) from ft1 group by 1;
--Testcase 236:
select exists(select 1 from pg_enum), sum(c1) from ft1 group by 1;


-- Testing ORDER BY, DISTINCT, FILTER, Ordered-sets and VARIADIC within aggregates

-- ORDER BY within aggregate, same column used to order
--Testcase 237:
explain (costs off)
select array_agg(c1 order by c1) from ft1 where c1 < 100 group by c2 order by 1;
--Testcase 238:
select array_agg(c1 order by c1) from ft1 where c1 < 100 group by c2 order by 1;

-- ORDER BY within aggregate, different column used to order also using DESC
--Testcase 239:
explain (costs off)
select array_agg(c5 order by c1 desc) from ft2 where c2 = 6 and c1 < 50;
--Testcase 240:
select array_agg(c5 order by c1 desc) from ft2 where c2 = 6 and c1 < 50;

-- DISTINCT within aggregate
--Testcase 241:
explain (costs off)
select array_agg(distinct (t1.c1)%5) from ft4 t1 full join ft5 t2 on (t1.c1 = t2.c1) where t1.c1 < 20 or (t1.c1 is null and t2.c1 < 5) group by (t2.c1)%3 order by 1;
--Testcase 242:
select array_agg(distinct (t1.c1)%5) from ft4 t1 full join ft5 t2 on (t1.c1 = t2.c1) where t1.c1 < 20 or (t1.c1 is null and t2.c1 < 5) group by (t2.c1)%3 order by 1;

-- DISTINCT combined with ORDER BY within aggregate
--Testcase 243:
explain (costs off)
select array_agg(distinct (t1.c1)%5 order by (t1.c1)%5) from ft4 t1 full join ft5 t2 on (t1.c1 = t2.c1) where t1.c1 < 20 or (t1.c1 is null and t2.c1 < 5) group by (t2.c1)%3 order by 1;
--Testcase 244:
select array_agg(distinct (t1.c1)%5 order by (t1.c1)%5) from ft4 t1 full join ft5 t2 on (t1.c1 = t2.c1) where t1.c1 < 20 or (t1.c1 is null and t2.c1 < 5) group by (t2.c1)%3 order by 1;

--Testcase 245:
explain (costs off)
select array_agg(distinct (t1.c1)%5 order by (t1.c1)%5 desc nulls last) from ft4 t1 full join ft5 t2 on (t1.c1 = t2.c1) where t1.c1 < 20 or (t1.c1 is null and t2.c1 < 5) group by (t2.c1)%3 order by 1;
--Testcase 246:
select array_agg(distinct (t1.c1)%5 order by (t1.c1)%5 desc nulls last) from ft4 t1 full join ft5 t2 on (t1.c1 = t2.c1) where t1.c1 < 20 or (t1.c1 is null and t2.c1 < 5) group by (t2.c1)%3 order by 1;

-- FILTER within aggregate
--Testcase 247:
explain (costs off)
select sum(c1) filter (where c1 < 100 and c2 > 5) from ft1 group by c2 order by 1 nulls last;
--Testcase 248:
select sum(c1) filter (where c1 < 100 and c2 > 5) from ft1 group by c2 order by 1 nulls last;

-- DISTINCT, ORDER BY and FILTER within aggregate
--Testcase 249:
explain (costs off)
select sum(c1%3), sum(distinct c1%3 order by c1%3) filter (where c1%3 < 2), c2 from ft1 where c2 = 6 group by c2;
--Testcase 250:
select sum(c1%3), sum(distinct c1%3 order by c1%3) filter (where c1%3 < 2), c2 from ft1 where c2 = 6 group by c2;

-- Outer query is aggregation query
--Testcase 251:
explain (costs off)
select distinct (select count(*) filter (where t2.c2 = 6 and t2.c1 < 10) from ft1 t1 where t1.c1 = 6) from ft2 t2 where t2.c2 % 6 = 0 order by 1;
--Testcase 252:
select distinct (select count(*) filter (where t2.c2 = 6 and t2.c1 < 10) from ft1 t1 where t1.c1 = 6) from ft2 t2 where t2.c2 % 6 = 0 order by 1;
-- Inner query is aggregation query
--Testcase 253:
explain (costs off)
select distinct (select count(t1.c1) filter (where t2.c2 = 6 and t2.c1 < 10) from ft1 t1 where t1.c1 = 6) from ft2 t2 where t2.c2 % 6 = 0 order by 1;
--Testcase 254:
select distinct (select count(t1.c1) filter (where t2.c2 = 6 and t2.c1 < 10) from ft1 t1 where t1.c1 = 6) from ft2 t2 where t2.c2 % 6 = 0 order by 1;

-- Aggregate not pushed down as FILTER condition is not pushable
--Testcase 255:
explain (costs off)
select sum(c1) filter (where (c1 / c1) * random() <= 1) from ft1 group by c2 order by 1;
--Testcase 256:
explain (costs off)
select sum(c2) filter (where c2 in (select c2 from ft1 where c2 < 5)) from ft1;

-- Ordered-sets within aggregate
--Testcase 257:
explain (costs off)
select c2, rank('10'::varchar) within group (order by c6), percentile_cont(c2/10::numeric) within group (order by c1) from ft1 where c2 < 10 group by c2 having percentile_cont(c2/10::numeric) within group (order by c1) < 500 order by c2;
--Testcase 258:
select c2, rank('10'::varchar) within group (order by c6), percentile_cont(c2/10::numeric) within group (order by c1) from ft1 where c2 < 10 group by c2 having percentile_cont(c2/10::numeric) within group (order by c1) < 500 order by c2;

-- Using multiple arguments within aggregates
--Testcase 259:
explain (costs off)
select c1, rank(c1, c2) within group (order by c1, c2) from ft1 group by c1, c2 having c1 = 6 order by 1;
--Testcase 260:
select c1, rank(c1, c2) within group (order by c1, c2) from ft1 group by c1, c2 having c1 = 6 order by 1;

-- User defined function for user defined aggregate, VARIADIC
--Testcase 261:
create function least_accum(anyelement, variadic anyarray)
returns anyelement language sql as
  'select least($1, min($2[i])) from generate_subscripts($2,1) g(i)';
--Testcase 262:
create aggregate least_agg(variadic items anyarray) (
  stype = anyelement, sfunc = least_accum
);

-- Disable hash aggregation for plan stability.
--Testcase 263:
set enable_hashagg to false;

-- Not pushed down due to user defined aggregate
--Testcase 264:
explain (costs off)
select c2, least_agg(c1) from ft1 group by c2 order by c2;

-- Add function and aggregate into extension
--Testcase 265:
alter extension oracle_fdw add function least_accum(anyelement, variadic anyarray);
--Testcase 266:
alter extension oracle_fdw add aggregate least_agg(variadic items anyarray);
--alter server oracle_srv options (set extensions 'oracle_fdw');

-- Now aggregate will be pushed.  Aggregate will display VARIADIC argument.
--Testcase 267:
explain (costs off)
select c2, least_agg(c1) from ft1 where c2 < 100 group by c2 order by c2;
--Testcase 268:
select c2, least_agg(c1) from ft1 where c2 < 100 group by c2 order by c2;

-- Remove function and aggregate from extension
--Testcase 269:
alter extension oracle_fdw drop function least_accum(anyelement, variadic anyarray);
--Testcase 270:
alter extension oracle_fdw drop aggregate least_agg(variadic items anyarray);
--alter server oracle_srv options (set extensions 'oracle_fdw');

-- Not pushed down as we have dropped objects from extension.
--Testcase 271:
explain (costs off)
select c2, least_agg(c1) from ft1 group by c2 order by c2;

-- Cleanup
--Testcase 272:
reset enable_hashagg;
--Testcase 273:
drop aggregate least_agg(variadic items anyarray);
--Testcase 274:
drop function least_accum(anyelement, variadic anyarray);

-- Testing USING OPERATOR() in ORDER BY within aggregate.
-- For this, we need user defined operators along with operator family and
-- operator class.  Create those and then add them in extension.  Note that
-- user defined objects are considered unshippable unless they are part of
-- the extension.
--Testcase 275:
create operator public.<^ (
 leftarg = int4,
 rightarg = int4,
 procedure = int4eq
);

--Testcase 276:
create operator public.=^ (
 leftarg = int4,
 rightarg = int4,
 procedure = int4lt
);

--Testcase 277:
create operator public.>^ (
 leftarg = int4,
 rightarg = int4,
 procedure = int4gt
);

--Testcase 278:
create operator family my_op_family using btree;

--Testcase 279:
create function my_op_cmp(a int, b int) returns int as
  $$begin return btint4cmp(a, b); end $$ language plpgsql;

--Testcase 280:
create operator class my_op_class for type int using btree family my_op_family as
 operator 1 public.<^,
 operator 3 public.=^,
 operator 5 public.>^,
 function 1 my_op_cmp(int, int);

-- This will not be pushed as user defined sort operator is not part of the
-- extension yet.
--Testcase 281:
explain (costs off)
select array_agg(c1 order by c1 using operator(public.<^)) from ft2 where c2 = 6 and c1 < 100 group by c2;

-- Update local stats on ft2
ANALYZE ft2;

-- Add into extension
--Testcase 282:
alter extension oracle_fdw add operator class my_op_class using btree;
--Testcase 283:
alter extension oracle_fdw add function my_op_cmp(a int, b int);
--Testcase 284:
alter extension oracle_fdw add operator family my_op_family using btree;
--Testcase 285:
alter extension oracle_fdw add operator public.<^(int, int);
--Testcase 286:
alter extension oracle_fdw add operator public.=^(int, int);
--Testcase 287:
alter extension oracle_fdw add operator public.>^(int, int);
--alter server oracle_srv options (set extensions 'oracle_fdw');

-- Now this will be pushed as sort operator is part of the extension.
--Testcase 288:
explain (costs off)
select array_agg(c1 order by c1 using operator(public.<^)) from ft2 where c2 = 6 and c1 < 100 group by c2;
--Testcase 289:
select array_agg(c1 order by c1 using operator(public.<^)) from ft2 where c2 = 6 and c1 < 100 group by c2;

-- Remove from extension
--Testcase 290:
alter extension oracle_fdw drop operator class my_op_class using btree;
--Testcase 291:
alter extension oracle_fdw drop function my_op_cmp(a int, b int);
--Testcase 292:
alter extension oracle_fdw drop operator family my_op_family using btree;
--Testcase 293:
alter extension oracle_fdw drop operator public.<^(int, int);
--Testcase 294:
alter extension oracle_fdw drop operator public.=^(int, int);
--Testcase 295:
alter extension oracle_fdw drop operator public.>^(int, int);
--alter server oracle_srv options (set extensions 'oracle_fdw');

-- This will not be pushed as sort operator is now removed from the extension.
--Testcase 296:
explain (costs off)
select array_agg(c1 order by c1 using operator(public.<^)) from ft2 where c2 = 6 and c1 < 100 group by c2;

-- Cleanup
--Testcase 297:
drop operator class my_op_class using btree;
--Testcase 298:
drop function my_op_cmp(a int, b int);
--Testcase 299:
drop operator family my_op_family using btree;
--Testcase 300:
drop operator public.>^(int, int);
--Testcase 301:
drop operator public.=^(int, int);
--Testcase 302:
drop operator public.<^(int, int);

-- Input relation to aggregate push down hook is not safe to pushdown and thus
-- the aggregate cannot be pushed down to foreign server.
--Testcase 303:
explain (costs off)
select count(t1.c3) from ft2 t1 left join ft2 t2 on (t1.c1 = random() * t2.c2);

-- Subquery in FROM clause having aggregate
--Testcase 304:
explain (costs off)
select count(*), x.b from ft1, (select c2 a, sum(c1) b from ft1 group by c2) x where ft1.c2 = x.a group by x.b order by 1, 2;
--Testcase 305:
select count(*), x.b from ft1, (select c2 a, sum(c1) b from ft1 group by c2) x where ft1.c2 = x.a group by x.b order by 1, 2;

-- FULL join with IS NULL check in HAVING
--Testcase 306:
explain (costs off)
select avg(t1.c1), sum(t2.c1) from ft4 t1 full join ft5 t2 on (t1.c1 = t2.c1) group by t2.c1 having (avg(t1.c1) is null and sum(t2.c1) < 10) or sum(t2.c1) is null order by 1 nulls last, 2;
--Testcase 307:
select avg(t1.c1), sum(t2.c1) from ft4 t1 full join ft5 t2 on (t1.c1 = t2.c1) group by t2.c1 having (avg(t1.c1) is null and sum(t2.c1) < 10) or sum(t2.c1) is null order by 1 nulls last, 2;

-- Aggregate over FULL join needing to deparse the joining relations as
-- subqueries.
--Testcase 308:
explain (costs off)
select count(*), sum(t1.c1), avg(t2.c1) from (select c1 from ft4 where c1 between 50 and 60) t1 full join (select c1 from ft5 where c1 between 50 and 60) t2 on (t1.c1 = t2.c1);
--Testcase 309:
select count(*), sum(t1.c1), avg(t2.c1) from (select c1 from ft4 where c1 between 50 and 60) t1 full join (select c1 from ft5 where c1 between 50 and 60) t2 on (t1.c1 = t2.c1);

-- ORDER BY expression is part of the target list but not pushed down to
-- foreign server.
--Testcase 310:
explain (costs off)
select sum(c2) * (random() <= 1)::int as sum from ft1 order by 1;
--Testcase 311:
select sum(c2) * (random() <= 1)::int as sum from ft1 order by 1;

-- LATERAL join, with parameterization
--Testcase 312:
set enable_hashagg to false;
--Testcase 313:
explain (costs off)
select c2, sum from "S 1"."t 1" t1, lateral (select sum(t2.c1 + t1."c 1") sum from ft2 t2 group by t2.c1) qry where t1.c2 * 2 = qry.sum and t1.c2 < 3 and t1."c 1" < 100 order by 1;
--Testcase 314:
select c2, sum from "S 1"."t 1" t1, lateral (select sum(t2.c1 + t1."c 1") sum from ft2 t2 group by t2.c1) qry where t1.c2 * 2 = qry.sum and t1.c2 < 3 and t1."c 1" < 100 order by 1;
--Testcase 315:
reset enable_hashagg;

-- bug #15613: bad plan for foreign table scan with lateral reference
--Testcase 316:
EXPLAIN (COSTS OFF)
SELECT ref_0.c2, subq_1.*
FROM
    "S 1"."t 1" AS ref_0,
    LATERAL (
        SELECT ref_0."c 1" c1, subq_0.*
        FROM (SELECT ref_0.c2, ref_1.c3
              FROM ft1 AS ref_1) AS subq_0
             RIGHT JOIN ft2 AS ref_3 ON (subq_0.c3 = ref_3.c3)
    ) AS subq_1
WHERE ref_0."c 1" < 10 AND subq_1.c3 = '00001'
ORDER BY ref_0."c 1";

--Testcase 317:
SELECT ref_0.c2, subq_1.*
FROM
    "S 1"."t 1" AS ref_0,
    LATERAL (
        SELECT ref_0."c 1" c1, subq_0.*
        FROM (SELECT ref_0.c2, ref_1.c3
              FROM ft1 AS ref_1) AS subq_0
             RIGHT JOIN ft2 AS ref_3 ON (subq_0.c3 = ref_3.c3)
    ) AS subq_1
WHERE ref_0."c 1" < 10 AND subq_1.c3 = '00001'
ORDER BY ref_0."c 1";

-- Check with placeHolderVars
--Testcase 318:
explain (costs off)
select sum(q.a), count(q.b) from ft4 left join (select 13, avg(ft1.c1), sum(ft2.c1) from ft1 right join ft2 on (ft1.c1 = ft2.c1)) q(a, b, c) on (ft4.c1 <= q.b);
--Testcase 319:
select sum(q.a), count(q.b) from ft4 left join (select 13, avg(ft1.c1), sum(ft2.c1) from ft1 right join ft2 on (ft1.c1 = ft2.c1)) q(a, b, c) on (ft4.c1 <= q.b);

-- Not supported cases
-- Grouping sets
--Testcase 320:
explain (costs off)
select c2, sum(c1) from ft1 where c2 < 3 group by rollup(c2) order by 1 nulls last;
--Testcase 321:
select c2, sum(c1) from ft1 where c2 < 3 group by rollup(c2) order by 1 nulls last;
--Testcase 322:
explain (costs off)
select c2, sum(c1) from ft1 where c2 < 3 group by cube(c2) order by 1 nulls last;
--Testcase 323:
select c2, sum(c1) from ft1 where c2 < 3 group by cube(c2) order by 1 nulls last;
--Testcase 324:
explain (costs off)
select c2, c6, sum(c1) from ft1 where c2 < 3 group by grouping sets(c2, c6) order by 1 nulls last, 2 nulls last;
--Testcase 325:
select c2, c6, sum(c1) from ft1 where c2 < 3 group by grouping sets(c2, c6) order by 1 nulls last, 2 nulls last;
--Testcase 326:
explain (costs off)
select c2, sum(c1), grouping(c2) from ft1 where c2 < 3 group by c2 order by 1 nulls last;
--Testcase 327:
select c2, sum(c1), grouping(c2) from ft1 where c2 < 3 group by c2 order by 1 nulls last;

-- DISTINCT itself is not pushed down, whereas underneath aggregate is pushed
--Testcase 328:
explain (costs off)
select distinct sum(c1)/1000 s from ft2 where c2 < 6 group by c2 order by 1;
--Testcase 329:
select distinct sum(c1)/1000 s from ft2 where c2 < 6 group by c2 order by 1;

-- WindowAgg
--Testcase 330:
explain (costs off)
select c2, sum(c2), count(c2) over (partition by c2%2) from ft2 where c2 < 10 group by c2 order by 1;
--Testcase 331:
select c2, sum(c2), count(c2) over (partition by c2%2) from ft2 where c2 < 10 group by c2 order by 1;
--Testcase 332:
explain (costs off)
select c2, array_agg(c2) over (partition by c2%2 order by c2 desc) from ft1 where c2 < 10 group by c2 order by 1;
--Testcase 333:
select c2, array_agg(c2) over (partition by c2%2 order by c2 desc) from ft1 where c2 < 10 group by c2 order by 1;
--Testcase 334:
explain (costs off)
select c2, array_agg(c2) over (partition by c2%2 order by c2 range between current row and unbounded following) from ft1 where c2 < 10 group by c2 order by 1;
--Testcase 335:
select c2, array_agg(c2) over (partition by c2%2 order by c2 range between current row and unbounded following) from ft1 where c2 < 10 group by c2 order by 1;


-- ===================================================================
-- parameterized queries
-- ===================================================================
-- simple join
--Testcase 336:
PREPARE st1(int, int) AS SELECT t1.c3, t2.c3 FROM ft1 t1, ft2 t2 WHERE t1.c1 = $1 AND t2.c1 = $2;
--Testcase 337:
EXPLAIN (COSTS OFF) EXECUTE st1(1, 2);
--Testcase 338:
EXECUTE st1(1, 1);
--Testcase 339:
EXECUTE st1(101, 101);
-- subquery using stable function (can't be sent to remote)
--Testcase 340:
PREPARE st2(int) AS SELECT * FROM ft1 t1 WHERE t1.c1 < $2 AND t1.c3 IN (SELECT c3 FROM ft2 t2 WHERE c1 > $1 AND date(c4) = '1970-01-17'::date) ORDER BY c1;
--Testcase 341:
EXPLAIN (COSTS OFF) EXECUTE st2(10, 20);
--Testcase 342:
EXECUTE st2(10, 20);
--Testcase 343:
EXECUTE st2(101, 121);
-- subquery using immutable function (can be sent to remote)
--Testcase 344:
PREPARE st3(int) AS SELECT * FROM ft1 t1 WHERE t1.c1 < $2 AND t1.c3 IN (SELECT c3 FROM ft2 t2 WHERE c1 > $1 AND date(c5) = '1970-01-17'::date) ORDER BY c1;
--Testcase 345:
EXPLAIN (COSTS OFF) EXECUTE st3(10, 20);
--Testcase 346:
EXECUTE st3(10, 20);
--Testcase 347:
EXECUTE st3(20, 30);
-- custom plan should be chosen initially
--Testcase 348:
PREPARE st4(int) AS SELECT * FROM ft1 t1 WHERE t1.c1 = $1;
--Testcase 349:
EXPLAIN (COSTS OFF) EXECUTE st4(1);
--Testcase 350:
EXPLAIN (COSTS OFF) EXECUTE st4(1);
--Testcase 351:
EXPLAIN (COSTS OFF) EXECUTE st4(1);
--Testcase 352:
EXPLAIN (COSTS OFF) EXECUTE st4(1);
--Testcase 353:
EXPLAIN (COSTS OFF) EXECUTE st4(1);
-- once we try it enough times, should switch to generic plan
--Testcase 354:
EXPLAIN (COSTS OFF) EXECUTE st4(1);
-- value of $1 should not be sent to remote
--Testcase 355:
PREPARE st5(text,int) AS SELECT * FROM ft1 t1 WHERE c8 = $1 and c1 = $2;
--Testcase 356:
EXPLAIN (COSTS OFF) EXECUTE st5('foo', 1);
--Testcase 357:
EXPLAIN (COSTS OFF) EXECUTE st5('foo', 1);
--Testcase 358:
EXPLAIN (COSTS OFF) EXECUTE st5('foo', 1);
--Testcase 359:
EXPLAIN (COSTS OFF) EXECUTE st5('foo', 1);
--Testcase 360:
EXPLAIN (COSTS OFF) EXECUTE st5('foo', 1);
--Testcase 361:
EXPLAIN (COSTS OFF) EXECUTE st5('foo', 1);
--Testcase 362:
EXECUTE st5('foo', 1);

-- altering FDW options requires replanning
--Testcase 363:
PREPARE st6 AS SELECT * FROM ft1 t1 WHERE t1.c1 = t1.c2;
--Testcase 364:
EXPLAIN (COSTS OFF) EXECUTE st6;
--Testcase 365:
PREPARE st7 AS INSERT INTO ft1 (c1,c2,c3) VALUES (1001,101,'foo');
--Testcase 366:
EXPLAIN (COSTS OFF) EXECUTE st7;
-- ALTER TABLE "S 1"."t 1" RENAME TO "t 0";
--Testcase 367:
SELECT oracle_execute(
          'oracle_srv',
          E'RENAME "T 1" TO "T 0"'
       );
--Testcase 368:
ALTER FOREIGN TABLE ft1 OPTIONS (SET table 'T 0');
--Testcase 369:
EXPLAIN (COSTS OFF) EXECUTE st6;
--Testcase 370:
EXECUTE st6;
--Testcase 371:
EXPLAIN (COSTS OFF) EXECUTE st7;
-- ALTER TABLE "S 1"."t 0" RENAME TO "t 1";
--Testcase 372:
SELECT oracle_execute(
          'oracle_srv',
          E'RENAME "T 0" TO "T 1"'
       );
--Testcase 373:
ALTER FOREIGN TABLE ft1 OPTIONS (SET table 'T 1');

--Testcase 374:
PREPARE st8 AS SELECT count(c3) FROM ft1 t1 WHERE t1.c1 === t1.c2;
--Testcase 375:
EXPLAIN (COSTS OFF) EXECUTE st8;
-- ALTER SERVER oracle_srv OPTIONS (DROP extensions);
--Testcase 376:
EXPLAIN (COSTS OFF) EXECUTE st8;
--Testcase 377:
EXECUTE st8;
-- ALTER SERVER oracle_srv OPTIONS (ADD extensions 'oracle_fdw');

-- cleanup
DEALLOCATE st1;
DEALLOCATE st2;
DEALLOCATE st3;
DEALLOCATE st4;
DEALLOCATE st5;
DEALLOCATE st6;
DEALLOCATE st7;
DEALLOCATE st8;

-- System columns, except ctid and oid, should not be sent to remote
--Testcase 378:
EXPLAIN (COSTS OFF)
SELECT * FROM ft1 t1 WHERE t1.tableoid = 'pg_class'::regclass LIMIT 1;
--Testcase 379:
SELECT * FROM ft1 t1 WHERE t1.tableoid = 'ft1'::regclass ORDER BY t1.c1 LIMIT 1;
--Testcase 380:
EXPLAIN (COSTS OFF)
SELECT tableoid::regclass, * FROM ft1 t1 LIMIT 1;
--Testcase 381:
SELECT tableoid::regclass, * FROM ft1 t1 ORDER BY t1.c1 LIMIT 1;
--Testcase 382:
EXPLAIN (COSTS OFF)
SELECT * FROM ft1 t1 WHERE t1.ctid = '(0,2)';
--Testcase 383:
SELECT * FROM ft1 t1 WHERE t1.ctid = '(0,2)';
--Testcase 384:
EXPLAIN (COSTS OFF)
SELECT ctid, * FROM ft1 t1 LIMIT 1;
--Testcase 385:
SELECT ctid, * FROM ft1 t1 ORDER BY t1.c1 LIMIT 1;

-- ===================================================================
-- used in PL/pgSQL function
-- ===================================================================
--Testcase 386:
CREATE OR REPLACE FUNCTION f_test(p_c1 int) RETURNS int AS $$
DECLARE
	v_c1 int;
BEGIN
--Testcase 387:
    SELECT c1 INTO v_c1 FROM ft1 WHERE c1 = p_c1 LIMIT 1;
    PERFORM c1 FROM ft1 WHERE c1 = p_c1 AND p_c1 = v_c1 LIMIT 1;
    RETURN v_c1;
END;
$$ LANGUAGE plpgsql;
--Testcase 388:
SELECT f_test(100);
--Testcase 389:
DROP FUNCTION f_test(int);

-- ===================================================================
-- REINDEX
-- ===================================================================
-- remote table is not created here
--Testcase 390:
CREATE FOREIGN TABLE reindex_foreign (c1 int, c2 int)
  SERVER oracle_srv2 OPTIONS (table 'reindex_local');
REINDEX TABLE reindex_foreign; -- error
REINDEX TABLE CONCURRENTLY reindex_foreign; -- error
--Testcase 391:
DROP FOREIGN TABLE reindex_foreign;
-- partitions and foreign tables
--Testcase 392:
CREATE TABLE reind_fdw_parent (c1 int) PARTITION BY RANGE (c1);
--Testcase 393:
CREATE TABLE reind_fdw_0_10 PARTITION OF reind_fdw_parent
  FOR VALUES FROM (0) TO (10);
--Testcase 394:
CREATE FOREIGN TABLE reind_fdw_10_20 PARTITION OF reind_fdw_parent
  FOR VALUES FROM (10) TO (20)
  SERVER oracle_srv OPTIONS (table 'reind_local_10_20');
REINDEX TABLE reind_fdw_parent; -- ok
REINDEX TABLE CONCURRENTLY reind_fdw_parent; -- ok
--Testcase 395:
DROP TABLE reind_fdw_parent;

-- ===================================================================
-- conversion error
-- ===================================================================
--Testcase 396:
ALTER FOREIGN TABLE ft1 ALTER COLUMN c8 TYPE int;
--Testcase 397:
SELECT * FROM ft1 ftx(x1,x2,x3,x4,x5,x6,x7,x8) WHERE x1 = 1;  -- ERROR
--Testcase 398:
SELECT ftx.x1, ft2.c2, ftx.x8 FROM ft1 ftx(x1,x2,x3,x4,x5,x6,x7,x8), ft2
  WHERE ftx.x1 = ft2.c1 AND ftx.x1 = 1; -- ERROR
--Testcase 399:
SELECT ftx.x1, ft2.c2, ftx FROM ft1 ftx(x1,x2,x3,x4,x5,x6,x7,x8), ft2
  WHERE ftx.x1 = ft2.c1 AND ftx.x1 = 1; -- ERROR
--Testcase 400:
SELECT sum(c2), array_agg(c8) FROM ft1 GROUP BY c8; -- ERROR
--Testcase 401:
ALTER FOREIGN TABLE ft1 ALTER COLUMN c8 TYPE text;

-- ===================================================================
-- subtransaction
--  + local/remote error doesn't break cursor
-- ===================================================================
BEGIN;
DECLARE c CURSOR FOR SELECT * FROM ft1 ORDER BY c1;
--Testcase 402:
FETCH c;
SAVEPOINT s;
ERROR OUT;          -- ERROR
ROLLBACK TO s;
--Testcase 403:
FETCH c;
SAVEPOINT s;
--Testcase 404:
SELECT * FROM ft1 WHERE 1 / (c1 - 1) > 0;  -- ERROR
ROLLBACK TO s;
--Testcase 405:
FETCH c;
--Testcase 406:
SELECT * FROM ft1 ORDER BY c1 LIMIT 1;
COMMIT;

-- ===================================================================
-- test handling of collations
-- ===================================================================
-- create table loct3 (f1 text collate "C" unique, f2 text, f3 varchar(10) unique);
--Testcase 407:
SELECT oracle_execute(
          'oracle_srv',
          E'CREATE TABLE test.loct3 (\n'
          '   id  NUMBER PRIMARY KEY,\n'
          '   f1  CLOB,\n'
          '   f2  CLOB, \n'
          '   f3  VARCHAR(10) \n'
          ') SEGMENT CREATION IMMEDIATE'
       );

--Testcase 408:
create foreign table ft3 (id int options (key 'yes'), f1 text collate "C", f2 text, f3 varchar(10))
  server oracle_srv options (table 'LOCT3');

-- can be sent to remote
--Testcase 409:
explain (costs off) select * from ft3 where f1 = 'foo';
--Testcase 410:
explain (costs off) select * from ft3 where f1 COLLATE "C" = 'foo';
--Testcase 411:
explain (costs off) select * from ft3 where f2 = 'foo';
--Testcase 412:
explain (costs off) select * from ft3 where f3 = 'foo';
--Testcase 413:
explain (costs off) select * from ft3 f, ft3 l
  where f.f3 = l.f3 and l.f1 = 'foo';
-- can't be sent to remote
--Testcase 414:
explain (costs off) select * from ft3 where f1 COLLATE "POSIX" = 'foo';
--Testcase 415:
explain (costs off) select * from ft3 where f1 = 'foo' COLLATE "C";
--Testcase 416:
explain (costs off) select * from ft3 where f2 COLLATE "C" = 'foo';
--Testcase 417:
explain (costs off) select * from ft3 where f2 = 'foo' COLLATE "C";
--Testcase 418:
explain (costs off) select * from ft3 f, ft3 l
  where f.f3 = l.f3 COLLATE "POSIX" and l.f1 = 'foo';

--Testcase 419:
SELECT oracle_execute('oracle_srv', 'DROP TABLE test.loct3 PURGE');
-- ===================================================================
-- test writable foreign table stuff
-- ===================================================================
-- oracle return result in random order, add ORDER BY to stable result
--Testcase 420:
EXPLAIN (costs off)
INSERT INTO ft2 (c1,c2,c3) SELECT c1+1000,c2+100, c3 || c3 FROM ft2 ORDER BY c1 LIMIT 20;
--Testcase 421:
INSERT INTO ft2 (c1,c2,c3) SELECT c1+1000,c2+100, c3 || c3 FROM ft2 ORDER BY c1 LIMIT 20;
--Testcase 422:
INSERT INTO ft2 (c1,c2,c3)
  VALUES (1101,201,'aaa'), (1102,202,'bbb'), (1103,203,'ccc') RETURNING *;
--Testcase 423:
INSERT INTO ft2 (c1,c2,c3) VALUES (1104,204,'ddd'), (1105,205,'eee');
--Testcase 424:
EXPLAIN (costs off)
UPDATE ft2 SET c2 = c2 + 300, c3 = c3 || '_update3' WHERE c1 % 10 = 3;              -- can be pushed down
--Testcase 425:
UPDATE ft2 SET c2 = c2 + 300, c3 = c3 || '_update3' WHERE c1 % 10 = 3;
--Testcase 426:
EXPLAIN (costs off)
UPDATE ft2 SET c2 = c2 + 400, c3 = c3 || '_update7' WHERE c1 % 10 = 7 RETURNING *;  -- can be pushed down
--Testcase 427:
UPDATE ft2 SET c2 = c2 + 400, c3 = c3 || '_update7' WHERE c1 % 10 = 7;-- RETURNING *;
-- RETURNING * does not return result in order, using SELECT with ORDER BY instead
-- for maintainability with the postgres_fdw's test.
--Testcase 428:
SELECT * FROM ft2 WHERE c1 % 10 = 7 ORDER BY c1;


--Testcase 429:
EXPLAIN (costs off)
UPDATE ft2 SET c2 = ft2.c2 + 500, c3 = ft2.c3 || '_update9', c7 = DEFAULT
  FROM ft1 WHERE ft1.c1 = ft2.c2 AND ft1.c1 % 10 = 9;                               -- can be pushed down
--Testcase 430:
UPDATE ft2 SET c2 = ft2.c2 + 500, c3 = ft2.c3 || '_update9', c7 = DEFAULT
  FROM ft1 WHERE ft1.c1 = ft2.c2 AND ft1.c1 % 10 = 9;
--Testcase 431:
EXPLAIN (verbose, costs off)
  DELETE FROM ft2 WHERE c1 % 10 = 5 RETURNING c1, c4;                               -- can be pushed down
-- RETURNING * does not return result in order, using SELECT with ORDER BY instead
-- for maintainability with the postgres_fdw's test.
--Testcase 432:
SELECT c1, c4 FROM ft2 WHERE c1% 10 = 5 ORDER BY c1;
--Testcase 433:
DELETE FROM ft2 WHERE c1 % 10 = 5;-- RETURNING c1, c4;
--Testcase 434:
EXPLAIN (costs off)
DELETE FROM ft2 USING ft1 WHERE ft1.c1 = ft2.c2 AND ft1.c1 % 10 = 2;                -- can be pushed down
--Testcase 435:
DELETE FROM ft2 USING ft1 WHERE ft1.c1 = ft2.c2 AND ft1.c1 % 10 = 2;
--Testcase 436:
SELECT c1,c2,c3,c4 FROM ft2 ORDER BY c1;
--Testcase 437:
EXPLAIN (costs off)
INSERT INTO ft2 (c1,c2,c3) VALUES (1200,999,'foo') RETURNING tableoid::regclass;
--Testcase 438:
INSERT INTO ft2 (c1,c2,c3) VALUES (1200,999,'foo') RETURNING tableoid::regclass;
--Testcase 439:
EXPLAIN (verbose, costs off)
UPDATE ft2 SET c3 = 'bar' WHERE c1 = 1200 RETURNING tableoid::regclass;             -- can be pushed down
--Testcase 440:
UPDATE ft2 SET c3 = 'bar' WHERE c1 = 1200 RETURNING tableoid::regclass;
--Testcase 441:
EXPLAIN (verbose, costs off)
DELETE FROM ft2 WHERE c1 = 1200 RETURNING tableoid::regclass;                       -- can be pushed down
--Testcase 442:
DELETE FROM ft2 WHERE c1 = 1200 RETURNING tableoid::regclass;

-- Test UPDATE/DELETE with RETURNING on a three-table join
--Testcase 443:
INSERT INTO ft2 (c1,c2,c3)
  SELECT id, id - 1200, to_char(id, 'FM00000') FROM generate_series(1201, 1300) id;
--Testcase 444:
EXPLAIN (costs off)
UPDATE ft2 SET c3 = 'foo'
  FROM ft4 INNER JOIN ft5 ON (ft4.c1 = ft5.c1)
  WHERE ft2.c1 > 1200 AND ft2.c2 = ft4.c1
  RETURNING ft2, ft2.*, ft4, ft4.*;       -- can be pushed down
--Testcase 445:
UPDATE ft2 SET c3 = 'foo'
  FROM ft4 INNER JOIN ft5 ON (ft4.c1 = ft5.c1)
  WHERE ft2.c1 > 1200 AND ft2.c2 = ft4.c1
  RETURNING ft2, ft2.*, ft4, ft4.*;
--Testcase 446:
EXPLAIN (costs off)
DELETE FROM ft2
  USING ft4 LEFT JOIN ft5 ON (ft4.c1 = ft5.c1)
  WHERE ft2.c1 > 1200 AND ft2.c1 % 10 = 0 AND ft2.c2 = ft4.c1
  RETURNING 100;                          -- can be pushed down
--Testcase 447:
DELETE FROM ft2
  USING ft4 LEFT JOIN ft5 ON (ft4.c1 = ft5.c1)
  WHERE ft2.c1 > 1200 AND ft2.c1 % 10 = 0 AND ft2.c2 = ft4.c1
  RETURNING 100;
--Testcase 448:
DELETE FROM ft2 WHERE ft2.c1 > 1200;

-- Test UPDATE with a MULTIEXPR sub-select
-- (maybe someday this'll be remotely executable, but not today)
--Testcase 449:
EXPLAIN (costs off)
UPDATE ft2 AS target SET (c2, c7) = (
    SELECT c2 * 10, c7
        FROM ft2 AS src
        WHERE target.c1 = src.c1
) WHERE c1 > 1100;
--Testcase 450:
UPDATE ft2 AS target SET (c2, c7) = (
    SELECT c2 * 10, c7
        FROM ft2 AS src
        WHERE target.c1 = src.c1
) WHERE c1 > 1100;

--Testcase 451:
UPDATE ft2 AS target SET (c2) = (
    SELECT c2 / 10
        FROM ft2 AS src
        WHERE target.c1 = src.c1
) WHERE c1 > 1100;

-- Test UPDATE involving a join that can be pushed down,
-- but a SET clause that can't be
--Testcase 452:
EXPLAIN (COSTS OFF)
UPDATE ft2 d SET c2 = CASE WHEN random() >= 0 THEN d.c2 ELSE 0 END
  FROM ft2 AS t WHERE d.c1 = t.c1 AND d.c1 > 1000;
--Testcase 453:
UPDATE ft2 d SET c2 = CASE WHEN random() >= 0 THEN d.c2 ELSE 0 END
  FROM ft2 AS t WHERE d.c1 = t.c1 AND d.c1 > 1000;

-- Test UPDATE/DELETE with WHERE or JOIN/ON conditions containing
-- user-defined operators/functions
--ALTER SERVER oracle_srv OPTIONS (DROP extensions);
--Testcase 454:
INSERT INTO ft2 (c1,c2,c3)
  SELECT id, id % 10, to_char(id, 'FM00000') FROM generate_series(2001, 2010) id;
--Testcase 455:
EXPLAIN (costs off)
UPDATE ft2 SET c3 = 'bar' WHERE oracle_fdw_abs(c1) > 2000 RETURNING *;            -- can't be pushed down
--Testcase 456:
UPDATE ft2 SET c3 = 'bar' WHERE oracle_fdw_abs(c1) > 2000;-- RETURNING *;
--Testcase 457:
SELECT * FROM ft2 WHERE oracle_fdw_abs(c1) > 2000 ORDER BY c1;
--Testcase 458:
EXPLAIN (costs off)
UPDATE ft2 SET c3 = 'baz'
  FROM ft4 INNER JOIN ft5 ON (ft4.c1 = ft5.c1)
  WHERE ft2.c1 > 2000 AND ft2.c2 === ft4.c1
  RETURNING ft2.*, ft4.*, ft5.*;                                                    -- can't be pushed down
--Testcase 459:
UPDATE ft2 SET c3 = 'baz'
  FROM ft4 INNER JOIN ft5 ON (ft4.c1 = ft5.c1)
  WHERE ft2.c1 > 2000 AND ft2.c2 === ft4.c1
  RETURNING ft2.*, ft4.*, ft5.*;
--Testcase 460:
EXPLAIN (costs off)
DELETE FROM ft2
  USING ft4 INNER JOIN ft5 ON (ft4.c1 === ft5.c1)
  WHERE ft2.c1 > 2000 AND ft2.c2 = ft4.c1
  RETURNING ft2.c1, ft2.c2, ft2.c3;       -- can't be pushed down
--Testcase 461:
DELETE FROM ft2
  USING ft4 INNER JOIN ft5 ON (ft4.c1 === ft5.c1)
  WHERE ft2.c1 > 2000 AND ft2.c2 = ft4.c1
  RETURNING ft2.c1, ft2.c2, ft2.c3;
--Testcase 462:
DELETE FROM ft2 WHERE ft2.c1 > 2000;
--ALTER SERVER oracle_srv OPTIONS (ADD extensions 'oracle_fdw');

-- Test that trigger on remote table works as expected
--Testcase 463:
CREATE OR REPLACE FUNCTION "S 1".F_BRTRIG() RETURNS trigger AS $$
BEGIN
    NEW.c3 = NEW.c3 || '_trig_update';
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;
--Testcase 464:
CREATE TRIGGER t1_br_insert BEFORE INSERT OR UPDATE
    ON ft2 FOR EACH ROW EXECUTE PROCEDURE "S 1".F_BRTRIG();

--Testcase 465:
INSERT INTO ft2 (c1,c2,c3) VALUES (1208, 818, 'fff') RETURNING *;
--Testcase 466:
INSERT INTO ft2 (c1,c2,c3,c6) VALUES (1218, 818, 'ggg', '(--;') RETURNING *;
--Testcase 467:
UPDATE ft2 SET c2 = c2 + 600 WHERE c1 % 10 = 8 AND c1 < 1200;-- RETURNING *;
-- RETURNING * does not return result in order, using SELECT with ORDER BY instead
-- for maintainability with the postgres_fdw's test.
--Testcase 468:
SELECT * FROM ft2 WHERE c1 % 10 = 8 AND c1 < 1200 ORDER BY c1;
-- Test errors thrown on remote side during update
--Testcase 469:
SELECT oracle_execute(
          'oracle_srv',
          E'ALTER TABLE test."T 1" \n'
          '   ADD CONSTRAINT c2positive CHECK (c2 >= 0)'
        );
-- ALTER TABLE ft1 ADD CONSTRAINT c2positive CHECK (c2 >= 0);
-- INSERT INTO ft1(c1, c2) VALUES(11, 12);  -- duplicate key

-- Oracle returns an error message with a random system id each time executing test.
-- To make test result more stable, we customize the return message of the ported test.
DO LANGUAGE plpgsql $$
DECLARE
    msg     TEXT;
    detail  TEXT;
BEGIN
--Testcase 470:
    INSERT INTO ft1(c1, c2) VALUES(11, 12);  -- duplicate key

    EXCEPTION WHEN OTHERS THEN
        GET stacked diagnostics
              msg     = message_text,
              detail  = pg_exception_detail;

        IF left(detail, 9) = 'ORA-00001' THEN
          detail := 'ORA-00001: unique constraint violated';
        END IF;

        RAISE EXCEPTION E'
          %
          %', msg, detail;
END; $$;

-- oracle fdw does not support ON CONFLICT
-- INSERT INTO ft1(c1, c2) VALUES(11, 12) ON CONFLICT DO NOTHING; -- works
--Testcase 471:
INSERT INTO ft1(c1, c2) VALUES(11, 12) ON CONFLICT (c1, c2) DO NOTHING; -- unsupported
--Testcase 472:
INSERT INTO ft1(c1, c2) VALUES(11, 12) ON CONFLICT (c1, c2) DO UPDATE SET c3 = 'ffg'; -- unsupported
--Testcase 473:
INSERT INTO ft1(c1, c2) VALUES(1111, -2);  -- c2positive
--Testcase 474:
UPDATE ft1 SET c2 = -c2 WHERE c1 = 1;  -- c2positive

-- Test savepoint/rollback behavior
--Testcase 475:
select c2, count(*) from ft2 where c2 < 500 group by 1 order by 1;
--Testcase 476:
select c2, count(*) from "S 1"."t 1" where c2 < 500 group by 1 order by 1;
begin;
--Testcase 477:
update ft2 set c2 = 42 where c2 = 0;
--Testcase 478:
select c2, count(*) from ft2 where c2 < 500 group by 1 order by 1;
savepoint s1;
--Testcase 479:
update ft2 set c2 = 44 where c2 = 4;
--Testcase 480:
select c2, count(*) from ft2 where c2 < 500 group by 1 order by 1;
release savepoint s1;
--Testcase 481:
select c2, count(*) from ft2 where c2 < 500 group by 1 order by 1;
savepoint s2;
--Testcase 482:
update ft2 set c2 = 46 where c2 = 6;
--Testcase 483:
select c2, count(*) from ft2 where c2 < 500 group by 1 order by 1;
rollback to savepoint s2;
--Testcase 484:
select c2, count(*) from ft2 where c2 < 500 group by 1 order by 1;
release savepoint s2;
--Testcase 485:
select c2, count(*) from ft2 where c2 < 500 group by 1 order by 1;
savepoint s3;
--Testcase 486:
update ft2 set c2 = -2 where c2 = 42 and c1 = 10; -- fail on remote side
rollback to savepoint s3;
--Testcase 487:
select c2, count(*) from ft2 where c2 < 500 group by 1 order by 1;
release savepoint s3;
--Testcase 488:
select c2, count(*) from ft2 where c2 < 500 group by 1 order by 1;
-- none of the above is committed yet remotely
-- orcale fdw commit data immediately, we will see the result different with postgres_fdw's test
--Testcase 489:
select c2, count(*) from "S 1"."t 1" where c2 < 500 group by 1 order by 1;
commit;
--Testcase 490:
select c2, count(*) from ft2 where c2 < 500 group by 1 order by 1;
--Testcase 491:
select c2, count(*) from "S 1"."t 1" where c2 < 500 group by 1 order by 1;

VACUUM ANALYZE "S 1"."t 1";

-- Above DMLs add data with c6 as NULL in ft1, so test ORDER BY NULLS LAST and NULLs
-- FIRST behavior here.
-- ORDER BY DESC NULLS LAST options
--Testcase 492:
EXPLAIN (COSTS OFF) SELECT * FROM ft1 ORDER BY c6 DESC NULLS LAST, c1 OFFSET 795 LIMIT 10;
--Testcase 493:
SELECT * FROM ft1 ORDER BY c6 DESC NULLS LAST, c1 OFFSET 795  LIMIT 10;
-- ORDER BY DESC NULLS FIRST options
--Testcase 494:
EXPLAIN (COSTS OFF) SELECT * FROM ft1 ORDER BY c6 DESC NULLS FIRST, c1 OFFSET 15 LIMIT 10;
--Testcase 495:
SELECT * FROM ft1 ORDER BY c6 DESC NULLS FIRST, c1 OFFSET 15 LIMIT 10;
-- ORDER BY ASC NULLS FIRST options
--Testcase 496:
EXPLAIN (COSTS OFF) SELECT * FROM ft1 ORDER BY c6 ASC NULLS FIRST, c1 OFFSET 15 LIMIT 10;
--Testcase 497:
SELECT * FROM ft1 ORDER BY c6 ASC NULLS FIRST, c1 OFFSET 15 LIMIT 10;

-- ===================================================================
-- test check constraints
-- ===================================================================

-- Consistent check constraints provide consistent results
--ALTER FOREIGN TABLE ft1 ADD CONSTRAINT ft1_c2positive CHECK (c2 >= 0);
--Testcase 498:
EXPLAIN (COSTS OFF) SELECT count(*) FROM ft1 WHERE c2 < 0;
--Testcase 499:
SELECT count(*) FROM ft1 WHERE c2 < 0;
--Testcase 500:
SET constraint_exclusion = 'on';
--Testcase 501:
EXPLAIN (COSTS OFF) SELECT count(*) FROM ft1 WHERE c2 < 0;
--Testcase 502:
SELECT count(*) FROM ft1 WHERE c2 < 0;
--Testcase 503:
RESET constraint_exclusion;
-- check constraint is enforced on the remote side, not locally
--Testcase 504:
INSERT INTO ft1(c1, c2) VALUES(1111, -2);  -- c2positive
--Testcase 505:
UPDATE ft1 SET c2 = -c2 WHERE c1 = 1;  -- c2positive
--ALTER FOREIGN TABLE ft1 DROP CONSTRAINT ft1_c2positive;
--Testcase 506:
SELECT oracle_execute(
          'oracle_srv',
          E'ALTER TABLE test."T 1" \n'
          '   DROP CONSTRAINT c2positive'
        );


-- But inconsistent check constraints provide inconsistent results
--Testcase 507:
ALTER FOREIGN TABLE ft1 ADD CONSTRAINT ft1_c2negative CHECK (c2 < 0);
--Testcase 508:
EXPLAIN (COSTS OFF) SELECT count(*) FROM ft1 WHERE c2 >= 0;
--Testcase 509:
SELECT count(*) FROM ft1 WHERE c2 >= 0;
--Testcase 510:
SET constraint_exclusion = 'on';
--Testcase 511:
EXPLAIN (COSTS OFF) SELECT count(*) FROM ft1 WHERE c2 >= 0;
--Testcase 512:
SELECT count(*) FROM ft1 WHERE c2 >= 0;
--Testcase 513:
RESET constraint_exclusion;
-- local check constraint is not actually enforced
--Testcase 514:
INSERT INTO ft1(c1, c2) VALUES(1111, 2);
--Testcase 515:
UPDATE ft1 SET c2 = c2 + 1 WHERE c1 = 1;
--Testcase 516:
ALTER FOREIGN TABLE ft1 DROP CONSTRAINT ft1_c2negative;

-- ===================================================================
-- test WITH CHECK OPTION constraints
-- oracle_fdw does not support WITH CHECK OPTION feature
-- ===================================================================

-- CREATE FUNCTION row_before_insupd_trigfunc() RETURNS trigger AS $$BEGIN NEW.a := NEW.a + 10; RETURN NEW; END$$ LANGUAGE plpgsql;
-- SELECT oracle_execute(
--           'oracle_srv',
--           E'CREATE TABLE test.base_tbl (\n'
--           '   a  NUMBER(5) PRIMARY KEY,\n'
--           '   b  NUMBER(5)'
--           ') SEGMENT CREATION IMMEDIATE'
--        );

-- CREATE FOREIGN TABLE foreign_tbl (a int OPTIONS (key 'yes'), b int)
--   SERVER oracle_srv OPTIONS (table 'BASE_TBL');

-- CREATE TRIGGER row_before_insupd_trigger BEFORE INSERT OR UPDATE ON foreign_tbl FOR EACH ROW EXECUTE PROCEDURE row_before_insupd_trigfunc();
-- CREATE VIEW rw_view AS SELECT * FROM foreign_tbl
--   WHERE a < b WITH CHECK OPTION;
-- \d+ rw_view

-- EXPLAIN (COSTS OFF)
-- INSERT INTO rw_view VALUES (0, 5);
-- INSERT INTO rw_view VALUES (0, 5); -- should fail
-- EXPLAIN (COSTS OFF)
-- INSERT INTO rw_view VALUES (0, 15);
-- INSERT INTO rw_view VALUES (0, 15); -- ok
-- SELECT * FROM foreign_tbl;

-- EXPLAIN (COSTS OFF)
-- UPDATE rw_view SET b = b + 5;
-- UPDATE rw_view SET b = b + 5; -- should fail
-- EXPLAIN (COSTS OFF)
-- UPDATE rw_view SET b = b + 15;
-- UPDATE rw_view SET b = b + 15; -- ok
-- SELECT * FROM foreign_tbl;

-- DROP FOREIGN TABLE foreign_tbl CASCADE;
-- DROP TRIGGER row_before_insupd_trigger ON foreign_tbl;
-- SELECT oracle_execute('oracle_srv', E'DROP TABLE test.base_tbl PURGE');

-- test WCO for partitions

-- CREATE TABLE child_tbl (a int, b int);
-- ALTER TABLE child_tbl SET (autovacuum_enabled = 'false');
-- CREATE TRIGGER row_before_insupd_trigger BEFORE INSERT OR UPDATE ON child_tbl FOR EACH ROW EXECUTE PROCEDURE row_before_insupd_trigfunc();
-- CREATE FOREIGN TABLE foreign_tbl (a int, b int)
--   SERVER oracle_srv OPTIONS (table 'child_tbl');

-- CREATE TABLE parent_tbl (a int, b int) PARTITION BY RANGE(a);
-- ALTER TABLE parent_tbl ATTACH PARTITION foreign_tbl FOR VALUES FROM (0) TO (100);

-- CREATE VIEW rw_view AS SELECT * FROM parent_tbl
--   WHERE a < b WITH CHECK OPTION;
-- \d+ rw_view

-- EXPLAIN (COSTS OFF)
-- INSERT INTO rw_view VALUES (0, 5);
-- INSERT INTO rw_view VALUES (0, 5); -- should fail
-- EXPLAIN (COSTS OFF)
-- INSERT INTO rw_view VALUES (0, 15);
-- INSERT INTO rw_view VALUES (0, 15); -- ok
-- SELECT * FROM foreign_tbl;

-- EXPLAIN (COSTS OFF)
-- UPDATE rw_view SET b = b + 5;
-- UPDATE rw_view SET b = b + 5; -- should fail
-- EXPLAIN (COSTS OFF)
-- UPDATE rw_view SET b = b + 15;
-- UPDATE rw_view SET b = b + 15; -- ok
-- SELECT * FROM foreign_tbl;

-- DROP FOREIGN TABLE foreign_tbl CASCADE;
-- DROP TRIGGER row_before_insupd_trigger ON child_tbl;
-- DROP TABLE parent_tbl CASCADE;

-- DROP FUNCTION row_before_insupd_trigfunc;

-- ===================================================================
-- test serial columns (ie, sequence-based defaults)
-- ===================================================================
--Testcase 517:
SELECT oracle_execute(
          'oracle_srv',
          E'CREATE TABLE test.loc1 (\n'
          '   id  NUMBER(5),\n'
          '   f1  NUMBER(5),\n'
          '   f2  CLOB,\n'
          '   PRIMARY KEY (id, f1)\n'
          ') SEGMENT CREATION IMMEDIATE'
       );

--Testcase 518:
create foreign table rem1 (id serial options (key 'yes'), f1 serial options (key 'yes'), f2 text)
  server oracle_srv options(table 'LOC1');
--Testcase 519:
create foreign table floc1 (id serial options (key 'yes'), f1 serial options (key 'yes'), f2 text)
  server oracle_srv options(table 'LOC1');
--Testcase 520:
select pg_catalog.setval('rem1_f1_seq', 10, false);
--Testcase 521:
insert into floc1(f2) values('hi');
--Testcase 522:
insert into rem1(f2) values('hi remote');
--Testcase 523:
insert into floc1(f2) values('bye');
--Testcase 524:
insert into rem1(f2) values('bye remote');
--Testcase 525:
select f1, f2 from floc1;
--Testcase 526:
select f1, f2 from rem1;

-- ===================================================================
-- test generated columns
-- ===================================================================
--Testcase 527:
SELECT oracle_execute(
          'oracle_srv',
          E'CREATE TABLE test.gloc1 (\n'
          '   a  NUMBER(5) PRIMARY KEY,\n'
          '   b  NUMBER(5)\n'
          ') SEGMENT CREATION IMMEDIATE'
       );
--Testcase 528:
create foreign table grem1 (
  a int OPTIONS (key 'yes'),
  b int generated always as (a * 2) stored)
  server oracle_srv options(table 'GLOC1');

--Testcase 529:
explain (costs off)
insert into grem1 (a) values (1), (2);
--Testcase 530:
insert into grem1 (a) values (1), (2);

--Testcase 531:
explain (costs off)
update grem1 set a = 22 where a = 2;
--Testcase 532:
update grem1 set a = 22 where a = 2;

--Testcase 533:
select a, b from grem1;
--Testcase 534:
delete from grem1;

-- test copy from
copy grem1 from stdin;
1
2
\.
--Testcase 535:
select * from grem1;
--Testcase 536:
delete from grem1;

-- oracle fdw does not support batch insert
-- test batch insert
-- alter server oracle_srv options (add batch_size '10');
-- explain (costs off)
-- insert into grem1 (a) values (1), (2);
-- insert into grem1 (a) values (1), (2);
-- select * from gloc1;
-- select * from grem1;
-- delete from grem1;
-- alter server oracle_srv options (drop batch_size);
--Testcase 537:
SELECT oracle_execute('oracle_srv', 'DROP TABLE test.gloc1 PURGE');

-- -- ===================================================================
-- -- test local triggers
-- -- ===================================================================

-- Trigger functions "borrowed" from triggers regress test.
--Testcase 538:
CREATE FUNCTION trigger_func() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
	RAISE NOTICE 'trigger_func(%) called: action = %, when = %, level = %',
		TG_ARGV[0], TG_OP, TG_WHEN, TG_LEVEL;
	RETURN NULL;
END;$$;

--Testcase 539:
CREATE TRIGGER trig_stmt_before BEFORE DELETE OR INSERT OR UPDATE ON rem1
	FOR EACH STATEMENT EXECUTE PROCEDURE trigger_func();
--Testcase 540:
CREATE TRIGGER trig_stmt_after AFTER DELETE OR INSERT OR UPDATE ON rem1
	FOR EACH STATEMENT EXECUTE PROCEDURE trigger_func();

--Testcase 541:
CREATE OR REPLACE FUNCTION trigger_data()  RETURNS trigger
LANGUAGE plpgsql AS $$

declare
	oldnew text[];
	relid text;
    argstr text;
begin

	relid := TG_relid::regclass;
	argstr := '';
	for i in 0 .. TG_nargs - 1 loop
		if i > 0 then
			argstr := argstr || ', ';
		end if;
		argstr := argstr || TG_argv[i];
	end loop;

    RAISE NOTICE '%(%) % % % ON %',
		tg_name, argstr, TG_when, TG_level, TG_OP, relid;
    oldnew := '{}'::text[];
	if TG_OP != 'INSERT' then
		oldnew := array_append(oldnew, format('OLD: %s', OLD));
	end if;

	if TG_OP != 'DELETE' then
		oldnew := array_append(oldnew, format('NEW: %s', NEW));
	end if;

    RAISE NOTICE '%', array_to_string(oldnew, ',');

	if TG_OP = 'DELETE' then
		return OLD;
	else
		return NEW;
	end if;
end;
$$;

-- Test basic functionality
--Testcase 542:
CREATE TRIGGER trig_row_before
BEFORE INSERT OR UPDATE OR DELETE ON rem1
FOR EACH ROW EXECUTE PROCEDURE trigger_data(23,'skidoo');

--Testcase 543:
CREATE TRIGGER trig_row_after
AFTER INSERT OR UPDATE OR DELETE ON rem1
FOR EACH ROW EXECUTE PROCEDURE trigger_data(23,'skidoo');

--Testcase 544:
delete from rem1;
--Testcase 545:
insert into rem1(f1, f2) values(1,'insert');
--Testcase 546:
update rem1 set f2  = 'update' where f1 = 1;
--Testcase 547:
update rem1 set f2 = f2 || f2;


-- cleanup
--Testcase 548:
DROP TRIGGER trig_row_before ON rem1;
--Testcase 549:
DROP TRIGGER trig_row_after ON rem1;
--Testcase 550:
DROP TRIGGER trig_stmt_before ON rem1;
--Testcase 551:
DROP TRIGGER trig_stmt_after ON rem1;

--Testcase 552:
DELETE from rem1;

-- Test multiple AFTER ROW triggers on a foreign table
--Testcase 553:
CREATE TRIGGER trig_row_after1
AFTER INSERT OR UPDATE OR DELETE ON rem1
FOR EACH ROW EXECUTE PROCEDURE trigger_data(23,'skidoo');

--Testcase 554:
CREATE TRIGGER trig_row_after2
AFTER INSERT OR UPDATE OR DELETE ON rem1
FOR EACH ROW EXECUTE PROCEDURE trigger_data(23,'skidoo');

--Testcase 555:
insert into rem1(f1, f2) values(1,'insert');
--Testcase 556:
update rem1 set f2  = 'update' where f1 = 1;
--Testcase 557:
update rem1 set f2 = f2 || f2;
--Testcase 558:
delete from rem1;

-- cleanup
--Testcase 559:
DROP TRIGGER trig_row_after1 ON rem1;
--Testcase 560:
DROP TRIGGER trig_row_after2 ON rem1;

-- Test WHEN conditions

--Testcase 561:
CREATE TRIGGER trig_row_before_insupd
BEFORE INSERT OR UPDATE ON rem1
FOR EACH ROW
WHEN (NEW.f2 like '%update%')
EXECUTE PROCEDURE trigger_data(23,'skidoo');

--Testcase 562:
CREATE TRIGGER trig_row_after_insupd
AFTER INSERT OR UPDATE ON rem1
FOR EACH ROW
WHEN (NEW.f2 like '%update%')
EXECUTE PROCEDURE trigger_data(23,'skidoo');

-- Insert or update not matching: nothing happens
--Testcase 563:
INSERT INTO rem1(f1, f2) values(1, 'insert');
--Testcase 564:
UPDATE rem1 set f2 = 'test';

-- Insert or update matching: triggers are fired
--Testcase 565:
INSERT INTO rem1(f1, f2) values(2, 'update');
--Testcase 566:
UPDATE rem1 set f2 = 'update update' where f1 = '2';

--Testcase 567:
CREATE TRIGGER trig_row_before_delete
BEFORE DELETE ON rem1
FOR EACH ROW
WHEN (OLD.f2 like '%update%')
EXECUTE PROCEDURE trigger_data(23,'skidoo');

--Testcase 568:
CREATE TRIGGER trig_row_after_delete
AFTER DELETE ON rem1
FOR EACH ROW
WHEN (OLD.f2 like '%update%')
EXECUTE PROCEDURE trigger_data(23,'skidoo');

-- Trigger is fired for f1=2, not for f1=1
--Testcase 569:
DELETE FROM rem1;

-- cleanup
--Testcase 570:
DROP TRIGGER trig_row_before_insupd ON rem1;
--Testcase 571:
DROP TRIGGER trig_row_after_insupd ON rem1;
--Testcase 572:
DROP TRIGGER trig_row_before_delete ON rem1;
--Testcase 573:
DROP TRIGGER trig_row_after_delete ON rem1;


-- Test various RETURN statements in BEFORE triggers.

--Testcase 574:
CREATE FUNCTION trig_row_before_insupdate() RETURNS TRIGGER AS $$
  BEGIN
    NEW.f2 := NEW.f2 || ' triggered !';
    RETURN NEW;
  END
$$ language plpgsql;

--Testcase 575:
CREATE TRIGGER trig_row_before_insupd
BEFORE INSERT OR UPDATE ON rem1
FOR EACH ROW EXECUTE PROCEDURE trig_row_before_insupdate();

-- The new values should have 'triggered' appended
--Testcase 576:
INSERT INTO rem1(f1, f2) values(1, 'insert');
--Testcase 577:
SELECT f1, f2 from rem1;
--Testcase 578:
INSERT INTO rem1(f1, f2) values(2, 'insert') RETURNING f2;
--Testcase 579:
SELECT f1, f2 from rem1;
--Testcase 580:
UPDATE rem1 set f2 = '';
--Testcase 581:
SELECT f1, f2 from rem1;
--Testcase 582:
UPDATE rem1 set f2 = 'skidoo' RETURNING f2;
--Testcase 583:
SELECT f1, f2 from rem1;

--Testcase 584:
EXPLAIN (costs off)
UPDATE rem1 set f1 = 10;          -- all columns should be transmitted
--Testcase 585:
UPDATE rem1 set f1 = 10;
--Testcase 586:
SELECT f1, f2 from rem1;

--Testcase 587:
DELETE FROM rem1;

-- Add a second trigger, to check that the changes are propagated correctly
-- from trigger to trigger
--Testcase 588:
CREATE TRIGGER trig_row_before_insupd2
BEFORE INSERT OR UPDATE ON rem1
FOR EACH ROW EXECUTE PROCEDURE trig_row_before_insupdate();

--Testcase 589:
INSERT INTO rem1(f1, f2) values(1, 'insert');
--Testcase 590:
SELECT f1, f2 from floc1;
--Testcase 591:
INSERT INTO rem1(f1, f2) values(2, 'insert') RETURNING f2;
--Testcase 592:
SELECT f1, f2 from floc1;
--Testcase 593:
UPDATE rem1 set f2 = '';
--Testcase 594:
SELECT f1, f2 from floc1;
--Testcase 595:
UPDATE rem1 set f2 = 'skidoo' RETURNING f2;
--Testcase 596:
SELECT f1, f2 from floc1;

--Testcase 597:
DROP TRIGGER trig_row_before_insupd ON rem1;
--Testcase 598:
DROP TRIGGER trig_row_before_insupd2 ON rem1;

--Testcase 599:
DELETE from rem1;

--Testcase 600:
INSERT INTO rem1(f1, f2) VALUES (1, 'test');

-- Test with a trigger returning NULL
--Testcase 601:
CREATE FUNCTION trig_null() RETURNS TRIGGER AS $$
  BEGIN
    RETURN NULL;
  END
$$ language plpgsql;

--Testcase 602:
CREATE TRIGGER trig_null
BEFORE INSERT OR UPDATE OR DELETE ON rem1
FOR EACH ROW EXECUTE PROCEDURE trig_null();

-- Nothing should have changed.
--Testcase 603:
INSERT INTO rem1(f1, f2) VALUES (2, 'test2');

--Testcase 604:
SELECT f1, f2 from floc1;

--Testcase 605:
UPDATE rem1 SET f2 = 'test2';

--Testcase 606:
SELECT f1, f2 from floc1;

--Testcase 607:
DELETE from rem1;

--Testcase 608:
SELECT f1, f2 from floc1;

--Testcase 609:
DROP TRIGGER trig_null ON rem1;
--Testcase 610:
DELETE from rem1;

-- Test a combination of local and remote triggers
--Testcase 611:
CREATE TRIGGER trig_row_before
BEFORE INSERT OR UPDATE OR DELETE ON rem1
FOR EACH ROW EXECUTE PROCEDURE trigger_data(23,'skidoo');

--Testcase 612:
CREATE TRIGGER trig_row_after
AFTER INSERT OR UPDATE OR DELETE ON rem1
FOR EACH ROW EXECUTE PROCEDURE trigger_data(23,'skidoo');

--Testcase 613:
CREATE TRIGGER trig_local_before BEFORE INSERT OR UPDATE ON rem1
FOR EACH ROW EXECUTE PROCEDURE trig_row_before_insupdate();

--Testcase 614:
INSERT INTO rem1(f2) VALUES ('test');
--Testcase 615:
UPDATE rem1 SET f2 = 'testo';

-- Test returning a system attribute
--Testcase 616:
INSERT INTO rem1(f2) VALUES ('test') RETURNING ctid;

-- cleanup
--Testcase 617:
DROP TRIGGER trig_row_before ON rem1;
--Testcase 618:
DROP TRIGGER trig_row_after ON rem1;
--Testcase 619:
DROP TRIGGER trig_local_before ON rem1;


-- Test direct foreign table modification functionality
--Testcase 620:
EXPLAIN (verbose, costs off)
DELETE FROM rem1;                 -- can be pushed down
--Testcase 621:
EXPLAIN (verbose, costs off)
DELETE FROM rem1 WHERE false;     -- currently can't be pushed down

-- Test with statement-level triggers
-- oracle does not support updating NULL to CLOB column
--Testcase 622:
CREATE TRIGGER trig_stmt_before
	BEFORE DELETE OR INSERT OR UPDATE ON rem1
	FOR EACH STATEMENT EXECUTE PROCEDURE trigger_func();
--Testcase 623:
EXPLAIN (verbose, costs off)
UPDATE rem1 set f2 = '';          -- can't be pushed down
--Testcase 624:
EXPLAIN (verbose, costs off)
DELETE FROM rem1;                 -- can be pushed down
--Testcase 625:
DROP TRIGGER trig_stmt_before ON rem1;

--Testcase 626:
CREATE TRIGGER trig_stmt_after
	AFTER DELETE OR INSERT OR UPDATE ON rem1
	FOR EACH STATEMENT EXECUTE PROCEDURE trigger_func();
--Testcase 627:
EXPLAIN (verbose, costs off)
UPDATE rem1 set f2 = '';          -- can't be pushed down
--Testcase 628:
EXPLAIN (verbose, costs off)
DELETE FROM rem1;                 -- can be pushed down
--Testcase 629:
DROP TRIGGER trig_stmt_after ON rem1;

-- Test with row-level ON INSERT triggers
--Testcase 630:
CREATE TRIGGER trig_row_before_insert
BEFORE INSERT ON rem1
FOR EACH ROW EXECUTE PROCEDURE trigger_data(23,'skidoo');
--Testcase 631:
EXPLAIN (verbose, costs off)
UPDATE rem1 set f2 = '';          -- can't be pushed down
--Testcase 632:
EXPLAIN (verbose, costs off)
DELETE FROM rem1;                 -- can be pushed down
--Testcase 633:
DROP TRIGGER trig_row_before_insert ON rem1;

--Testcase 634:
CREATE TRIGGER trig_row_after_insert
AFTER INSERT ON rem1
FOR EACH ROW EXECUTE PROCEDURE trigger_data(23,'skidoo');
--Testcase 635:
EXPLAIN (verbose, costs off)
UPDATE rem1 set f2 = '';          -- can't be pushed down
--Testcase 636:
EXPLAIN (verbose, costs off)
DELETE FROM rem1;                 -- can be pushed down
--Testcase 637:
DROP TRIGGER trig_row_after_insert ON rem1;

-- Test with row-level ON UPDATE triggers
--Testcase 638:
CREATE TRIGGER trig_row_before_update
BEFORE UPDATE ON rem1
FOR EACH ROW EXECUTE PROCEDURE trigger_data(23,'skidoo');
--Testcase 639:
EXPLAIN (verbose, costs off)
UPDATE rem1 set f2 = '';          -- can't be pushed down
--Testcase 640:
EXPLAIN (verbose, costs off)
DELETE FROM rem1;                 -- can be pushed down
--Testcase 641:
DROP TRIGGER trig_row_before_update ON rem1;

--Testcase 642:
CREATE TRIGGER trig_row_after_update
AFTER UPDATE ON rem1
FOR EACH ROW EXECUTE PROCEDURE trigger_data(23,'skidoo');
--Testcase 643:
EXPLAIN (verbose, costs off)
UPDATE rem1 set f2 = '';          -- can't be pushed down
--Testcase 644:
EXPLAIN (verbose, costs off)
DELETE FROM rem1;                 -- can be pushed down
--Testcase 645:
DROP TRIGGER trig_row_after_update ON rem1;

-- Test with row-level ON DELETE triggers
--Testcase 646:
CREATE TRIGGER trig_row_before_delete
BEFORE DELETE ON rem1
FOR EACH ROW EXECUTE PROCEDURE trigger_data(23,'skidoo');
--Testcase 647:
EXPLAIN (verbose, costs off)
UPDATE rem1 set f2 = '';          -- can't be pushed down
--Testcase 648:
EXPLAIN (verbose, costs off)
DELETE FROM rem1;                 -- can't be pushed down
--Testcase 649:
DROP TRIGGER trig_row_before_delete ON rem1;

--Testcase 650:
CREATE TRIGGER trig_row_after_delete
AFTER DELETE ON rem1
FOR EACH ROW EXECUTE PROCEDURE trigger_data(23,'skidoo');
--Testcase 651:
EXPLAIN (verbose, costs off)
UPDATE rem1 set f2 = '';          -- can't be pushed down
--Testcase 652:
EXPLAIN (verbose, costs off)
DELETE FROM rem1;                 -- can't be pushed down
--Testcase 653:
DROP TRIGGER trig_row_after_delete ON rem1;

--Testcase 654:
SELECT oracle_execute('oracle_srv', 'DROP TABLE test.loc1 PURGE');
-- ===================================================================
-- test inheritance features
-- ===================================================================

--Testcase 655:
CREATE TABLE a (aa TEXT);
-- CREATE TABLE loct (aa TEXT, bb TEXT);
--Testcase 656:
SELECT oracle_execute(
          'oracle_srv',
          E'CREATE TABLE test.loct (\n'
          '   aa    CLOB,\n'
          '   id    NUMBER(5) PRIMARY KEY,\n'
          '   bb    CLOB\n'
          ') SEGMENT CREATION IMMEDIATE'
       );

-- ALTER TABLE a SET (autovacuum_enabled = 'false');
-- ALTER TABLE loct SET (autovacuum_enabled = 'false');
--Testcase 657:
CREATE FOREIGN TABLE b (id serial OPTIONS (key 'yes'), bb TEXT) INHERITS (a)
  SERVER oracle_srv OPTIONS (table 'LOCT');

--Testcase 658:
INSERT INTO a(aa) VALUES('aaa');
--Testcase 659:
INSERT INTO a(aa) VALUES('aaaa');
--Testcase 660:
INSERT INTO a(aa) VALUES('aaaaa');

--Testcase 661:
INSERT INTO b(aa) VALUES('bbb');
--Testcase 662:
INSERT INTO b(aa) VALUES('bbbb');
--Testcase 663:
INSERT INTO b(aa) VALUES('bbbbb');

--Testcase 664:
SELECT tableoid::regclass, aa FROM a;
--Testcase 665:
SELECT tableoid::regclass, aa, bb FROM b;
--Testcase 666:
SELECT tableoid::regclass, aa FROM ONLY a;

--Testcase 667:
UPDATE a SET aa = 'zzzzzz' WHERE aa LIKE 'aaaa%';

--Testcase 668:
SELECT tableoid::regclass, aa FROM a;
--Testcase 669:
SELECT tableoid::regclass, aa, bb FROM b;
--Testcase 670:
SELECT tableoid::regclass, aa FROM ONLY a;

--Testcase 671:
UPDATE b SET aa = 'new';

--Testcase 672:
SELECT tableoid::regclass, aa FROM a;
--Testcase 673:
SELECT tableoid::regclass, aa, bb FROM b;
--Testcase 674:
SELECT tableoid::regclass, aa FROM ONLY a;

--Testcase 675:
UPDATE a SET aa = 'newtoo';

--Testcase 676:
SELECT tableoid::regclass, aa FROM a;
--Testcase 677:
SELECT tableoid::regclass, aa, bb FROM b;
--Testcase 678:
SELECT tableoid::regclass, aa FROM ONLY a;

--Testcase 679:
DELETE FROM a;

--Testcase 680:
SELECT tableoid::regclass, aa FROM a;
--Testcase 681:
SELECT tableoid::regclass, aa, bb FROM b;
--Testcase 682:
SELECT tableoid::regclass, aa FROM ONLY a;

--Testcase 683:
DROP TABLE a CASCADE;
-- DROP TABLE loct;
--Testcase 684:
SELECT oracle_execute('oracle_srv', 'DROP TABLE test.LOCT PURGE');

-- Check SELECT FOR UPDATE/SHARE with an inherited source table
-- create table loct1 (f1 int, f2 int, f3 int);
-- create table loct2 (f1 int, f2 int, f3 int);
--Testcase 685:
SELECT oracle_execute(
          'oracle_srv',
          E'CREATE TABLE test.LOCT1 (\n'
          '   f1  NUMBER(5) PRIMARY KEY,\n'
          '   f2  NUMBER(5) ,\n'
          '   f3  NUMBER(5)\n'
          ') SEGMENT CREATION IMMEDIATE'
       );

--Testcase 686:
SELECT oracle_execute(
          'oracle_srv',
          E'CREATE TABLE test.LOCT2 (\n'
          '   f1  NUMBER(5) PRIMARY KEY,\n'
          '   f2  NUMBER(5) ,\n'
          '   f3  NUMBER(5)\n'
          ') SEGMENT CREATION IMMEDIATE'
       );


-- alter table loct1 set (autovacuum_enabled = 'false');
-- alter table loct2 set (autovacuum_enabled = 'false');

--Testcase 687:
create table foo (f1 int, f2 int);
--Testcase 688:
create foreign table foo2 (f3 int OPTIONS (key 'yes')) inherits (foo)
  server oracle_srv options (table 'LOCT1');
--Testcase 689:
create table bar (f1 int, f2 int);
--Testcase 690:
create foreign table bar2 (f3 int OPTIONS (key 'yes')) inherits (bar)
  server oracle_srv options (table 'LOCT2');

-- alter table foo set (autovacuum_enabled = 'false');
-- alter table bar set (autovacuum_enabled = 'false');

--Testcase 691:
insert into foo values(1,1);
--Testcase 692:
insert into foo values(3,3);
--Testcase 693:
insert into foo2 values(2,2,2);
--Testcase 694:
insert into foo2 values(4,4,4);
--Testcase 695:
insert into bar values(1,11);
--Testcase 696:
insert into bar values(2,22);
--Testcase 697:
insert into bar values(6,66);
--Testcase 698:
insert into bar2 values(3,33,33);
--Testcase 699:
insert into bar2 values(4,44,44);
--Testcase 700:
insert into bar2 values(7,77,77);

--Testcase 701:
explain (costs off)
select * from bar where f1 in (select f1 from foo) for update;
--Testcase 702:
select * from bar where f1 in (select f1 from foo) for update;

--Testcase 703:
explain (costs off)
select * from bar where f1 in (select f1 from foo) for share;
--Testcase 704:
select * from bar where f1 in (select f1 from foo) for share;

-- Now check SELECT FOR UPDATE/SHARE with an inherited source table,
-- where the parent is itself a foreign table
-- create table loct4 (f1 int, f2 int, f3 int);
--Testcase 705:
SELECT oracle_execute(
          'oracle_srv',
          E'CREATE TABLE test.LOCT4 (\n'
          '   f1  NUMBER(5) PRIMARY KEY,\n'
          '   f2  NUMBER(5) ,\n'
          '   f3  NUMBER(5)\n'
          ') SEGMENT CREATION IMMEDIATE'
       );

--Testcase 706:
create foreign table foo2child (f3 int) inherits (foo2)
  server oracle_srv options (table 'LOCT4');

--Testcase 707:
explain (costs off)
select * from bar where f1 in (select f1 from foo2) for share;
--Testcase 708:
select * from bar where f1 in (select f1 from foo2) for share;

--Testcase 709:
drop foreign table foo2child;

-- And with a local child relation of the foreign table parent
--Testcase 710:
create table foo2child (f3 int) inherits (foo2);

--Testcase 711:
explain (costs off)
select * from bar where f1 in (select f1 from foo2) for share;
--Testcase 712:
select * from bar where f1 in (select f1 from foo2) for share;

--Testcase 713:
drop table foo2child;
--Testcase 714:
SELECT oracle_execute('oracle_srv', 'DROP TABLE test.LOCT4 PURGE');

-- Check UPDATE with inherited target and an inherited source table
--Testcase 715:
explain (costs off)
update bar set f2 = f2 + 100 where f1 in (select f1 from foo);
--Testcase 716:
update bar set f2 = f2 + 100 where f1 in (select f1 from foo);

--Testcase 717:
select tableoid::regclass, * from bar order by 1,2;

-- Check UPDATE with inherited target and an appendrel subquery
--Testcase 718:
explain (costs off)
update bar set f2 = f2 + 100
from
  ( select f1 from foo union all select f1+3 from foo ) ss
where bar.f1 = ss.f1;
--Testcase 719:
update bar set f2 = f2 + 100
from
  ( select f1 from foo union all select f1+3 from foo ) ss
where bar.f1 = ss.f1;

--Testcase 720:
select tableoid::regclass, * from bar order by 1,2;

-- Test forcing the remote server to produce sorted data for a merge join,
-- but the foreign table is an inheritance child.
--Testcase 721:
delete from foo2;
truncate table only foo;
\set num_rows_foo 2000
--Testcase 722:
insert into foo2 select generate_series(0, :num_rows_foo, 2), generate_series(0, :num_rows_foo, 2), generate_series(0, :num_rows_foo, 2);
--Testcase 723:
insert into foo select generate_series(1, :num_rows_foo, 2), generate_series(1, :num_rows_foo, 2);
--Testcase 724:
SET enable_hashjoin to false;
--Testcase 725:
SET enable_nestloop to false;
-- alter foreign table foo2 options (use_remote_estimate 'true');
-- create index i_loct1_f1 on loct1(f1);
-- create index i_foo_f1 on foo(f1);
analyze foo;
analyze foo2;
-- inner join; expressions in the clauses appear in the equivalence class list
--Testcase 726:
explain (costs off)
	select foo.f1, foo2.f1 from foo join foo2 on (foo.f1 = foo2.f1) order by foo.f2 offset 10 limit 10;
--Testcase 727:
select foo.f1, foo2.f1 from foo join foo2 on (foo.f1 = foo2.f1) order by foo.f2 offset 10 limit 10;
-- outer join; expressions in the clauses do not appear in equivalence class
-- list but no output change as compared to the previous query
--Testcase 728:
explain (costs off)
	select foo.f1, foo2.f1 from foo left join foo2 on (foo.f1 = foo2.f1) order by foo.f2 offset 10 limit 10;
--Testcase 729:
select foo.f1, foo2.f1 from foo left join foo2 on (foo.f1 = foo2.f1) order by foo.f2 offset 10 limit 10;
--Testcase 730:
RESET enable_hashjoin;
--Testcase 731:
RESET enable_nestloop;

-- Test that WHERE CURRENT OF is not supported
begin;
declare c cursor for select * from bar where f1 = 7;
--Testcase 732:
fetch from c;
--Testcase 733:
update bar set f2 = null where current of c;
rollback;

--Testcase 734:
explain (costs off)
delete from foo where f1 < 5 returning *;
--Testcase 735:
delete from foo where f1 < 5 returning *;
--Testcase 736:
explain (costs off)
update bar set f2 = f2 + 100 returning *;
--Testcase 737:
update bar set f2 = f2 + 100 returning *;

-- Test that UPDATE/DELETE with inherited target works with row-level triggers
--Testcase 738:
CREATE TRIGGER trig_row_before
BEFORE UPDATE OR DELETE ON bar2
FOR EACH ROW EXECUTE PROCEDURE trigger_data(23,'skidoo');

--Testcase 739:
CREATE TRIGGER trig_row_after
AFTER UPDATE OR DELETE ON bar2
FOR EACH ROW EXECUTE PROCEDURE trigger_data(23,'skidoo');

--Testcase 740:
explain (costs off)
update bar set f2 = f2 + 100;
--Testcase 741:
update bar set f2 = f2 + 100;

--Testcase 742:
explain (costs off)
delete from bar where f2 < 400;
--Testcase 743:
delete from bar where f2 < 400;

-- cleanup
--Testcase 744:
drop table foo cascade;
--Testcase 745:
drop table bar cascade;
-- drop table loct1;
-- drop table loct2;
--Testcase 746:
SELECT oracle_execute('oracle_srv', 'DROP TABLE test.LOCT1 PURGE');
--Testcase 747:
SELECT oracle_execute('oracle_srv', 'DROP TABLE test.LOCT2 PURGE');

-- Test pushing down UPDATE/DELETE joins to the remote server
--Testcase 748:
create table parent (a int, b text);
-- create table loct1 (a int, b text);
-- create table loct2 (a int, b text);
--Testcase 749:
SELECT oracle_execute(
          'oracle_srv',
          E'CREATE TABLE test.LOCT1 (\n'
          '   a  NUMBER(5) PRIMARY KEY,\n'
          '   b  CLOB\n'
          ') SEGMENT CREATION IMMEDIATE'
       );

--Testcase 750:
SELECT oracle_execute(
          'oracle_srv',
          E'CREATE TABLE test.LOCT2 (\n'
          '   a  NUMBER(5) PRIMARY KEY,\n'
          '   b  CLOB\n'
          ') SEGMENT CREATION IMMEDIATE'
       );
--Testcase 751:
create foreign table remt1 (a int options (key 'yes'), b text)
  server oracle_srv options (table 'LOCT1');
--Testcase 752:
create foreign table remt2 (a int options (key 'yes'), b text)
  server oracle_srv options (table 'LOCT2');
--Testcase 753:
alter foreign table remt1 inherit parent;

--Testcase 754:
insert into remt1 values (1, 'foo');
--Testcase 755:
insert into remt1 values (2, 'bar');
--Testcase 756:
insert into remt2 values (1, 'foo');
--Testcase 757:
insert into remt2 values (2, 'bar');

analyze remt1;
analyze remt2;

--Testcase 758:
explain (costs off)
update parent set b = parent.b || remt2.b from remt2 where parent.a = remt2.a returning *;
--Testcase 759:
update parent set b = parent.b || remt2.b from remt2 where parent.a = remt2.a returning *;
--Testcase 760:
explain (costs off)
delete from parent using remt2 where parent.a = remt2.a returning parent;
--Testcase 761:
delete from parent using remt2 where parent.a = remt2.a returning parent;

-- cleanup
--Testcase 762:
drop foreign table remt1;
--Testcase 763:
drop foreign table remt2;
-- drop table loct1;
-- drop table loct2;
--Testcase 764:
SELECT oracle_execute('oracle_srv', 'DROP TABLE test.LOCT1 PURGE');
--Testcase 765:
SELECT oracle_execute('oracle_srv', 'DROP TABLE test.LOCT2 PURGE');
--Testcase 766:
drop table parent;

-- ===================================================================
-- test tuple routing for foreign-table partitions
-- ===================================================================

-- Test insert tuple routing
--Testcase 767:
create table itrtest (id serial, a int, b text) partition by list (a);
-- create table loct1 (a int check (a in (1)), b text);
-- create table loct2 (a int check (a in (2)), b text);
--Testcase 768:
SELECT oracle_execute(
          'oracle_srv',
          E'CREATE TABLE test.LOCT1 (\n'
          '   id  NUMBER(5) PRIMARY KEY,\n'
          '   a  NUMBER(5),\n'
          '   b  CLOB\n'
          ') SEGMENT CREATION IMMEDIATE'
       );

--Testcase 769:
SELECT oracle_execute(
          'oracle_srv',
          E'CREATE TABLE test.LOCT2 (\n'
          '   id  NUMBER(5) PRIMARY KEY,\n'
          '   b  CLOB,\n'
          '   a  NUMBER(5)\n'
          ') SEGMENT CREATION IMMEDIATE'
       );
--Testcase 770:
create foreign table remp1 (id serial, a int check (a in (1)), b text)
  server oracle_srv options (table 'LOCT1');
--Testcase 771:
create foreign table remp2 (id serial, b text, a int check (a in (2)))
  server oracle_srv options (table 'LOCT2');

--Testcase 772:
alter foreign table remp1 alter column id options (key 'yes');
--Testcase 773:
alter foreign table remp2 alter column id options (key 'yes');

--Testcase 774:
alter table itrtest attach partition remp1 for values in (1);
--Testcase 775:
alter table itrtest attach partition remp2 for values in (2);

--Testcase 776:
insert into itrtest(a, b) values (1, 'foo');
--Testcase 777:
insert into itrtest(a, b) values (1, 'bar') returning a, b;
--Testcase 778:
insert into itrtest(a, b) values (2, 'baz');
--Testcase 779:
insert into itrtest(a, b) values (2, 'qux') returning a, b;
--Testcase 780:
insert into itrtest(a, b) values (1, 'test1'), (2, 'test2') returning a, b;

--Testcase 781:
select tableoid::regclass, a, b FROM itrtest;
--Testcase 782:
select tableoid::regclass, a, b FROM remp1;
--Testcase 783:
select tableoid::regclass, b, a FROM remp2;

--Testcase 784:
delete from itrtest;

--create unique index loct1_idx on loct1 (a);

-- DO NOTHING without an inference specification is supported
-- oracle fdw does not support ON CONFLICT
--insert into itrtest values (1, 'foo') on conflict do nothing returning *;
--Testcase 785:
insert into itrtest(a, b) values (1, 'foo') returning a, b;
--insert into itrtest values (1, 'foo') on conflict do nothing returning *;

-- But other cases are not supported
--insert into itrtest values (1, 'bar') on conflict (a) do nothing;
--insert into itrtest values (1, 'bar') on conflict (a) do update set b = excluded.b;

--Testcase 786:
select tableoid::regclass, a, b FROM itrtest;

--Testcase 787:
delete from itrtest;

--drop index loct1_idx;

-- Test that remote triggers work with insert tuple routing
--Testcase 788:
create function br_insert_trigfunc() returns trigger as $$
begin
	new.b := new.b || ' triggered !';
	return new;
end
$$ language plpgsql;
--Testcase 789:
create trigger remp1_br_insert_trigger before insert on remp1
	for each row execute procedure br_insert_trigfunc();
--Testcase 790:
create trigger remp2_br_insert_trigger before insert on remp2
	for each row execute procedure br_insert_trigfunc();

-- The new values are concatenated with ' triggered !'
--Testcase 791:
insert into itrtest(a, b) values (1, 'foo') returning a, b;
--Testcase 792:
insert into itrtest(a, b) values (2, 'qux') returning a, b;
--Testcase 793:
insert into itrtest(a, b) values (1, 'test1'), (2, 'test2') returning a, b;

-- oracle fdw does not support this case
--Testcase 794:
with result as (insert into itrtest(a, b) values (1, 'test1'), (2, 'test2') returning a, b) select a, b from result;

--Testcase 795:
drop trigger remp1_br_insert_trigger on remp1;
--Testcase 796:
drop trigger remp2_br_insert_trigger on remp2;

--Testcase 797:
drop table itrtest;
-- drop table loct1;
-- drop table loct2;
--Testcase 798:
SELECT oracle_execute('oracle_srv', 'DROP TABLE test.LOCT1 PURGE');
--Testcase 799:
SELECT oracle_execute('oracle_srv', 'DROP TABLE test.LOCT2 PURGE');

-- Test update tuple routing
--Testcase 800:
create table utrtest (id serial, a int, b text) partition by list (a);
-- create table loct (a int check (a in (1)), b text);
--Testcase 801:
SELECT oracle_execute(
          'oracle_srv',
          E'CREATE TABLE test.LOCT (\n'
          '   id  NUMBER(5) PRIMARY KEY,\n'
          '   a  NUMBER(5),\n'
          '   b  CLOB\n'
          ') SEGMENT CREATION IMMEDIATE'
       );
--Testcase 802:
create foreign table remp (id serial OPTIONS (key 'yes'), a int check (a in (1)), b text)
  server oracle_srv options (table 'LOCT');
--Testcase 803:
create table locp (id serial, a int check (a in (2)), b text);
--Testcase 804:
alter table utrtest attach partition remp for values in (1);
--Testcase 805:
alter table utrtest attach partition locp for values in (2);

--Testcase 806:
insert into utrtest(a, b) values (1, 'foo');
--Testcase 807:
insert into utrtest(a, b) values (2, 'qux');

--Testcase 808:
select tableoid::regclass, a, b FROM utrtest;
--Testcase 809:
select tableoid::regclass, a, b FROM remp;
--Testcase 810:
select tableoid::regclass, a, b FROM locp;

-- It's not allowed to move a row from a partition that is foreign to another
-- oracle fdw does not support
-- update utrtest set a = 2 where b = 'foo' returning a, b;

-- But the reverse is allowed
--Testcase 811:
update utrtest set a = 1 where b = 'qux' returning a, b;

--Testcase 812:
select tableoid::regclass, a, b FROM utrtest;
--Testcase 813:
select tableoid::regclass, a, b FROM remp;
--Testcase 814:
select tableoid::regclass, a, b FROM locp;

-- The executor should not let unexercised FDWs shut down
--Testcase 815:
update utrtest set a = 1 where b = 'foo';

-- Test that remote triggers work with update tuple routing
--Testcase 816:
create trigger loct_br_insert_trigger before insert on remp
	for each row execute procedure br_insert_trigfunc();

--Testcase 817:
delete from utrtest;
--Testcase 818:
insert into utrtest(a, b) values (2, 'qux');

-- Check case where the foreign partition is a subplan target rel
--Testcase 819:
explain (costs off)
update utrtest set a = 1 where a = 1 or a = 2 returning a, b;
-- The new values are concatenated with ' triggered !'
--Testcase 820:
update utrtest set a = 1 where a = 1 or a = 2 returning a, b;

--Testcase 821:
delete from utrtest;
--Testcase 822:
insert into utrtest(a, b) values (2, 'qux');

-- Check case where the foreign partition isn't a subplan target rel
--Testcase 823:
explain (costs off)
update utrtest set a = 1 where a = 2 returning a, b;
-- The new values are concatenated with ' triggered !'
--Testcase 824:
update utrtest set a = 1 where a = 2 returning a, b;

--Testcase 825:
drop trigger loct_br_insert_trigger on remp;

-- We can move rows to a foreign partition that has been updated already,
-- but can't move rows to a foreign partition that hasn't been updated yet

--Testcase 826:
delete from utrtest;
--Testcase 827:
insert into utrtest(a, b) values (1, 'foo');
--Testcase 828:
insert into utrtest(a, b) values (2, 'qux');

-- Test the former case:
-- with a direct modification plan
--Testcase 829:
explain (costs off)
update utrtest set a = 1 returning *;
--Testcase 830:
update utrtest set a = 1 returning *;

--Testcase 831:
delete from utrtest;
--Testcase 832:
insert into utrtest(a, b) values (1, 'foo');
--Testcase 833:
insert into utrtest(a, b) values (2, 'qux');

-- with a non-direct modification plan
--Testcase 834:
explain (costs off)
update utrtest set a = 1 from (values (1), (2)) s(x) where a = s.x returning *;
--Testcase 835:
update utrtest set a = 1 from (values (1), (2)) s(x) where a = s.x returning *;

-- Change the definition of utrtest so that the foreign partition get updated
-- after the local partition
--Testcase 836:
delete from utrtest;
--Testcase 837:
alter table utrtest detach partition remp;
--Testcase 838:
drop foreign table remp;
-- alter table loct drop constraint loct_a_check;
-- alter table loct add check (a in (3));
--Testcase 839:
create foreign table remp (id serial OPTIONS (key 'yes'), a int check (a in (3)), b text) server oracle_srv options (table 'LOCT');
--Testcase 840:
alter table utrtest attach partition remp for values in (3);
--Testcase 841:
insert into utrtest(a, b) values (2, 'qux');
--Testcase 842:
insert into utrtest(a, b) values (3, 'xyzzy');

-- Test the latter case:
-- with a direct modification plan
--Testcase 843:
explain (costs off)
update utrtest set a = 3 returning a, b;
--Testcase 844:
update utrtest set a = 3 returning a, b; -- ERROR

-- with a non-direct modification plan
--Testcase 845:
explain (costs off)
update utrtest set a = 3 from (values (2), (3)) s(x) where a = s.x returning a, b;
--Testcase 846:
update utrtest set a = 3 from (values (2), (3)) s(x) where a = s.x returning a, b; -- ERROR

--Testcase 847:
drop table utrtest;
-- drop table loct;
--Testcase 848:
SELECT oracle_execute('oracle_srv', 'DROP TABLE test.LOCT PURGE');


-- Test copy tuple routing
--Testcase 849:
create table ctrtest (id serial, a int, b text) partition by list (a);
--create table loct1 (a int check (a in (1)), b text);
--create table loct2 (a int check (a in (2)), b text);
--Testcase 850:
SELECT oracle_execute(
          'oracle_srv',
          E'CREATE TABLE test.LOCT1 (\n'
          '   id  NUMBER(5) PRIMARY KEY,\n'
          '   a  NUMBER(5),\n'
          '   b  CLOB\n'
          ') SEGMENT CREATION IMMEDIATE'
       );

--Testcase 851:
SELECT oracle_execute(
          'oracle_srv',
          E'CREATE TABLE test.LOCT2 (\n'
          '   id  NUMBER(5) PRIMARY KEY,\n'
          '   b  CLOB,\n'
          '   a  NUMBER(5)\n'
          ') SEGMENT CREATION IMMEDIATE'
       );

--Testcase 852:
create foreign table remp1 (id serial, a int check (a in (1)), b text)
  server oracle_srv options (table 'LOCT1');
--Testcase 853:
create foreign table remp2 (id serial, b text, a int check (a in (2)))
  server oracle_srv options (table 'LOCT2');

--Testcase 854:
alter foreign table remp1 alter column id options (key 'yes');
--Testcase 855:
alter foreign table remp2 alter column id options (key 'yes');
--Testcase 856:
alter table ctrtest attach partition remp1 for values in (1);
--Testcase 857:
alter table ctrtest attach partition remp2 for values in (2);


copy ctrtest(a, b) from stdin;
1	foo
2	qux
\.

--Testcase 858:
select tableoid::regclass, a, b FROM ctrtest;
--Testcase 859:
select tableoid::regclass, a, b FROM remp1;
--Testcase 860:
select tableoid::regclass, b, a FROM remp2;

-- Copying into foreign partitions directly should work as well
-- set start value of id column to avoid unique constraint
-- because id will be reset when copy data on new table 
--Testcase 861:
select pg_catalog.setval('remp1_id_seq', 100, false);
copy remp1(a, b) from stdin;
1	bar
\.

--Testcase 862:
select tableoid::regclass, a, b FROM remp1;

--Testcase 863:
drop table ctrtest;
-- drop table loct1;
-- drop table loct2;
--Testcase 864:
SELECT oracle_execute('oracle_srv', 'DROP TABLE test.LOCT1 PURGE');
--Testcase 865:
SELECT oracle_execute('oracle_srv', 'DROP TABLE test.LOCT2 PURGE');

-- ===================================================================
-- test COPY FROM
-- ===================================================================

--create table loc2 (f1 int, f2 text);
--alter table loc2 set (autovacuum_enabled = 'false');
--Testcase 866:
SELECT oracle_execute(
          'oracle_srv',
          E'CREATE TABLE test.loc2 (\n'
          '   id  NUMBER(5) PRIMARY KEY,\n'
          '   f1  NUMBER(5),\n'
          '   f2  CLOB\n'
          ') SEGMENT CREATION IMMEDIATE'
       );
--Testcase 867:
create foreign table rem2 (id serial options (key 'yes'), f1 int, f2 text)
  server oracle_srv options(table 'LOC2');

-- Test basic functionality
copy rem2(f1, f2) from stdin;
1	foo
2	bar
\.
--Testcase 868:
select f1, f2 from rem2;

--Testcase 869:
delete from rem2;

-- Test check constraints
--alter table loc2 add constraint loc2_f1positive check (f1 >= 0);
--Testcase 870:
SELECT oracle_execute(
          'oracle_srv',
          E'ALTER TABLE test.loc2 \n'
          '   ADD CONSTRAINT loc2_f1positive CHECK (f1 >= 0)'
        );
--Testcase 871:
alter foreign table rem2 add constraint rem2_f1positive check (f1 >= 0);

-- check constraint is enforced on the remote side, not locally
copy rem2(f1, f2) from stdin;
1	foo
2	bar
\.
copy rem2(f1, f2) from stdin; -- ERROR
-1	xyzzy
\.
--Testcase 872:
select f1, f2 from rem2;

--Testcase 873:
alter foreign table rem2 drop constraint rem2_f1positive;
--alter table loc2 drop constraint loc2_f1positive;
--Testcase 874:
SELECT oracle_execute(
          'oracle_srv',
          E'ALTER TABLE test.loc2 \n'
          '   DROP CONSTRAINT loc2_f1positive'
        );

--Testcase 875:
delete from rem2;

-- Test local triggers
--Testcase 876:
create trigger trig_stmt_before before insert on rem2
	for each statement execute procedure trigger_func();
--Testcase 877:
create trigger trig_stmt_after after insert on rem2
	for each statement execute procedure trigger_func();
--Testcase 878:
create trigger trig_row_before before insert on rem2
	for each row execute procedure trigger_data(23,'skidoo');
--Testcase 879:
create trigger trig_row_after after insert on rem2
	for each row execute procedure trigger_data(23,'skidoo');

copy rem2(f1, f2) from stdin;
1	foo
2	bar
\.
--Testcase 880:
select f1, f2 from rem2;

--Testcase 881:
drop trigger trig_row_before on rem2;
--Testcase 882:
drop trigger trig_row_after on rem2;
--Testcase 883:
drop trigger trig_stmt_before on rem2;
--Testcase 884:
drop trigger trig_stmt_after on rem2;

--Testcase 885:
delete from rem2;

--Testcase 886:
create trigger trig_row_before_insert before insert on rem2
	for each row execute procedure trig_row_before_insupdate();

-- The new values are concatenated with ' triggered !'
copy rem2(f1, f2) from stdin;
1	foo
2	bar
\.
--Testcase 887:
select f1, f2 from rem2;

--Testcase 888:
drop trigger trig_row_before_insert on rem2;

--Testcase 889:
delete from rem2;

--Testcase 890:
create trigger trig_null before insert on rem2
	for each row execute procedure trig_null();

-- Nothing happens
copy rem2(f1, f2) from stdin;
1	foo
2	bar
\.
--Testcase 891:
select f1, f2 from rem2;

--Testcase 892:
drop trigger trig_null on rem2;

--Testcase 893:
delete from rem2;

-- Test remote triggers
--Testcase 894:
create trigger trig_row_before_insert before insert on rem2
	for each row execute procedure trig_row_before_insupdate();

-- The new values are concatenated with ' triggered !'
copy rem2(f1, f2) from stdin;
1	foo
2	bar
\.
--Testcase 895:
select f1, f2 from rem2;

--Testcase 896:
drop trigger trig_row_before_insert on rem2;

--Testcase 897:
delete from rem2;

--Testcase 898:
create trigger trig_null before insert on rem2
	for each row execute procedure trig_null();

-- Nothing happens
copy rem2(f1, f2) from stdin;
1	foo
2	bar
\.
--Testcase 899:
select f1, f2 from rem2;

--Testcase 900:
drop trigger trig_null on rem2;

--Testcase 901:
delete from rem2;

-- Test a combination of local and remote triggers
--Testcase 902:
create trigger rem2_trig_row_before before insert on rem2
	for each row execute procedure trigger_data(23,'skidoo');
--Testcase 903:
create trigger rem2_trig_row_after after insert on rem2
	for each row execute procedure trigger_data(23,'skidoo');
--Testcase 904:
create trigger loc2_trig_row_before_insert before insert on rem2
	for each row execute procedure trig_row_before_insupdate();

copy rem2(f1, f2) from stdin;
1	foo
2	bar
\.
--Testcase 905:
select f1, f2 from rem2;

--Testcase 906:
drop trigger rem2_trig_row_before on rem2;
--Testcase 907:
drop trigger rem2_trig_row_after on rem2;
--Testcase 908:
drop trigger loc2_trig_row_before_insert on rem2;

--Testcase 909:
delete from rem2;
--Testcase 910:
SELECT oracle_execute('oracle_srv', 'DROP TABLE test.loc2 PURGE');

-- test COPY FROM with foreign table created in the same transaction
--create table loc3 (f1 int, f2 text);
--Testcase 911:
SELECT oracle_execute(
          'oracle_srv',
          E'CREATE TABLE test.loc3 (\n'
          '   id  NUMBER(5) PRIMARY KEY,\n'
          '   f1  NUMBER(5),\n'
          '   f2  CLOB\n'
          ') SEGMENT CREATION IMMEDIATE'
       );

begin;
--Testcase 912:
create foreign table rem3 (id serial options (key 'yes'), f1 int, f2 text)
	server oracle_srv options(table 'LOC3');
copy rem3(f1, f2) from stdin;
1	foo
2	bar
\.
commit;
--Testcase 913:
select f1, f2 from rem3;
--Testcase 914:
drop foreign table rem3;
--drop table loc3;
--Testcase 915:
SELECT oracle_execute('oracle_srv', 'DROP TABLE test.loc3 PURGE');

-- ===================================================================
-- test for TRUNCATE
-- oracle fdw does not support TRUNCATE
-- ===================================================================
-- CREATE TABLE tru_rtable0 (id int primary key);
-- CREATE FOREIGN TABLE tru_ftable (id int)
--        SERVER oracle_srv OPTIONS (table 'tru_rtable0');
-- INSERT INTO tru_rtable0 (SELECT x FROM generate_series(1,10) x);

-- CREATE TABLE tru_ptable (id int) PARTITION BY HASH(id);
-- CREATE TABLE tru_ptable__p0 PARTITION OF tru_ptable
--                             FOR VALUES WITH (MODULUS 2, REMAINDER 0);
-- CREATE TABLE tru_rtable1 (id int primary key);
-- CREATE FOREIGN TABLE tru_ftable__p1 PARTITION OF tru_ptable
--                                     FOR VALUES WITH (MODULUS 2, REMAINDER 1)
--        SERVER oracle_srv OPTIONS (table 'tru_rtable1');
-- INSERT INTO tru_ptable (SELECT x FROM generate_series(11,20) x);

-- CREATE TABLE tru_pk_table(id int primary key);
-- CREATE TABLE tru_fk_table(fkey int references tru_pk_table(id));
-- INSERT INTO tru_pk_table (SELECT x FROM generate_series(1,10) x);
-- INSERT INTO tru_fk_table (SELECT x % 10 + 1 FROM generate_series(5,25) x);
-- CREATE FOREIGN TABLE tru_pk_ftable (id int)
--        SERVER oracle_srv OPTIONS (table 'tru_pk_table');

-- CREATE TABLE tru_rtable_parent (id int);
-- CREATE TABLE tru_rtable_child (id int);
-- CREATE FOREIGN TABLE tru_ftable_parent (id int)
--        SERVER oracle_srv OPTIONS (table 'tru_rtable_parent');
-- CREATE FOREIGN TABLE tru_ftable_child () INHERITS (tru_ftable_parent)
--        SERVER oracle_srv OPTIONS (table 'tru_rtable_child');
-- INSERT INTO tru_rtable_parent (SELECT x FROM generate_series(1,8) x);
-- INSERT INTO tru_rtable_child  (SELECT x FROM generate_series(10, 18) x);

-- -- normal truncate
-- SELECT sum(id) FROM tru_ftable;        -- 55
-- TRUNCATE tru_ftable;
-- SELECT count(*) FROM tru_rtable0;		-- 0
-- SELECT count(*) FROM tru_ftable;		-- 0

-- -- 'truncatable' option
-- ALTER SERVER oracle_srv OPTIONS (ADD truncatable 'false');
-- TRUNCATE tru_ftable;			-- error
-- ALTER FOREIGN TABLE tru_ftable OPTIONS (ADD truncatable 'true');
-- TRUNCATE tru_ftable;			-- accepted
-- ALTER FOREIGN TABLE tru_ftable OPTIONS (SET truncatable 'false');
-- TRUNCATE tru_ftable;			-- error
-- ALTER SERVER oracle_srv OPTIONS (DROP truncatable);
-- ALTER FOREIGN TABLE tru_ftable OPTIONS (SET truncatable 'false');
-- TRUNCATE tru_ftable;			-- error
-- ALTER FOREIGN TABLE tru_ftable OPTIONS (SET truncatable 'true');
-- TRUNCATE tru_ftable;			-- accepted

-- -- partitioned table with both local and foreign tables as partitions
-- SELECT sum(id) FROM tru_ptable;        -- 155
-- TRUNCATE tru_ptable;
-- SELECT count(*) FROM tru_ptable;		-- 0
-- SELECT count(*) FROM tru_ptable__p0;	-- 0
-- SELECT count(*) FROM tru_ftable__p1;	-- 0
-- SELECT count(*) FROM tru_rtable1;		-- 0

-- -- 'CASCADE' option
-- SELECT sum(id) FROM tru_pk_ftable;      -- 55
-- TRUNCATE tru_pk_ftable;	-- failed by FK reference
-- TRUNCATE tru_pk_ftable CASCADE;
-- SELECT count(*) FROM tru_pk_ftable;    -- 0
-- SELECT count(*) FROM tru_fk_table;		-- also truncated,0

-- -- truncate two tables at a command
-- INSERT INTO tru_ftable (SELECT x FROM generate_series(1,8) x);
-- INSERT INTO tru_pk_ftable (SELECT x FROM generate_series(3,10) x);
-- SELECT count(*) from tru_ftable; -- 8
-- SELECT count(*) from tru_pk_ftable; -- 8
-- TRUNCATE tru_ftable, tru_pk_ftable CASCADE;
-- SELECT count(*) from tru_ftable; -- 0
-- SELECT count(*) from tru_pk_ftable; -- 0

-- -- truncate with ONLY clause
-- -- Since ONLY is specified, the table tru_ftable_child that inherits
-- -- tru_ftable_parent locally is not truncated.
-- TRUNCATE ONLY tru_ftable_parent;
-- SELECT sum(id) FROM tru_ftable_parent;  -- 126
-- TRUNCATE tru_ftable_parent;
-- SELECT count(*) FROM tru_ftable_parent; -- 0

-- -- in case when remote table has inherited children
-- CREATE TABLE tru_rtable0_child () INHERITS (tru_rtable0);
-- INSERT INTO tru_rtable0 (SELECT x FROM generate_series(5,9) x);
-- INSERT INTO tru_rtable0_child (SELECT x FROM generate_series(10,14) x);
-- SELECT sum(id) FROM tru_ftable;   -- 95

-- -- Both parent and child tables in the foreign server are truncated
-- -- even though ONLY is specified because ONLY has no effect
-- -- when truncating a foreign table.
-- TRUNCATE ONLY tru_ftable;
-- SELECT count(*) FROM tru_ftable;   -- 0

-- INSERT INTO tru_rtable0 (SELECT x FROM generate_series(21,25) x);
-- INSERT INTO tru_rtable0_child (SELECT x FROM generate_series(26,30) x);
-- SELECT sum(id) FROM tru_ftable;		-- 255
-- TRUNCATE tru_ftable;			-- truncate both of parent and child
-- SELECT count(*) FROM tru_ftable;    -- 0

-- -- cleanup
-- DROP FOREIGN TABLE tru_ftable_parent, tru_ftable_child, tru_pk_ftable,tru_ftable__p1,tru_ftable;
-- DROP TABLE tru_rtable0, tru_rtable1, tru_ptable, tru_ptable__p0, tru_pk_table, tru_fk_table,
-- tru_rtable_parent,tru_rtable_child, tru_rtable0_child;

-- ===================================================================
-- test IMPORT FOREIGN SCHEMA
-- note: already used in the begining of the test file
-- ===================================================================

-- CREATE SCHEMA import_source;
-- CREATE TABLE import_source.t1 (c1 int, c2 varchar NOT NULL);
-- CREATE TABLE import_source.t2 (c1 int default 42, c2 varchar NULL, c3 text collate "POSIX");
-- CREATE TYPE typ1 AS (m1 int, m2 varchar);
-- CREATE TABLE import_source.t3 (c1 timestamptz default now(), c2 typ1);
-- CREATE TABLE import_source."x 4" (c1 float8, "C 2" text, c3 varchar(42));
-- CREATE TABLE import_source."x 5" (c1 float8);
-- ALTER TABLE import_source."x 5" DROP COLUMN c1;
-- CREATE TABLE import_source."x 6" (c1 int, c2 int generated always as (c1 * 2) stored);
-- CREATE TABLE import_source.t4 (c1 int) PARTITION BY RANGE (c1);
-- CREATE TABLE import_source.t4_part PARTITION OF import_source.t4
--   FOR VALUES FROM (1) TO (100);
-- CREATE TABLE import_source.t4_part2 PARTITION OF import_source.t4
--   FOR VALUES FROM (100) TO (200);

-- CREATE SCHEMA import_dest1;
-- IMPORT FOREIGN SCHEMA import_source FROM SERVER oracle_srv INTO import_dest1;
-- \det+ import_dest1.*
-- \d import_dest1.*

-- -- Options
-- CREATE SCHEMA import_dest2;
-- IMPORT FOREIGN SCHEMA import_source FROM SERVER oracle_srv INTO import_dest2
--   OPTIONS (import_default 'true');
-- \det+ import_dest2.*
-- \d import_dest2.*
-- CREATE SCHEMA import_dest3;
-- IMPORT FOREIGN SCHEMA import_source FROM SERVER oracle_srv INTO import_dest3
--   OPTIONS (import_collate 'false', import_generated 'false', import_not_null 'false');
-- \det+ import_dest3.*
-- \d import_dest3.*

-- -- Check LIMIT TO and EXCEPT
-- CREATE SCHEMA import_dest4;
-- IMPORT FOREIGN SCHEMA import_source LIMIT TO (t1, nonesuch, t4_part)
--   FROM SERVER oracle_srv INTO import_dest4;
-- \det+ import_dest4.*
-- IMPORT FOREIGN SCHEMA import_source EXCEPT (t1, "x 4", nonesuch, t4_part)
--   FROM SERVER oracle_srv INTO import_dest4;
-- \det+ import_dest4.*

-- -- Assorted error cases
-- IMPORT FOREIGN SCHEMA import_source FROM SERVER oracle_srv INTO import_dest4;
-- IMPORT FOREIGN SCHEMA nonesuch FROM SERVER oracle_srv INTO import_dest4;
-- IMPORT FOREIGN SCHEMA nonesuch FROM SERVER oracle_srv INTO notthere;
-- IMPORT FOREIGN SCHEMA nonesuch FROM SERVER nowhere INTO notthere;

-- -- Check case of a type present only on the remote server.
-- -- We can fake this by dropping the type locally in our transaction.
-- CREATE TYPE "Colors" AS ENUM ('red', 'green', 'blue');
-- CREATE TABLE import_source.t5 (c1 int, c2 text collate "C", "Col" "Colors");

-- CREATE SCHEMA import_dest5;
-- BEGIN;
-- DROP TYPE "Colors" CASCADE;
-- IMPORT FOREIGN SCHEMA import_source LIMIT TO (t5)
--   FROM SERVER oracle_srv INTO import_dest5;  -- ERROR

-- ROLLBACK;

-- BEGIN;


-- CREATE SERVER fetch101 FOREIGN DATA WRAPPER oracle_fdw OPTIONS( fetch_size '101' );

-- SELECT count(*)
-- FROM pg_foreign_server
-- WHERE srvname = 'fetch101'
-- AND srvoptions @> array['fetch_size=101'];

-- ALTER SERVER fetch101 OPTIONS( SET fetch_size '202' );

-- SELECT count(*)
-- FROM pg_foreign_server
-- WHERE srvname = 'fetch101'
-- AND srvoptions @> array['fetch_size=101'];

-- SELECT count(*)
-- FROM pg_foreign_server
-- WHERE srvname = 'fetch101'
-- AND srvoptions @> array['fetch_size=202'];

-- CREATE FOREIGN TABLE table30000 ( x int ) SERVER fetch101 OPTIONS ( fetch_size '30000' );

-- SELECT COUNT(*)
-- FROM pg_foreign_table
-- WHERE ftrelid = 'table30000'::regclass
-- AND ftoptions @> array['fetch_size=30000'];

-- ALTER FOREIGN TABLE table30000 OPTIONS ( SET fetch_size '60000');

-- SELECT COUNT(*)
-- FROM pg_foreign_table
-- WHERE ftrelid = 'table30000'::regclass
-- AND ftoptions @> array['fetch_size=30000'];

-- SELECT COUNT(*)
-- FROM pg_foreign_table
-- WHERE ftrelid = 'table30000'::regclass
-- AND ftoptions @> array['fetch_size=60000'];

-- ROLLBACK;

-- ===================================================================
-- test partitionwise joins
-- oracle_fdw does not support this feature
-- ===================================================================
-- SET enable_partitionwise_join=on;

-- CREATE TABLE fprt1 (a int, b int, c varchar) PARTITION BY RANGE(a);
-- CREATE TABLE fprt1_p1 (LIKE fprt1);
-- CREATE TABLE fprt1_p2 (LIKE fprt1);
-- ALTER TABLE fprt1_p1 SET (autovacuum_enabled = 'false');
-- ALTER TABLE fprt1_p2 SET (autovacuum_enabled = 'false');
-- INSERT INTO fprt1_p1 SELECT i, i, to_char(i/50, 'FM0000') FROM generate_series(0, 249, 2) i;
-- INSERT INTO fprt1_p2 SELECT i, i, to_char(i/50, 'FM0000') FROM generate_series(250, 499, 2) i;
-- CREATE FOREIGN TABLE ftprt1_p1 PARTITION OF fprt1 FOR VALUES FROM (0) TO (250)
-- 	SERVER oracle_srv OPTIONS (table_name 'fprt1_p1', use_remote_estimate 'true');
-- CREATE FOREIGN TABLE ftprt1_p2 PARTITION OF fprt1 FOR VALUES FROM (250) TO (500)
-- 	SERVER oracle_srv OPTIONS (TABLE_NAME 'fprt1_p2');
-- ANALYZE fprt1;
-- ANALYZE fprt1_p1;
-- ANALYZE fprt1_p2;

-- CREATE TABLE fprt2 (a int, b int, c varchar) PARTITION BY RANGE(b);
-- CREATE TABLE fprt2_p1 (LIKE fprt2);
-- CREATE TABLE fprt2_p2 (LIKE fprt2);
-- ALTER TABLE fprt2_p1 SET (autovacuum_enabled = 'false');
-- ALTER TABLE fprt2_p2 SET (autovacuum_enabled = 'false');
-- INSERT INTO fprt2_p1 SELECT i, i, to_char(i/50, 'FM0000') FROM generate_series(0, 249, 3) i;
-- INSERT INTO fprt2_p2 SELECT i, i, to_char(i/50, 'FM0000') FROM generate_series(250, 499, 3) i;
-- CREATE FOREIGN TABLE ftprt2_p1 (b int, c varchar, a int)
-- 	SERVER oracle_srv OPTIONS (table_name 'fprt2_p1', use_remote_estimate 'true');
-- ALTER TABLE fprt2 ATTACH PARTITION ftprt2_p1 FOR VALUES FROM (0) TO (250);
-- CREATE FOREIGN TABLE ftprt2_p2 PARTITION OF fprt2 FOR VALUES FROM (250) TO (500)
-- 	SERVER oracle_srv OPTIONS (table_name 'fprt2_p2', use_remote_estimate 'true');
-- ANALYZE fprt2;
-- ANALYZE fprt2_p1;
-- ANALYZE fprt2_p2;

-- -- inner join three tables
-- EXPLAIN (COSTS OFF)
-- SELECT t1.a,t2.b,t3.c FROM fprt1 t1 INNER JOIN fprt2 t2 ON (t1.a = t2.b) INNER JOIN fprt1 t3 ON (t2.b = t3.a) WHERE t1.a % 25 =0 ORDER BY 1,2,3;
-- SELECT t1.a,t2.b,t3.c FROM fprt1 t1 INNER JOIN fprt2 t2 ON (t1.a = t2.b) INNER JOIN fprt1 t3 ON (t2.b = t3.a) WHERE t1.a % 25 =0 ORDER BY 1,2,3;

-- -- left outer join + nullable clause
-- EXPLAIN (COSTS OFF)
-- SELECT t1.a,t2.b,t2.c FROM fprt1 t1 LEFT JOIN (SELECT * FROM fprt2 WHERE a < 10) t2 ON (t1.a = t2.b and t1.b = t2.a) WHERE t1.a < 10 ORDER BY 1,2,3;
-- SELECT t1.a,t2.b,t2.c FROM fprt1 t1 LEFT JOIN (SELECT * FROM fprt2 WHERE a < 10) t2 ON (t1.a = t2.b and t1.b = t2.a) WHERE t1.a < 10 ORDER BY 1,2,3;

-- -- with whole-row reference; partitionwise join does not apply
-- EXPLAIN (COSTS OFF)
-- SELECT t1.wr, t2.wr FROM (SELECT t1 wr, a FROM fprt1 t1 WHERE t1.a % 25 = 0) t1 FULL JOIN (SELECT t2 wr, b FROM fprt2 t2 WHERE t2.b % 25 = 0) t2 ON (t1.a = t2.b) ORDER BY 1,2;
-- SELECT t1.wr, t2.wr FROM (SELECT t1 wr, a FROM fprt1 t1 WHERE t1.a % 25 = 0) t1 FULL JOIN (SELECT t2 wr, b FROM fprt2 t2 WHERE t2.b % 25 = 0) t2 ON (t1.a = t2.b) ORDER BY 1,2;

-- -- join with lateral reference
-- EXPLAIN (COSTS OFF)
-- SELECT t1.a,t1.b FROM fprt1 t1, LATERAL (SELECT t2.a, t2.b FROM fprt2 t2 WHERE t1.a = t2.b AND t1.b = t2.a) q WHERE t1.a%25 = 0 ORDER BY 1,2;
-- SELECT t1.a,t1.b FROM fprt1 t1, LATERAL (SELECT t2.a, t2.b FROM fprt2 t2 WHERE t1.a = t2.b AND t1.b = t2.a) q WHERE t1.a%25 = 0 ORDER BY 1,2;

-- -- with PHVs, partitionwise join selected but no join pushdown
-- EXPLAIN (COSTS OFF)
-- SELECT t1.a, t1.phv, t2.b, t2.phv FROM (SELECT 't1_phv' phv, * FROM fprt1 WHERE a % 25 = 0) t1 FULL JOIN (SELECT 't2_phv' phv, * FROM fprt2 WHERE b % 25 = 0) t2 ON (t1.a = t2.b) ORDER BY t1.a, t2.b;
-- SELECT t1.a, t1.phv, t2.b, t2.phv FROM (SELECT 't1_phv' phv, * FROM fprt1 WHERE a % 25 = 0) t1 FULL JOIN (SELECT 't2_phv' phv, * FROM fprt2 WHERE b % 25 = 0) t2 ON (t1.a = t2.b) ORDER BY t1.a, t2.b;

-- -- test FOR UPDATE; partitionwise join does not apply
-- EXPLAIN (COSTS OFF)
-- SELECT t1.a, t2.b FROM fprt1 t1 INNER JOIN fprt2 t2 ON (t1.a = t2.b) WHERE t1.a % 25 = 0 ORDER BY 1,2 FOR UPDATE OF t1;
-- SELECT t1.a, t2.b FROM fprt1 t1 INNER JOIN fprt2 t2 ON (t1.a = t2.b) WHERE t1.a % 25 = 0 ORDER BY 1,2 FOR UPDATE OF t1;

-- RESET enable_partitionwise_join;


-- ===================================================================
-- test partitionwise aggregates
-- oracle_fdw does not support this feature
-- ===================================================================

-- CREATE TABLE pagg_tab (a int, b int, c text) PARTITION BY RANGE(a);

-- CREATE TABLE pagg_tab_p1 (LIKE pagg_tab);
-- CREATE TABLE pagg_tab_p2 (LIKE pagg_tab);
-- CREATE TABLE pagg_tab_p3 (LIKE pagg_tab);

-- INSERT INTO pagg_tab_p1 SELECT i % 30, i % 50, to_char(i/30, 'FM0000') FROM generate_series(1, 3000) i WHERE (i % 30) < 10;
-- INSERT INTO pagg_tab_p2 SELECT i % 30, i % 50, to_char(i/30, 'FM0000') FROM generate_series(1, 3000) i WHERE (i % 30) < 20 and (i % 30) >= 10;
-- INSERT INTO pagg_tab_p3 SELECT i % 30, i % 50, to_char(i/30, 'FM0000') FROM generate_series(1, 3000) i WHERE (i % 30) < 30 and (i % 30) >= 20;

-- -- Create foreign partitions
-- CREATE FOREIGN TABLE fpagg_tab_p1 PARTITION OF pagg_tab FOR VALUES FROM (0) TO (10) SERVER oracle_srv OPTIONS (table_name 'pagg_tab_p1');
-- CREATE FOREIGN TABLE fpagg_tab_p2 PARTITION OF pagg_tab FOR VALUES FROM (10) TO (20) SERVER oracle_srv OPTIONS (table_name 'pagg_tab_p2');
-- CREATE FOREIGN TABLE fpagg_tab_p3 PARTITION OF pagg_tab FOR VALUES FROM (20) TO (30) SERVER oracle_srv OPTIONS (table_name 'pagg_tab_p3');

-- ANALYZE pagg_tab;
-- ANALYZE fpagg_tab_p1;
-- ANALYZE fpagg_tab_p2;
-- ANALYZE fpagg_tab_p3;

-- -- When GROUP BY clause matches with PARTITION KEY.
-- -- Plan with partitionwise aggregates is disabled
-- SET enable_partitionwise_aggregate TO false;
-- EXPLAIN (COSTS OFF)
-- SELECT a, sum(b), min(b), count(*) FROM pagg_tab GROUP BY a HAVING avg(b) < 22 ORDER BY 1;

-- -- Plan with partitionwise aggregates is enabled
-- SET enable_partitionwise_aggregate TO true;
-- EXPLAIN (COSTS OFF)
-- SELECT a, sum(b), min(b), count(*) FROM pagg_tab GROUP BY a HAVING avg(b) < 22 ORDER BY 1;
-- SELECT a, sum(b), min(b), count(*) FROM pagg_tab GROUP BY a HAVING avg(b) < 22 ORDER BY 1;

-- -- Check with whole-row reference
-- -- Should have all the columns in the target list for the given relation
-- EXPLAIN (COSTS OFF)
-- SELECT a, count(t1) FROM pagg_tab t1 GROUP BY a HAVING avg(b) < 22 ORDER BY 1;
-- SELECT a, count(t1) FROM pagg_tab t1 GROUP BY a HAVING avg(b) < 22 ORDER BY 1;

-- -- When GROUP BY clause does not match with PARTITION KEY.
-- EXPLAIN (COSTS OFF)
-- SELECT b, avg(a), max(a), count(*) FROM pagg_tab GROUP BY b HAVING sum(a) < 700 ORDER BY 1;

-- ===================================================================
-- access rights and superuser
-- oracle_fdw does not support this feature
-- ===================================================================

-- Non-superuser cannot create a FDW without a password in the connstr
-- CREATE ROLE regress_nosuper NOSUPERUSER;

-- GRANT USAGE ON FOREIGN DATA WRAPPER oracle_fdw TO regress_nosuper;

-- SET ROLE regress_nosuper;

-- SHOW is_superuser;

-- -- This will be OK, we can create the FDW
-- DO $d$
--     BEGIN
--         EXECUTE $$CREATE SERVER loopback_nopw FOREIGN DATA WRAPPER oracle_fdw
--             OPTIONS (dbname '$$||current_database()||$$',
--                      port '$$||current_setting('port')||$$'
--             )$$;
--     END;
-- $d$;

-- But creation of user mappings for non-superusers should fail
-- CREATE USER MAPPING FOR public SERVER loopback_nopw;
-- CREATE USER MAPPING FOR CURRENT_USER SERVER loopback_nopw;

-- CREATE FOREIGN TABLE ft1_nopw (
-- 	c1 int NOT NULL,
-- 	c2 int NOT NULL,
-- 	c3 text,
-- 	c4 timestamptz,
-- 	c5 timestamp,
-- 	c6 varchar(10),
-- 	c7 char(10) default 'ft1',
-- 	c8 user_enum
-- ) SERVER loopback_nopw OPTIONS (schema_name 'public', table_name 'ft1');

-- SELECT 1 FROM ft1_nopw LIMIT 1;

-- -- If we add a password to the connstr it'll fail, because we don't allow passwords
-- -- in connstrs only in user mappings.

-- DO $d$
--     BEGIN
--         EXECUTE $$ALTER SERVER loopback_nopw OPTIONS (ADD password 'dummypw')$$;
--     END;
-- $d$;

-- -- If we add a password for our user mapping instead, we should get a different
-- -- error because the password wasn't actually *used* when we run with trust auth.
-- --
-- -- This won't work with installcheck, but neither will most of the FDW checks.

-- ALTER USER MAPPING FOR CURRENT_USER SERVER loopback_nopw OPTIONS (ADD password 'dummypw');

-- SELECT 1 FROM ft1_nopw LIMIT 1;

-- -- Unpriv user cannot make the mapping passwordless
-- ALTER USER MAPPING FOR CURRENT_USER SERVER loopback_nopw OPTIONS (ADD password_required 'false');


-- SELECT 1 FROM ft1_nopw LIMIT 1;

-- RESET ROLE;

-- -- But the superuser can
-- ALTER USER MAPPING FOR regress_nosuper SERVER loopback_nopw OPTIONS (ADD password_required 'false');

-- SET ROLE regress_nosuper;

-- -- Should finally work now
-- SELECT 1 FROM ft1_nopw LIMIT 1;

-- -- unpriv user also cannot set sslcert / sslkey on the user mapping
-- -- first set password_required so we see the right error messages
-- ALTER USER MAPPING FOR CURRENT_USER SERVER loopback_nopw OPTIONS (SET password_required 'true');
-- ALTER USER MAPPING FOR CURRENT_USER SERVER loopback_nopw OPTIONS (ADD sslcert 'foo.crt');
-- ALTER USER MAPPING FOR CURRENT_USER SERVER loopback_nopw OPTIONS (ADD sslkey 'foo.key');

-- -- We're done with the role named after a specific user and need to check the
-- -- changes to the public mapping.
-- DROP USER MAPPING FOR CURRENT_USER SERVER loopback_nopw;

-- -- This will fail again as it'll resolve the user mapping for public, which
-- -- lacks password_required=false
-- SELECT 1 FROM ft1_nopw LIMIT 1;

-- RESET ROLE;

-- -- The user mapping for public is passwordless and lacks the password_required=false
-- -- mapping option, but will work because the current user is a superuser.
-- SELECT 1 FROM ft1_nopw LIMIT 1;

-- -- cleanup
-- DROP USER MAPPING FOR public SERVER loopback_nopw;
-- DROP OWNED BY regress_nosuper;
-- DROP ROLE regress_nosuper;

-- -- Clean-up
-- RESET enable_partitionwise_aggregate;

-- -- Two-phase transactions are not supported.
-- BEGIN;
-- SELECT count(*) FROM ft1;
-- -- error here
-- PREPARE TRANSACTION 'fdw_tpc';
-- ROLLBACK;

-- ===================================================================
-- reestablish new connection
-- oracle_fdw does not support this feature
-- ===================================================================

-- -- Change application_name of remote connection to special one
-- -- so that we can easily terminate the connection later.
-- ALTER SERVER oracle_srv OPTIONS (application_name 'fdw_retry_check');

-- -- If debug_discard_caches is active, it results in
-- -- dropping remote connections after every transaction, making it
-- -- impossible to test termination meaningfully.  So turn that off
-- -- for this test.
-- SET debug_discard_caches = 0;

-- -- Make sure we have a remote connection.
-- SELECT 1 FROM ft1 LIMIT 1;

-- -- Terminate the remote connection and wait for the termination to complete.
-- SELECT pg_terminate_backend(pid, 180000) FROM pg_stat_activity
-- 	WHERE application_name = 'fdw_retry_check';

-- -- This query should detect the broken connection when starting new remote
-- -- transaction, reestablish new connection, and then succeed.
-- BEGIN;
-- SELECT 1 FROM ft1 LIMIT 1;

-- -- If we detect the broken connection when starting a new remote
-- -- subtransaction, we should fail instead of establishing a new connection.
-- -- Terminate the remote connection and wait for the termination to complete.
-- SELECT pg_terminate_backend(pid, 180000) FROM pg_stat_activity
-- 	WHERE application_name = 'fdw_retry_check';
-- SAVEPOINT s;
-- -- The text of the error might vary across platforms, so only show SQLSTATE.
-- \set VERBOSITY sqlstate
-- SELECT 1 FROM ft1 LIMIT 1;    -- should fail
-- \set VERBOSITY default
-- COMMIT;

-- RESET debug_discard_caches;

-- =============================================================================
-- test connection invalidation cases and postgres_fdw_get_connections function
-- oracle_fdw does not support this feature
-- =============================================================================
-- -- Let's ensure to close all the existing cached connections.
-- SELECT 1 FROM postgres_fdw_disconnect_all();
-- -- No cached connections, so no records should be output.
-- SELECT server_name FROM postgres_fdw_get_connections() ORDER BY 1;
-- -- This test case is for closing the connection in pgfdw_xact_callback
-- BEGIN;
-- -- Connection xact depth becomes 1 i.e. the connection is in midst of the xact.
-- SELECT 1 FROM ft1 LIMIT 1;
-- SELECT 1 FROM ft7 LIMIT 1;
-- -- List all the existing cached connections. oracle_srv and loopback3 should be
-- -- output.
-- SELECT server_name FROM postgres_fdw_get_connections() ORDER BY 1;
-- -- Connections are not closed at the end of the alter and drop statements.
-- -- That's because the connections are in midst of this xact,
-- -- they are just marked as invalid in pgfdw_inval_callback.
-- ALTER SERVER oracle_srv OPTIONS (ADD use_remote_estimate 'off');
-- DROP SERVER loopback3 CASCADE;
-- -- List all the existing cached connections. oracle_srv and loopback3
-- -- should be output as invalid connections. Also the server name for
-- -- loopback3 should be NULL because the server was dropped.
-- SELECT * FROM postgres_fdw_get_connections() ORDER BY 1;
-- -- The invalid connections get closed in pgfdw_xact_callback during commit.
-- COMMIT;
-- -- All cached connections were closed while committing above xact, so no
-- -- records should be output.
-- SELECT server_name FROM postgres_fdw_get_connections() ORDER BY 1;

-- =======================================================================
-- test postgres_fdw_disconnect and postgres_fdw_disconnect_all functions
-- oracle_fdw does not support this feature
-- =======================================================================
-- BEGIN;
-- -- Ensure to cache oracle_srv connection.
-- SELECT 1 FROM ft1 LIMIT 1;
-- -- Ensure to cache oracle_srv2 connection.
-- SELECT 1 FROM ft6 LIMIT 1;
-- -- List all the existing cached connections. oracle_srv and oracle_srv2 should be
-- -- output.
-- SELECT server_name FROM postgres_fdw_get_connections() ORDER BY 1;
-- -- Issue a warning and return false as oracle_srv connection is still in use and
-- -- can not be closed.
-- SELECT postgres_fdw_disconnect('oracle_srv');
-- -- List all the existing cached connections. oracle_srv and oracle_srv2 should be
-- -- output.
-- SELECT server_name FROM postgres_fdw_get_connections() ORDER BY 1;
-- -- Return false as connections are still in use, warnings are issued.
-- -- But disable warnings temporarily because the order of them is not stable.
-- SET client_min_messages = 'ERROR';
-- SELECT postgres_fdw_disconnect_all();
-- RESET client_min_messages;
-- COMMIT;
-- -- Ensure that oracle_srv2 connection is closed.
-- SELECT 1 FROM postgres_fdw_disconnect('oracle_srv2');
-- SELECT server_name FROM postgres_fdw_get_connections() WHERE server_name = 'oracle_srv2';
-- -- Return false as oracle_srv2 connection is closed already.
-- SELECT postgres_fdw_disconnect('oracle_srv2');
-- -- Return an error as there is no foreign server with given name.
-- SELECT postgres_fdw_disconnect('unknownserver');
-- -- Let's ensure to close all the existing cached connections.
-- SELECT 1 FROM postgres_fdw_disconnect_all();
-- -- No cached connections, so no records should be output.
-- SELECT server_name FROM postgres_fdw_get_connections() ORDER BY 1;

-- =============================================================================
-- test case for having multiple cached connections for a foreign server
-- oracle_fdw does not support this feature
-- =============================================================================
-- CREATE ROLE regress_multi_conn_user1 SUPERUSER;
-- CREATE ROLE regress_multi_conn_user2 SUPERUSER;
-- CREATE USER MAPPING FOR regress_multi_conn_user1 SERVER oracle_srv;
-- CREATE USER MAPPING FOR regress_multi_conn_user2 SERVER oracle_srv;

-- BEGIN;
-- -- Will cache oracle_srv connection with user mapping for regress_multi_conn_user1
-- SET ROLE regress_multi_conn_user1;
-- SELECT 1 FROM ft1 LIMIT 1;
-- RESET ROLE;

-- -- Will cache oracle_srv connection with user mapping for regress_multi_conn_user2
-- SET ROLE regress_multi_conn_user2;
-- SELECT 1 FROM ft1 LIMIT 1;
-- RESET ROLE;

-- -- Should output two connections for oracle_srv server
-- SELECT server_name FROM postgres_fdw_get_connections() ORDER BY 1;
-- COMMIT;
-- -- Let's ensure to close all the existing cached connections.
-- SELECT 1 FROM postgres_fdw_disconnect_all();
-- -- No cached connections, so no records should be output.
-- SELECT server_name FROM postgres_fdw_get_connections() ORDER BY 1;

-- -- Clean up
-- DROP USER MAPPING FOR regress_multi_conn_user1 SERVER oracle_srv;
-- DROP USER MAPPING FOR regress_multi_conn_user2 SERVER oracle_srv;
-- DROP ROLE regress_multi_conn_user1;
-- DROP ROLE regress_multi_conn_user2;

-- ===================================================================
-- Test foreign server level option keep_connections
-- oracle_fdw does not support this feature
-- ===================================================================
-- -- By default, the connections associated with foreign server are cached i.e.
-- -- keep_connections option is on. Set it to off.
-- ALTER SERVER oracle_srv OPTIONS (keep_connections 'off');
-- -- connection to oracle_srv server is closed at the end of xact
-- -- as keep_connections was set to off.
-- SELECT 1 FROM ft1 LIMIT 1;
-- -- No cached connections, so no records should be output.
-- SELECT server_name FROM postgres_fdw_get_connections() ORDER BY 1;
-- ALTER SERVER oracle_srv OPTIONS (SET keep_connections 'on');

-- ===================================================================
-- batch insert
-- oracle_fdw does not support this feature
-- ===================================================================

-- BEGIN;

-- CREATE SERVER batch10 FOREIGN DATA WRAPPER oracle_fdw OPTIONS( batch_size '10' );

-- SELECT count(*)
-- FROM pg_foreign_server
-- WHERE srvname = 'batch10'
-- AND srvoptions @> array['batch_size=10'];

-- ALTER SERVER batch10 OPTIONS( SET batch_size '20' );

-- SELECT count(*)
-- FROM pg_foreign_server
-- WHERE srvname = 'batch10'
-- AND srvoptions @> array['batch_size=10'];

-- SELECT count(*)
-- FROM pg_foreign_server
-- WHERE srvname = 'batch10'
-- AND srvoptions @> array['batch_size=20'];

-- CREATE FOREIGN TABLE table30 ( x int ) SERVER batch10 OPTIONS ( batch_size '30' );

-- SELECT COUNT(*)
-- FROM pg_foreign_table
-- WHERE ftrelid = 'table30'::regclass
-- AND ftoptions @> array['batch_size=30'];

-- ALTER FOREIGN TABLE table30 OPTIONS ( SET batch_size '40');

-- SELECT COUNT(*)
-- FROM pg_foreign_table
-- WHERE ftrelid = 'table30'::regclass
-- AND ftoptions @> array['batch_size=30'];

-- SELECT COUNT(*)
-- FROM pg_foreign_table
-- WHERE ftrelid = 'table30'::regclass
-- AND ftoptions @> array['batch_size=40'];

-- ROLLBACK;

-- CREATE TABLE batch_table ( x int );

-- CREATE FOREIGN TABLE ftable ( x int ) SERVER oracle_srv OPTIONS ( table_name 'batch_table', batch_size '10' );
-- EXPLAIN (COSTS OFF) INSERT INTO ftable SELECT * FROM generate_series(1, 10) i;
-- INSERT INTO ftable SELECT * FROM generate_series(1, 10) i;
-- INSERT INTO ftable SELECT * FROM generate_series(11, 31) i;
-- INSERT INTO ftable VALUES (32);
-- INSERT INTO ftable VALUES (33), (34);
-- SELECT COUNT(*) FROM ftable;
-- TRUNCATE batch_table;
-- DROP FOREIGN TABLE ftable;

-- -- try if large batches exceed max number of bind parameters
-- CREATE FOREIGN TABLE ftable ( x int ) SERVER oracle_srv OPTIONS ( table_name 'batch_table', batch_size '100000' );
-- INSERT INTO ftable SELECT * FROM generate_series(1, 70000) i;
-- SELECT COUNT(*) FROM ftable;
-- TRUNCATE batch_table;
-- DROP FOREIGN TABLE ftable;

-- -- Disable batch insert
-- CREATE FOREIGN TABLE ftable ( x int ) SERVER oracle_srv OPTIONS ( table_name 'batch_table', batch_size '1' );
-- EXPLAIN (COSTS OFF) INSERT INTO ftable VALUES (1), (2);
-- INSERT INTO ftable VALUES (1), (2);
-- SELECT COUNT(*) FROM ftable;
-- DROP FOREIGN TABLE ftable;
-- DROP TABLE batch_table;

-- -- Use partitioning
-- CREATE TABLE batch_table ( x int ) PARTITION BY HASH (x);

-- CREATE TABLE batch_table_p0 (LIKE batch_table);
-- CREATE FOREIGN TABLE batch_table_p0f
-- 	PARTITION OF batch_table
-- 	FOR VALUES WITH (MODULUS 3, REMAINDER 0)
-- 	SERVER oracle_srv
-- 	OPTIONS (table_name 'batch_table_p0', batch_size '10');

-- CREATE TABLE batch_table_p1 (LIKE batch_table);
-- CREATE FOREIGN TABLE batch_table_p1f
-- 	PARTITION OF batch_table
-- 	FOR VALUES WITH (MODULUS 3, REMAINDER 1)
-- 	SERVER oracle_srv
-- 	OPTIONS (table_name 'batch_table_p1', batch_size '1');

-- CREATE TABLE batch_table_p2
-- 	PARTITION OF batch_table
-- 	FOR VALUES WITH (MODULUS 3, REMAINDER 2);

-- INSERT INTO batch_table SELECT * FROM generate_series(1, 66) i;
-- SELECT COUNT(*) FROM batch_table;

-- -- Check that enabling batched inserts doesn't interfere with cross-partition
-- -- updates
-- CREATE TABLE batch_cp_upd_test (a int) PARTITION BY LIST (a);
-- CREATE TABLE batch_cp_upd_test1 (LIKE batch_cp_upd_test);
-- CREATE FOREIGN TABLE batch_cp_upd_test1_f
-- 	PARTITION OF batch_cp_upd_test
-- 	FOR VALUES IN (1)
-- 	SERVER oracle_srv
-- 	OPTIONS (table_name 'batch_cp_upd_test1', batch_size '10');
-- CREATE TABLE batch_cp_up_test1 PARTITION OF batch_cp_upd_test
-- 	FOR VALUES IN (2);
-- INSERT INTO batch_cp_upd_test VALUES (1), (2);

-- -- The following moves a row from the local partition to the foreign one
-- UPDATE batch_cp_upd_test t SET a = 1 FROM (VALUES (1), (2)) s(a) WHERE t.a = s.a;
-- SELECT tableoid::regclass, * FROM batch_cp_upd_test;

-- -- Clean up
-- DROP TABLE batch_table, batch_cp_upd_test, batch_table_p0, batch_table_p1 CASCADE;

-- -- Use partitioning
-- ALTER SERVER oracle_srv OPTIONS (ADD batch_size '10');

-- CREATE TABLE batch_table ( x int, field1 text, field2 text) PARTITION BY HASH (x);

-- CREATE TABLE batch_table_p0 (LIKE batch_table);
-- ALTER TABLE batch_table_p0 ADD CONSTRAINT p0_pkey PRIMARY KEY (x);
-- CREATE FOREIGN TABLE batch_table_p0f
-- 	PARTITION OF batch_table
-- 	FOR VALUES WITH (MODULUS 2, REMAINDER 0)
-- 	SERVER oracle_srv
-- 	OPTIONS (table_name 'batch_table_p0');

-- CREATE TABLE batch_table_p1 (LIKE batch_table);
-- ALTER TABLE batch_table_p1 ADD CONSTRAINT p1_pkey PRIMARY KEY (x);
-- CREATE FOREIGN TABLE batch_table_p1f
-- 	PARTITION OF batch_table
-- 	FOR VALUES WITH (MODULUS 2, REMAINDER 1)
-- 	SERVER oracle_srv
-- 	OPTIONS (table_name 'batch_table_p1');

-- INSERT INTO batch_table SELECT i, 'test'||i, 'test'|| i FROM generate_series(1, 50) i;
-- SELECT COUNT(*) FROM batch_table;
-- SELECT * FROM batch_table ORDER BY x;

-- ALTER SERVER oracle_srv OPTIONS (DROP batch_size);

-- ===================================================================
-- test asynchronous execution
-- oracle_fdw does not support this feature
-- ===================================================================

-- ALTER SERVER oracle_srv OPTIONS (DROP extensions);
-- ALTER SERVER oracle_srv OPTIONS (ADD async_capable 'true');
-- ALTER SERVER oracle_srv2 OPTIONS (ADD async_capable 'true');

-- CREATE TABLE async_pt (a int, b int, c text) PARTITION BY RANGE (a);
-- CREATE TABLE base_tbl1 (a int, b int, c text);
-- CREATE TABLE base_tbl2 (a int, b int, c text);
-- CREATE FOREIGN TABLE async_p1 PARTITION OF async_pt FOR VALUES FROM (1000) TO (2000)
--   SERVER oracle_srv OPTIONS (table_name 'base_tbl1');
-- CREATE FOREIGN TABLE async_p2 PARTITION OF async_pt FOR VALUES FROM (2000) TO (3000)
--   SERVER oracle_srv2 OPTIONS (table_name 'base_tbl2');
-- INSERT INTO async_p1 SELECT 1000 + i, i, to_char(i, 'FM0000') FROM generate_series(0, 999, 5) i;
-- INSERT INTO async_p2 SELECT 2000 + i, i, to_char(i, 'FM0000') FROM generate_series(0, 999, 5) i;
-- ANALYZE async_pt;

-- -- simple queries
-- CREATE TABLE result_tbl (a int, b int, c text);

-- EXPLAIN (COSTS OFF)
-- INSERT INTO result_tbl SELECT * FROM async_pt WHERE b % 100 = 0;
-- INSERT INTO result_tbl SELECT * FROM async_pt WHERE b % 100 = 0;

-- SELECT * FROM result_tbl ORDER BY a;
-- DELETE FROM result_tbl;

-- EXPLAIN (COSTS OFF)
-- INSERT INTO result_tbl SELECT * FROM async_pt WHERE b === 505;
-- INSERT INTO result_tbl SELECT * FROM async_pt WHERE b === 505;

-- SELECT * FROM result_tbl ORDER BY a;
-- DELETE FROM result_tbl;

-- -- Check case where multiple partitions use the same connection
-- CREATE TABLE base_tbl3 (a int, b int, c text);
-- CREATE FOREIGN TABLE async_p3 PARTITION OF async_pt FOR VALUES FROM (3000) TO (4000)
--   SERVER oracle_srv2 OPTIONS (table_name 'base_tbl3');
-- INSERT INTO async_p3 SELECT 3000 + i, i, to_char(i, 'FM0000') FROM generate_series(0, 999, 5) i;
-- ANALYZE async_pt;

-- EXPLAIN (COSTS OFF)
-- INSERT INTO result_tbl SELECT * FROM async_pt WHERE b === 505;
-- INSERT INTO result_tbl SELECT * FROM async_pt WHERE b === 505;

-- SELECT * FROM result_tbl ORDER BY a;
-- DELETE FROM result_tbl;

-- DROP FOREIGN TABLE async_p3;
-- DROP TABLE base_tbl3;

-- -- Check case where the partitioned table has local/remote partitions
-- CREATE TABLE async_p3 PARTITION OF async_pt FOR VALUES FROM (3000) TO (4000);
-- INSERT INTO async_p3 SELECT 3000 + i, i, to_char(i, 'FM0000') FROM generate_series(0, 999, 5) i;
-- ANALYZE async_pt;

-- EXPLAIN (COSTS OFF)
-- INSERT INTO result_tbl SELECT * FROM async_pt WHERE b === 505;
-- INSERT INTO result_tbl SELECT * FROM async_pt WHERE b === 505;

-- SELECT * FROM result_tbl ORDER BY a;
-- DELETE FROM result_tbl;

-- -- partitionwise joins
-- SET enable_partitionwise_join TO true;

-- CREATE TABLE join_tbl (a1 int, b1 int, c1 text, a2 int, b2 int, c2 text);

-- EXPLAIN (COSTS OFF)
-- INSERT INTO join_tbl SELECT * FROM async_pt t1, async_pt t2 WHERE t1.a = t2.a AND t1.b = t2.b AND t1.b % 100 = 0;
-- INSERT INTO join_tbl SELECT * FROM async_pt t1, async_pt t2 WHERE t1.a = t2.a AND t1.b = t2.b AND t1.b % 100 = 0;

-- SELECT * FROM join_tbl ORDER BY a1;
-- DELETE FROM join_tbl;

-- RESET enable_partitionwise_join;

-- -- Test rescan of an async Append node with do_exec_prune=false
-- SET enable_hashjoin TO false;

-- EXPLAIN (COSTS OFF)
-- INSERT INTO join_tbl SELECT * FROM async_p1 t1, async_pt t2 WHERE t1.a = t2.a AND t1.b = t2.b AND t1.b % 100 = 0;
-- INSERT INTO join_tbl SELECT * FROM async_p1 t1, async_pt t2 WHERE t1.a = t2.a AND t1.b = t2.b AND t1.b % 100 = 0;

-- SELECT * FROM join_tbl ORDER BY a1;
-- DELETE FROM join_tbl;

-- RESET enable_hashjoin;

-- -- Test interaction of async execution with plan-time partition pruning
-- EXPLAIN (COSTS OFF)
-- SELECT * FROM async_pt WHERE a < 3000;

-- EXPLAIN (COSTS OFF)
-- SELECT * FROM async_pt WHERE a < 2000;

-- -- Test interaction of async execution with run-time partition pruning
-- SET plan_cache_mode TO force_generic_plan;

-- PREPARE async_pt_query (int, int) AS
--   INSERT INTO result_tbl SELECT * FROM async_pt WHERE a < $1 AND b === $2;

-- EXPLAIN (COSTS OFF)
-- EXECUTE async_pt_query (3000, 505);
-- EXECUTE async_pt_query (3000, 505);

-- SELECT * FROM result_tbl ORDER BY a;
-- DELETE FROM result_tbl;

-- EXPLAIN (COSTS OFF)
-- EXECUTE async_pt_query (2000, 505);
-- EXECUTE async_pt_query (2000, 505);

-- SELECT * FROM result_tbl ORDER BY a;
-- DELETE FROM result_tbl;

-- RESET plan_cache_mode;

-- CREATE TABLE local_tbl(a int, b int, c text);
-- INSERT INTO local_tbl VALUES (1505, 505, 'foo'), (2505, 505, 'bar');
-- ANALYZE local_tbl;

-- CREATE INDEX base_tbl1_idx ON base_tbl1 (a);
-- CREATE INDEX base_tbl2_idx ON base_tbl2 (a);
-- CREATE INDEX async_p3_idx ON async_p3 (a);
-- ANALYZE base_tbl1;
-- ANALYZE base_tbl2;
-- ANALYZE async_p3;

-- ALTER FOREIGN TABLE async_p1 OPTIONS (use_remote_estimate 'true');
-- ALTER FOREIGN TABLE async_p2 OPTIONS (use_remote_estimate 'true');

-- EXPLAIN (COSTS OFF)
-- SELECT * FROM local_tbl, async_pt WHERE local_tbl.a = async_pt.a AND local_tbl.c = 'bar';
-- EXPLAIN (ANALYZE, COSTS OFF, SUMMARY OFF, TIMING OFF)
-- SELECT * FROM local_tbl, async_pt WHERE local_tbl.a = async_pt.a AND local_tbl.c = 'bar';
-- SELECT * FROM local_tbl, async_pt WHERE local_tbl.a = async_pt.a AND local_tbl.c = 'bar';

-- ALTER FOREIGN TABLE async_p1 OPTIONS (DROP use_remote_estimate);
-- ALTER FOREIGN TABLE async_p2 OPTIONS (DROP use_remote_estimate);

-- DROP TABLE local_tbl;
-- DROP INDEX base_tbl1_idx;
-- DROP INDEX base_tbl2_idx;
-- DROP INDEX async_p3_idx;

-- -- Test that pending requests are processed properly
-- SET enable_mergejoin TO false;
-- SET enable_hashjoin TO false;

-- EXPLAIN (COSTS OFF)
-- SELECT * FROM async_pt t1, async_p2 t2 WHERE t1.a = t2.a AND t1.b === 505;
-- SELECT * FROM async_pt t1, async_p2 t2 WHERE t1.a = t2.a AND t1.b === 505;

-- CREATE TABLE local_tbl (a int, b int, c text);
-- INSERT INTO local_tbl VALUES (1505, 505, 'foo');
-- ANALYZE local_tbl;

-- EXPLAIN (COSTS OFF)
-- SELECT * FROM local_tbl t1 LEFT JOIN (SELECT *, (SELECT count(*) FROM async_pt WHERE a < 3000) FROM async_pt WHERE a < 3000) t2 ON t1.a = t2.a;
-- EXPLAIN (ANALYZE, COSTS OFF, SUMMARY OFF, TIMING OFF)
-- SELECT * FROM local_tbl t1 LEFT JOIN (SELECT *, (SELECT count(*) FROM async_pt WHERE a < 3000) FROM async_pt WHERE a < 3000) t2 ON t1.a = t2.a;
-- SELECT * FROM local_tbl t1 LEFT JOIN (SELECT *, (SELECT count(*) FROM async_pt WHERE a < 3000) FROM async_pt WHERE a < 3000) t2 ON t1.a = t2.a;

-- EXPLAIN (COSTS OFF)
-- SELECT * FROM async_pt t1 WHERE t1.b === 505 LIMIT 1;
-- EXPLAIN (ANALYZE, COSTS OFF, SUMMARY OFF, TIMING OFF)
-- SELECT * FROM async_pt t1 WHERE t1.b === 505 LIMIT 1;
-- SELECT * FROM async_pt t1 WHERE t1.b === 505 LIMIT 1;

-- -- Check with foreign modify
-- CREATE TABLE base_tbl3 (a int, b int, c text);
-- CREATE FOREIGN TABLE remote_tbl (a int, b int, c text)
--   SERVER oracle_srv OPTIONS (table_name 'base_tbl3');
-- INSERT INTO remote_tbl VALUES (2505, 505, 'bar');

-- CREATE TABLE base_tbl4 (a int, b int, c text);
-- CREATE FOREIGN TABLE insert_tbl (a int, b int, c text)
--   SERVER oracle_srv OPTIONS (table_name 'base_tbl4');

-- EXPLAIN (COSTS OFF)
-- INSERT INTO insert_tbl (SELECT * FROM local_tbl UNION ALL SELECT * FROM remote_tbl);
-- INSERT INTO insert_tbl (SELECT * FROM local_tbl UNION ALL SELECT * FROM remote_tbl);

-- SELECT * FROM insert_tbl ORDER BY a;

-- -- Check with direct modify
-- EXPLAIN (COSTS OFF)
-- WITH t AS (UPDATE remote_tbl SET c = c || c RETURNING *)
-- INSERT INTO join_tbl SELECT * FROM async_pt LEFT JOIN t ON (async_pt.a = t.a AND async_pt.b = t.b) WHERE async_pt.b === 505;
-- WITH t AS (UPDATE remote_tbl SET c = c || c RETURNING *)
-- INSERT INTO join_tbl SELECT * FROM async_pt LEFT JOIN t ON (async_pt.a = t.a AND async_pt.b = t.b) WHERE async_pt.b === 505;

-- SELECT * FROM join_tbl ORDER BY a1;
-- DELETE FROM join_tbl;

-- DROP TABLE local_tbl;
-- DROP FOREIGN TABLE remote_tbl;
-- DROP FOREIGN TABLE insert_tbl;
-- DROP TABLE base_tbl3;
-- DROP TABLE base_tbl4;

-- RESET enable_mergejoin;
-- RESET enable_hashjoin;

-- -- Test that UPDATE/DELETE with inherited target works with async_capable enabled
-- EXPLAIN (COSTS OFF)
-- UPDATE async_pt SET c = c || c WHERE b = 0 RETURNING *;
-- UPDATE async_pt SET c = c || c WHERE b = 0 RETURNING *;
-- EXPLAIN (COSTS OFF)
-- DELETE FROM async_pt WHERE b = 0 RETURNING *;
-- DELETE FROM async_pt WHERE b = 0 RETURNING *;

-- -- Check EXPLAIN ANALYZE for a query that scans empty partitions asynchronously
-- DELETE FROM async_p1;
-- DELETE FROM async_p2;
-- DELETE FROM async_p3;

-- EXPLAIN (ANALYZE, COSTS OFF, SUMMARY OFF, TIMING OFF)
-- SELECT * FROM async_pt;

-- -- Clean up
-- DROP TABLE async_pt;
-- DROP TABLE base_tbl1;
-- DROP TABLE base_tbl2;
-- DROP TABLE result_tbl;
-- DROP TABLE join_tbl;

-- ALTER SERVER oracle_srv OPTIONS (DROP async_capable);
-- ALTER SERVER oracle_srv2 OPTIONS (DROP async_capable);

-- ===================================================================
-- test invalid server and foreign table options
-- oracle_fdw does not support this feature
-- ===================================================================
-- Invalid fdw_startup_cost option
-- CREATE SERVER inv_scst FOREIGN DATA WRAPPER oracle_fdw
-- 	OPTIONS(fdw_startup_cost '100$%$#$#');
-- Invalid fdw_tuple_cost option
-- CREATE SERVER inv_scst FOREIGN DATA WRAPPER oracle_fdw
-- 	OPTIONS(fdw_tuple_cost '100$%$#$#');
-- Invalid fetch_size option
-- CREATE FOREIGN TABLE inv_fsz (c1 int )
-- 	SERVER oracle_srv OPTIONS (fetch_size '100$%$#$#');
-- Invalid batch_size option
-- CREATE FOREIGN TABLE inv_bsz (c1 int )
-- 	SERVER oracle_srv OPTIONS (batch_size '100$%$#$#');

-- clean up
--Testcase 916:
DROP EXTENSION oracle_fdw CASCADE;
