
SET client_min_messages = WARNING;

CREATE EXTENSION oracle_fdw;

-- TWO_TASK or ORACLE_HOME and ORACLE_SID must be set in the server's environment for this to work
CREATE SERVER oracle FOREIGN DATA WRAPPER oracle_fdw OPTIONS (dbserver '', isolation_level 'read_committed', nchar 'true');

CREATE USER MAPPING FOR CURRENT_USER SERVER oracle OPTIONS (user 'SCOTT', password 'tiger');

-- drop the Oracle tables if they exist
DO
$$BEGIN
   SELECT oracle_execute('oracle', 'DROP TABLE scott.extra1 PURGE');
EXCEPTION
   WHEN OTHERS THEN
      NULL;
END;$$;


SELECT oracle_execute(
          'oracle',
          E'CREATE TABLE scott.extra1 (\n'
          '   id  NUMBER(5)\n'
          '      CONSTRAINT extra1_pkey PRIMARY KEY,\n'
          '   c   CHAR(10 CHAR),\n'
          '   nc  NCHAR(10),\n'
          '   vc  VARCHAR2(10 CHAR),\n'
          '   nvc NVARCHAR2(10),\n'
          '   lc  CLOB,\n'
          '   r   RAW(10),\n'
          '   u   RAW(16),\n'
          '   lb  BLOB,\n'
          '   lr  LONG RAW,\n'
          '   b   NUMBER(1),\n'
          '   num NUMBER(7,5),\n'
          '   fl  BINARY_FLOAT,\n'
          '   db  BINARY_DOUBLE,\n'
          '   d   DATE,\n'
          '   ts  TIMESTAMP WITH TIME ZONE,\n'
          '   ids INTERVAL DAY TO SECOND,\n'
          '   iym INTERVAL YEAR TO MONTH\n'
          ') SEGMENT CREATION IMMEDIATE'
       );


-- create the foreign tables
CREATE FOREIGN TABLE extra1 (
   id  integer OPTIONS (key 'yes') NOT NULL,
   c   character(10),
   nc  character(10),
   vc  character varying(10),
   nvc character varying(10),
   lc  text,
   r   bytea,
   u   uuid,
   lb  bytea,
   lr  bytea,
   b   boolean,
   num numeric(7,5),
   fl  float,
   db  double precision,
   d   date,
   ts  timestamp with time zone,
   ids interval,
   iym interval
) SERVER oracle OPTIONS (table 'EXTRA1');

CREATE FOREIGN TABLE extra2 (
   id  integer OPTIONS (key 'yes') NOT NULL,
   c   character(10),
   nc  character(10),
   vc  character varying(10),
   nvc character varying(10)
) SERVER oracle OPTIONS (table 'EXTRA1');

--
-- Test INSERT ... RETURNING with Large Object data (LOB).
--
INSERT INTO extra1 (id, lc) VALUES (11111, 'aaaaaaaa') RETURNING id, lc;
 
--
-- Test INSERT ... RETURNING whole row.
--
INSERT INTO extra2 (id, vc) VALUES (33333, 'cc') RETURNING extra2;

--
-- Test that trigger on remote table works as expected
--
INSERT INTO extra2 (id, vc) VALUES (22222, 'bb') RETURNING *;

CREATE OR REPLACE FUNCTION F_BRTRIG() RETURNS trigger AS $$
BEGIN
    NEW.vc = NEW.vc || '_brtrig';
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;
CREATE TRIGGER extra2_br_insert BEFORE INSERT OR UPDATE
    ON extra2 FOR EACH ROW EXECUTE PROCEDURE F_BRTRIG();

UPDATE extra2 SET c = 'aa' WHERE id = 22222 RETURNING *;


--
-- Test data types which are not allowed in ORDER BY and GROUP BY clauses
--

-- Test ORDER BY and GROUP BY with TEXT data type
EXPLAIN (COSTS OFF)
SELECT id, lc FROM extra1 GROUP BY id, lc ORDER BY lc;
SELECT id, lc FROM extra1 GROUP BY id, lc ORDER BY lc;

-- Test ORDER BY and GROUP BY with BYTE data type
EXPLAIN (COSTS OFF)
SELECT id, r FROM extra1 GROUP BY id, r ORDER BY r;
SELECT id, r FROM extra1 GROUP BY id, r ORDER BY r;

--
-- Test string comparison
--
INSERT INTO extra1 (id, lc) VALUES (11112, 'bbbbbbbb') RETURNING id, lc;
INSERT INTO extra1 (id, lc) VALUES (11113, 'CCCCCCCC') RETURNING id, lc;

-- Do not pushdown string comparison with CLOB
EXPLAIN (COSTS OFF)
SELECT lc FROM extra1 WHERE lc = 'aaaaaaaa';
SELECT lc FROM extra1 WHERE lc = 'aaaaaaaa';

EXPLAIN (COSTS OFF)
SELECT lc FROM extra1 WHERE upper(lc) = 'CCCCCCCC';
SELECT lc FROM extra1 WHERE upper(lc) = 'CCCCCCCC';

EXPLAIN (COSTS OFF)
SELECT lc FROM extra1 WHERE lc != 'aaaaaaaa';
SELECT lc FROM extra1 WHERE lc != 'aaaaaaaa';

EXPLAIN (COSTS OFF)
SELECT lc FROM extra1 WHERE upper(lc) != 'CCCCCCCC';
SELECT lc FROM extra1 WHERE upper(lc) != 'CCCCCCCC';

-- Pushdown number comparison with CLOB
EXPLAIN (COSTS OFF)
SELECT lc FROM extra1 WHERE length(lc) > 1;
SELECT lc FROM extra1 WHERE length(lc) > 1;

-- Pushdown string comparison with VARCHAR
EXPLAIN (COSTS OFF)
SELECT vc FROM extra1 WHERE vc = 'cc';
SELECT vc FROM extra1 WHERE vc = 'cc';


-- Test aggregation function pushdown (variance)
EXPLAIN (COSTS OFF)
SELECT variance(id) FROM extra1;
SELECT variance(id) FROM extra1;

--
-- Init data for testing aggregate functions
--
DO
$$BEGIN
   SELECT oracle_execute('oracle', 'DROP TABLE scott.aggtest PURGE');
EXCEPTION
   WHEN OTHERS THEN
      NULL;
END;$$;

SELECT oracle_execute(
          'oracle',
          E'CREATE TABLE scott.aggtest (\n'
          '   a        NUMBER(5) PRIMARY KEY,\n'
          '   b        BINARY_FLOAT\n'
          ') SEGMENT CREATION IMMEDIATE'
       );

CREATE FOREIGN TABLE aggtest (
  a       int2,
  b       float4
) SERVER oracle OPTIONS (table 'AGGTEST');

INSERT INTO aggtest (a, b) VALUES (56, 7.8), (100, 99.097), (0, 0.09561), (42, 324.78);

--
-- Test built-in aggregate functions
--

-- select stddev_pop (builtin function, explain)
EXPLAIN (COSTS OFF)
SELECT stddev_pop(b) FROM aggtest;

-- select stddev_pop (builtin function, result)
SELECT stddev_pop(b) FROM aggtest;

-- select stddev_samp (builtin function, explain)
EXPLAIN (COSTS OFF)
SELECT stddev_samp(b) FROM aggtest;

-- select stddev_samp (builtin function, result)
SELECT stddev_samp(b) FROM aggtest;

-- select var_pop (builtin function, explain)
EXPLAIN (COSTS OFF)
SELECT var_pop(b) FROM aggtest;

-- select var_pop (builtin function, result)
SELECT var_pop(b) FROM aggtest;

-- select var_samp (builtin function, explain)
EXPLAIN (COSTS OFF)
SELECT var_samp(b) FROM aggtest;

-- select var_samp (builtin function, result)
SELECT var_samp(b) FROM aggtest;

-- select stddev_pop (not pushdown builtin function, explain)
EXPLAIN (COSTS OFF)
SELECT stddev_pop(b::numeric) FROM aggtest;

-- select stddev_pop (not pushdown builtin function, result)
SELECT stddev_pop(b::numeric) FROM aggtest;

-- select stddev_samp (not pushdown builtin function, explain)
EXPLAIN (COSTS OFF)
SELECT stddev_samp(b::numeric) FROM aggtest;

-- select stddev_samp (not pushdown builtin function, result)
SELECT stddev_samp(b::numeric) FROM aggtest;

-- select var_pop (not pushdown builtin function, explain)
EXPLAIN (COSTS OFF)
SELECT var_pop(b::numeric) FROM aggtest;

-- select var_pop (not pushdown builtin function, result)
SELECT var_pop(b::numeric) FROM aggtest;

-- select var_samp (not pushdown builtin function, explain)
EXPLAIN (COSTS OFF)
SELECT var_samp(b::numeric) FROM aggtest;

-- select var_samp (not pushdown builtin function, result)
SELECT var_samp(b::numeric) FROM aggtest;

-- select covar_pop (builtin function, explain)
EXPLAIN (COSTS OFF)
SELECT covar_pop(b, a), covar_samp(b, a) FROM aggtest;

-- select covar_pop (builtin function, result)
SELECT covar_pop(b, a), covar_samp(b, a) FROM aggtest;

-- select corr (builtin function, explain)
EXPLAIN (COSTS OFF)
SELECT corr(b, a) FROM aggtest;

-- select corr (builtin function, result)
SELECT corr(b, a) FROM aggtest;

-- select percentile_cont (builtin function, explain)
EXPLAIN (COSTS OFF)
SELECT (percentile_cont(0.5) WITHIN GROUP (ORDER BY b)) FROM aggtest;

-- select percentile_cont (builtin function, result)
select (percentile_cont(0.5) WITHIN GROUP (ORDER BY b)) FROM aggtest;

-- select percentile_cont, sum (builtin function, explain)
EXPLAIN (COSTS OFF)
SELECT (percentile_cont(0.5) WITHIN GROUP (ORDER BY b)), sum(b) FROM aggtest;

-- select percentile_cont, sum (builtin function, result)
SELECT (percentile_cont(0.5) WITHIN GROUP (ORDER BY b)), sum(b) FROM aggtest;

-- select percentile_disc (builtin function, explain)
EXPLAIN (COSTS OFF)
SELECT (percentile_disc(0.5) WITHIN GROUP (ORDER BY b)) FROM aggtest;

-- select percentile_disc (builtin function, result)
SELECT (percentile_disc(0.5) WITHIN GROUP (ORDER BY b)) FROM aggtest;

-- select percent_rank (builtin function, explain)
EXPLAIN (COSTS OFF)
SELECT (percent_rank(3) WITHIN GROUP (ORDER BY a)) FROM aggtest;

-- select percent_rank (builtin function, result)
SELECT (percent_rank(3) WITHIN GROUP (ORDER BY a)) FROM aggtest;

-- select dense_rank (builtin function, explain)
EXPLAIN (COSTS OFF)
SELECT (dense_rank(3) WITHIN GROUP (ORDER BY a)) FROM aggtest;

-- select dense_rank (builtin function, result)
SELECT (dense_rank(3) WITHIN GROUP (ORDER BY a)) FROM aggtest;

-- select dense_rank (builtin function, explain)
EXPLAIN (COSTS OFF)
SELECT (cume_dist(3) WITHIN GROUP (ORDER BY a)) FROM aggtest;

-- select dense_rank (builtin function, result)
SELECT (cume_dist(3) WITHIN GROUP (ORDER BY a)) FROM aggtest;

--
-- Test unique aggregate functions
--

-- select approx_count_distinct (unique function, explain)
EXPLAIN (COSTS OFF)
SELECT approx_count_distinct(b) FROM aggtest;

-- select approx_count_distinct (unique function, result)
SELECT approx_count_distinct(b) FROM aggtest;

-- Insert duplicate value of column b
INSERT INTO aggtest (a, b) VALUES (57, 7.8);

-- select approx_count_distinct (unique function, result)
SELECT approx_count_distinct(b) FROM aggtest;

-- clean up
DROP EXTENSION oracle_fdw CASCADE;
