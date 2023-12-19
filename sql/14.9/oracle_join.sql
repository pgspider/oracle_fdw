\pset border 1
\pset linestyle ascii
\set VERBOSITY terse
--Testcase 1:
SET client_min_messages = INFO;

--Testcase 2:
CREATE EXTENSION oracle_fdw;

-- TWO_TASK or ORACLE_HOME and ORACLE_SID must be set in the server's environment for this to work
--Testcase 3:
CREATE SERVER oracle FOREIGN DATA WRAPPER oracle_fdw OPTIONS (dbserver '', isolation_level 'read_committed', nchar 'true');
--Testcase 4:
CREATE USER MAPPING FOR CURRENT_USER SERVER oracle OPTIONS (user 'SCOTT', password 'tiger');

-- create the foreign tables
--Testcase 5:
CREATE FOREIGN TABLE typetest1 (
   id  integer OPTIONS (key 'yes') NOT NULL,
   q   double precision,
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
) SERVER oracle OPTIONS (table 'TYPETEST1');
--Testcase 6:
ALTER FOREIGN TABLE typetest1 DROP q;

-- a table that is missing some fields
--Testcase 7:
CREATE FOREIGN TABLE shorty (
   id  integer OPTIONS (key 'yes') NOT NULL,
   c   character(10)
) SERVER oracle OPTIONS (table 'TYPETEST1');

-- a table that has some extra fields
--Testcase 8:
CREATE FOREIGN TABLE longy (
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
   iym interval,
   x   integer
) SERVER oracle OPTIONS (table 'TYPETEST1');

/* analyze table for reliable output */
ANALYZE typetest1;
ANALYZE longy;
ANALYZE shorty;

/* default setting sometimes leads to merge joins */
--Testcase 9:
SET enable_mergejoin = off;

/*
 * Cases that should be pushed down.
 */
-- inner join two tables
--Testcase 10:
EXPLAIN (COSTS off)
SELECT t1.id, t2.id FROM typetest1  t1, typetest1  t2 WHERE t1.c = t2.c ORDER BY t1.id, t2.id;
--Testcase 11:
SELECT t1.id, t2.id FROM typetest1  t1, typetest1  t2 WHERE t1.c = t2.c ORDER BY t1.id, t2.id;
--Testcase 12:
EXPLAIN (COSTS off)
SELECT length(t1.lb), length(t2.lc) FROM typetest1  t1 JOIN typetest1  t2 ON (t1.id + t2.id = 2) ORDER BY t1.id, t2.id;
--Testcase 13:
SELECT length(t1.lb), length(t2.lc) FROM typetest1  t1 JOIN typetest1  t2 ON (t1.id + t2.id = 2) ORDER BY t1.id, t2.id;
-- inner join two tables with ORDER BY clause, but ORDER BY does not get pushed down
--Testcase 14:
EXPLAIN (COSTS off)
SELECT t1.id, t2.id FROM typetest1  t1 JOIN typetest1  t2 USING (ts, num) ORDER BY t1.id, t2.id;
--Testcase 15:
SELECT t1.id, t2.id FROM typetest1  t1 JOIN typetest1  t2 USING (ts, num) ORDER BY t1.id, t2.id;
-- natural join two tables
--Testcase 16:
EXPLAIN (COSTS off)
SELECT id FROM typetest1  NATURAL JOIN shorty  ORDER BY id;
--Testcase 17:
SELECT id FROM typetest1  NATURAL JOIN shorty  ORDER BY id;
-- table with column that does not exist in Oracle (should become NULL)
--Testcase 18:
EXPLAIN (COSTS off)
SELECT t1.id, t2.x FROM typetest1  t1 JOIN longy t2  ON t1.c = t2.c ORDER BY t1.id, t2.x;
--Testcase 19:
SELECT t1.id, t2.x FROM typetest1  t1 JOIN longy t2  ON t1.c = t2.c ORDER BY t1.id, t2.x;
-- left outer join two tables
--Testcase 20:
EXPLAIN (COSTS off)
SELECT t1.id, t2.id FROM typetest1  t1 LEFT  JOIN typetest1  t2 ON t1.d = t2.d ORDER BY t1.id, t2.id;
--Testcase 21:
SELECT t1.id, t2.id FROM typetest1  t1 LEFT  JOIN typetest1  t2 ON t1.d = t2.d ORDER BY t1.id, t2.id;
-- right outer join two tables
--Testcase 22:
EXPLAIN (COSTS off)
SELECT t1.id, t2.id FROM typetest1  t1 RIGHT JOIN typetest1  t2 ON t1.d = t2.d ORDER BY t1.id, t2.id;
--Testcase 23:
SELECT t1.id, t2.id FROM typetest1  t1 RIGHT JOIN typetest1  t2 ON t1.d = t2.d ORDER BY t1.id, t2.id;
-- full outer join two tables
--Testcase 24:
EXPLAIN (COSTS off)
SELECT t1.id, t2.id FROM typetest1  t1 FULL  JOIN typetest1  t2 ON t1.d = t2.d ORDER BY t1.id, t2.id;
--Testcase 25:
SELECT t1.id, t2.id FROM typetest1  t1 FULL  JOIN typetest1  t2 ON t1.d = t2.d ORDER BY t1.id, t2.id;
-- joins with filter conditions
---- inner join with WHERE clause
--Testcase 26:
EXPLAIN (COSTS off)
SELECT t1.id, t2.id FROM typetest1  t1 INNER JOIN typetest1  t2 ON t1.d = t2.d WHERE t1.id > 1 ORDER BY t1.id, t2.id;
--Testcase 27:
SELECT t1.id, t2.id FROM typetest1  t1 INNER JOIN typetest1  t2 ON t1.d = t2.d WHERE t1.id > 1 ORDER BY t1.id, t2.id;
---- left outer join with WHERE clause
--Testcase 28:
EXPLAIN (COSTS off)
SELECT t1.id, t2.id FROM typetest1  t1 LEFT  JOIN typetest1  t2 ON t1.d = t2.d WHERE t1.id > 1 ORDER BY t1.id, t2.id;
--Testcase 29:
SELECT t1.id, t2.id FROM typetest1  t1 LEFT  JOIN typetest1  t2 ON t1.d = t2.d WHERE t1.id > 1 ORDER BY t1.id, t2.id;
---- right outer join with WHERE clause
--Testcase 30:
EXPLAIN (COSTS off)
SELECT t1.id, t2.id FROM typetest1  t1 RIGHT JOIN typetest1  t2 ON t1.d = t2.d WHERE t1.id > 1 ORDER BY t1.id, t2.id;
--Testcase 31:
SELECT t1.id, t2.id FROM typetest1  t1 RIGHT JOIN typetest1  t2 ON t1.d = t2.d WHERE t1.id > 1 ORDER BY t1.id, t2.id;
---- full outer join with WHERE clause
--Testcase 32:
EXPLAIN (COSTS off)
SELECT t1.id, t2.id FROM typetest1  t1 FULL  JOIN typetest1  t2 ON t1.d = t2.d WHERE t1.id > 1 ORDER BY t1.id, t2.id;
--Testcase 33:
SELECT t1.id, t2.id FROM typetest1  t1 FULL  JOIN typetest1  t2 ON t1.d = t2.d WHERE t1.id > 1 ORDER BY t1.id, t2.id;

/*
 * Cases that should not be pushed down.
 */
-- join expression cannot be pushed down
--Testcase 34:
EXPLAIN (COSTS off)
SELECT t1.id, t2.id FROM typetest1  t1, typetest1  t2 WHERE t1.lc = t2.lc ORDER BY t1.id, t2.id;
--Testcase 35:
SELECT t1.id, t2.id FROM typetest1  t1, typetest1  t2 WHERE t1.lc = t2.lc ORDER BY t1.id, t2.id;
-- only one join condition cannot be pushed down
--Testcase 36:
EXPLAIN (COSTS off)
SELECT t1.id, t2.id FROM typetest1  t1 JOIN typetest1  t2 ON t1.vc = t2.vc AND t1.lb = t2.lb ORDER BY t1.id, t2.id;
--Testcase 37:
SELECT t1.id, t2.id FROM typetest1  t1 JOIN typetest1  t2 ON t1.vc = t2.vc AND t1.lb = t2.lb ORDER BY t1.id, t2.id;
-- condition on one table needs to be evaluated locally
--Testcase 38:
EXPLAIN (COSTS off)
SELECT max(t1.id), min(t2.id) FROM typetest1  t1 JOIN typetest1  t2 ON t1.fl = t2.fl WHERE t1.vc || 'x' = 'shortx' ORDER BY 1, 2;
--Testcase 39:
SELECT max(t1.id), min(t2.id) FROM typetest1  t1 JOIN typetest1  t2 ON t1.fl = t2.fl WHERE t1.vc || 'x' = 'shortx' ORDER BY 1, 2;
--Testcase 40:
EXPLAIN (COSTS off)
SELECT t1.c, t2.nc FROM typetest1  t1 JOIN (SELECT * FROM typetest1)  t2 ON (t1.id = t2.id AND t1.c >= t2.c) ORDER BY t1.id, t2.nc;
--Testcase 41:
SELECT t1.c, t2.nc FROM typetest1  t1 JOIN (SELECT * FROM typetest1)  t2 ON (t1.id = t2.id AND t1.c >= t2.c) ORDER BY t1.id, t2.nc;
--Testcase 42:
EXPLAIN (COSTS off)
SELECT t1.c, t2.nc FROM typetest1  t1 LEFT JOIN (SELECT * FROM typetest1)  t2 ON (t1.id = t2.id AND t1.c >= t2.c) ORDER BY t1.id, t2.nc;
--Testcase 43:
SELECT t1.c, t2.nc FROM typetest1  t1 LEFT JOIN (SELECT * FROM typetest1)  t2 ON (t1.id = t2.id AND t1.c >= t2.c) ORDER BY t1.id, t2.nc;
-- subquery with where clause cannnot be pushed down in full outer join query
--Testcase 44:
EXPLAIN (COSTS off)
SELECT t1.c, t2.nc FROM typetest1  t1 FULL JOIN (SELECT * FROM typetest1  WHERE id > 1) t2 USING (id) ORDER BY t1.id, t2.nc;
--Testcase 45:
SELECT t1.c, t2.nc FROM typetest1  t1 FULL JOIN (SELECT * FROM typetest1  WHERE id > 1) t2 USING (id) ORDER BY t1.id, t2.nc;
-- left outer join with placeholder, not pushed down
--Testcase 46:
EXPLAIN (COSTS off)
SELECT t1.id, sq1.x, sq1.y
FROM typetest1  t1 LEFT OUTER JOIN (SELECT id AS x, 99 AS y FROM typetest1  t2 WHERE id > 1) sq1 ON t1.id = sq1.x ORDER BY t1.id, sq1.x;
--Testcase 47:
SELECT t1.id, sq1.x, sq1.y
FROM typetest1  t1 LEFT OUTER JOIN (SELECT id AS x, 99 AS y FROM typetest1  t2 WHERE id > 1) sq1 ON t1.id = sq1.x ORDER BY t1.id, sq1.x;
-- inner join with placeholder, not pushed down
--Testcase 48:
EXPLAIN (COSTS off)
SELECT subq2.c3
FROM typetest1
RIGHT JOIN (SELECT c AS c1 FROM typetest1)  AS subq1 ON TRUE
LEFT JOIN  (SELECT ref1.nc AS c2, 10 AS c3 FROM typetest1  AS ref1
            INNER JOIN typetest1  AS ref2 ON ref1.fl = ref2.fl) AS subq2
ON subq1.c1 = subq2.c2 ORDER BY subq2.c3;
--Testcase 49:
SELECT subq2.c3
FROM typetest1
RIGHT JOIN (SELECT c AS c1 FROM typetest1)  AS subq1 ON TRUE
LEFT JOIN  (SELECT ref1.nc AS c2, 10 AS c3 FROM typetest1  AS ref1
            INNER JOIN typetest1  AS ref2 ON ref1.fl = ref2.fl) AS subq2
ON subq1.c1 = subq2.c2 ORDER BY subq2.c3;
-- inner rel is false, not pushed down
--Testcase 50:
EXPLAIN (COSTS off)
SELECT 1 FROM (SELECT 1 FROM typetest1  WHERE false) AS subq1 RIGHT JOIN typetest1  AS ref1 ON NULL ORDER BY ref1.id;
--Testcase 51:
SELECT 1 FROM (SELECT 1 FROM typetest1  WHERE false) AS subq1 RIGHT JOIN typetest1  AS ref1 ON NULL ORDER BY ref1.id;
-- semi-join, not pushed down
--Testcase 52:
EXPLAIN (COSTS off)
SELECT t1.id FROM typetest1  t1 WHERE EXISTS (SELECT 1 FROM typetest1  t2 WHERE t1.d = t2.d) ORDER BY t1.id;
--Testcase 53:
SELECT t1.id FROM typetest1  t1 WHERE EXISTS (SELECT 1 FROM typetest1  t2 WHERE t1.d = t2.d) ORDER BY t1.id;
-- anti-join, not pushed down
--Testcase 54:
EXPLAIN (COSTS off)
SELECT t1.id FROM typetest1  t1 WHERE NOT EXISTS (SELECT 1 FROM typetest1  t2 WHERE t1.d = t2.d) ORDER BY t1.id;
--Testcase 55:
SELECT t1.id FROM typetest1  t1 WHERE NOT EXISTS (SELECT 1 FROM typetest1  t2 WHERE t1.d = t2.d) ORDER BY t1.id;
-- cross join, not pushed down
--Testcase 56:
EXPLAIN (COSTS off)
SELECT t1.id, t2.id FROM typetest1  t1 CROSS JOIN typetest1  t2 ORDER BY t1.id, t2.id;
--Testcase 57:
SELECT t1.id, t2.id FROM typetest1  t1 CROSS JOIN typetest1  t2 ORDER BY t1.id, t2.id;
--Testcase 58:
EXPLAIN (COSTS off)
SELECT t1.id, t2.id FROM typetest1  t1 INNER JOIN typetest1  t2 ON true ORDER BY t1.id, t2.id;
--Testcase 59:
SELECT t1.id, t2.id FROM typetest1  t1 INNER JOIN typetest1  t2 ON true ORDER BY t1.id, t2.id;
--Testcase 60:
EXPLAIN (COSTS off)
SELECT t1.id, t2.id FROM typetest1  t1 LEFT  JOIN typetest1  t2 ON true ORDER BY t1.id, t2.id;
--Testcase 61:
SELECT t1.id, t2.id FROM typetest1  t1 LEFT  JOIN typetest1  t2 ON true ORDER BY t1.id, t2.id;
--Testcase 62:
EXPLAIN (COSTS off)
SELECT t1.id, t2.id FROM typetest1  t1 RIGHT JOIN typetest1  t2 ON true ORDER BY t1.id, t2.id;
--Testcase 63:
SELECT t1.id, t2.id FROM typetest1  t1 RIGHT JOIN typetest1  t2 ON true ORDER BY t1.id, t2.id;
--Testcase 64:
EXPLAIN (COSTS off)
SELECT t1.id, t2.id FROM typetest1  t1 FULL  JOIN typetest1  t2 ON true ORDER BY t1.id, t2.id;
--Testcase 65:
SELECT t1.id, t2.id FROM typetest1  t1 FULL  JOIN typetest1  t2 ON true ORDER BY t1.id, t2.id;
--Testcase 66:
EXPLAIN (COSTS off)
SELECT t1.id, t2.id FROM typetest1  t1 CROSS JOIN (SELECT * FROM typetest1  WHERE vc = 'short') t2 ORDER BY t1.id, t2.id;
--Testcase 67:
SELECT t1.id, t2.id FROM typetest1  t1 CROSS JOIN (SELECT * FROM typetest1  WHERE vc = 'short') t2 ORDER BY t1.id, t2.id;
--Testcase 68:
EXPLAIN (COSTS off)
SELECT t1.id, t2.id FROM typetest1  t1 INNER JOIN (SELECT * FROM typetest1  WHERE vc = 'short') t2 ON true ORDER BY t1.id, t2.id;
--Testcase 69:
SELECT t1.id, t2.id FROM typetest1  t1 INNER JOIN (SELECT * FROM typetest1  WHERE vc = 'short') t2 ON true ORDER BY t1.id, t2.id;
--Testcase 70:
EXPLAIN (COSTS off)
SELECT t1.id, t2.id FROM typetest1  t1 LEFT  JOIN (SELECT * FROM typetest1  WHERE vc = 'short') t2 ON true ORDER BY t1.id, t2.id;
--Testcase 71:
SELECT t1.id, t2.id FROM typetest1  t1 LEFT  JOIN (SELECT * FROM typetest1  WHERE vc = 'short') t2 ON true ORDER BY t1.id, t2.id;
--Testcase 72:
EXPLAIN (COSTS off)
SELECT t1.id, t2.id FROM typetest1  t1 RIGHT JOIN (SELECT * FROM typetest1  WHERE vc = 'short') t2 ON true ORDER BY t1.id, t2.id;
--Testcase 73:
SELECT t1.id, t2.id FROM typetest1  t1 RIGHT JOIN (SELECT * FROM typetest1  WHERE vc = 'short') t2 ON true ORDER BY t1.id, t2.id;
--Testcase 74:
EXPLAIN (COSTS off)
SELECT t1.id, t2.id FROM typetest1  t1 FULL  JOIN (SELECT * FROM typetest1  WHERE vc = 'short') t2 ON true ORDER BY t1.id, t2.id;
--Testcase 75:
SELECT t1.id, t2.id FROM typetest1  t1 FULL  JOIN (SELECT * FROM typetest1  WHERE vc = 'short') t2 ON true ORDER BY t1.id, t2.id;
-- update statement, not pushed down
--Testcase 76:
EXPLAIN (COSTS off) UPDATE typetest1 t1 SET c = NULL FROM typetest1 t2 WHERE t1.vc = t2.vc AND t2.num = 3.14159;
-- join with FOR UPDATE, not pushed down
--Testcase 77:
EXPLAIN (COSTS off) SELECT t1.id FROM typetest1 t1, typetest1 t2 WHERE t1.id = t2.id FOR UPDATE;
-- join in CTE
--Testcase 78:
WITH t (t1_id, t2_id) AS (SELECT t1.id, t2.id FROM typetest1  t1 JOIN typetest1  t2 ON t1.d = t2.d) SELECT t1_id, t2_id FROM t ORDER BY t1_id, t2_id;
-- whole-row and system columns, not pushed down
--Testcase 79:
EXPLAIN (COSTS off)
SELECT t1, t1.ctid FROM shorty t1 INNER JOIN longy t2 ON t1.id = t2.id ORDER BY t1.id;
--Testcase 80:
SELECT t1, t1.ctid FROM shorty t1 INNER JOIN longy t2 ON t1.id = t2.id ORDER BY t1.id;
--Testcase 81:
EXPLAIN (COSTS off)
SELECT t1, t1.ctid FROM shorty t1 LEFT  JOIN longy t2 ON t1.id = t2.id ORDER BY t1.id;
--Testcase 82:
SELECT t1, t1.ctid FROM shorty t1 LEFT  JOIN longy t2 ON t1.id = t2.id ORDER BY t1.id;
--Testcase 83:
EXPLAIN (COSTS off)
SELECT t1, t1.ctid FROM shorty t1 RIGHT JOIN longy t2 ON t1.id = t2.id ORDER BY t1.id;
--Testcase 84:
SELECT t1, t1.ctid FROM shorty t1 RIGHT JOIN longy t2 ON t1.id = t2.id ORDER BY t1.id;
--Testcase 85:
EXPLAIN (COSTS off)
SELECT t1, t1.ctid FROM shorty t1 FULL  JOIN longy t2 ON t1.id = t2.id ORDER BY t1.id;
--Testcase 86:
SELECT t1, t1.ctid FROM shorty t1 FULL  JOIN longy t2 ON t1.id = t2.id ORDER BY t1.id;
--Testcase 87:
EXPLAIN (COSTS off)
SELECT t1, t1.ctid FROM shorty t1 CROSS JOIN longy t2 ORDER BY t1.id;
--Testcase 88:
SELECT t1, t1.ctid FROM shorty t1 CROSS JOIN longy t2 ORDER BY t1.id;
-- only part of a three-way join will be pushed down
---- inner join three tables
--Testcase 89:
EXPLAIN (COSTS off)
SELECT t1.id, t3.id FROM typetest1  t1 JOIN typetest1  t2 USING (nvc) JOIN typetest1  t3 ON t2.db = t3.db ORDER BY t1.id, t3.id;
--Testcase 90:
SELECT t1.id, t3.id FROM typetest1  t1 JOIN typetest1  t2 USING (nvc) JOIN typetest1  t3 ON t2.db = t3.db ORDER BY t1.id, t3.id;
--Testcase 91:
EXPLAIN (COSTS off)
SELECT t1.id, t2.id, t3.id FROM typetest1  t1 INNER JOIN typetest1  t2 ON t1.d = t2.d INNER JOIN typetest1  t3 ON t2.d = t3.d ORDER BY t1.id, t2.id;
--Testcase 92:
SELECT t1.id, t2.id, t3.id FROM typetest1  t1 INNER JOIN typetest1  t2 ON t1.d = t2.d INNER JOIN typetest1  t3 ON t2.d = t3.d ORDER BY t1.id, t2.id;
---- inner outer join + left outer join
--Testcase 93:
EXPLAIN (COSTS off)
SELECT t1.id, t2.id, t3.id FROM typetest1  t1 INNER JOIN typetest1  t2 ON t1.d = t2.d LEFT  JOIN typetest1  t3 ON t2.d = t3.d ORDER BY t1.id, t2.id;
--Testcase 94:
SELECT t1.id, t2.id, t3.id FROM typetest1  t1 INNER JOIN typetest1  t2 ON t1.d = t2.d LEFT  JOIN typetest1  t3 ON t2.d = t3.d ORDER BY t1.id, t2.id;
---- inner outer join + right outer join
--Testcase 95:
EXPLAIN (COSTS off)
SELECT t1.id, t2.id, t3.id FROM typetest1  t1 INNER JOIN typetest1  t2 ON t1.d = t2.d RIGHT JOIN typetest1  t3 ON t2.d = t3.d ORDER BY t1.id, t2.id;
--Testcase 96:
SELECT t1.id, t2.id, t3.id FROM typetest1  t1 INNER JOIN typetest1  t2 ON t1.d = t2.d RIGHT JOIN typetest1  t3 ON t2.d = t3.d ORDER BY t1.id, t2.id;
---- inner outer join + full outer join
--Testcase 97:
EXPLAIN (COSTS off)
SELECT t1.id, t2.id, t3.id FROM typetest1  t1 INNER JOIN typetest1  t2 ON t1.d = t2.d FULL  JOIN typetest1  t3 ON t2.d = t3.d ORDER BY t1.id, t2.id;
--Testcase 98:
SELECT t1.id, t2.id, t3.id FROM typetest1  t1 INNER JOIN typetest1  t2 ON t1.d = t2.d FULL  JOIN typetest1  t3 ON t2.d = t3.d ORDER BY t1.id, t2.id;
---- left outer join three tables
--Testcase 99:
EXPLAIN (COSTS off)
SELECT t1.id, t2.id, t3.id FROM typetest1  t1 LEFT  JOIN typetest1  t2 ON t1.d = t2.d LEFT  JOIN typetest1  t3 ON t2.d = t3.d ORDER BY t1.id, t2.id;
--Testcase 100:
SELECT t1.id, t2.id, t3.id FROM typetest1  t1 LEFT  JOIN typetest1  t2 ON t1.d = t2.d LEFT  JOIN typetest1  t3 ON t2.d = t3.d ORDER BY t1.id, t2.id;
---- left outer join + inner outer join
--Testcase 101:
EXPLAIN (COSTS off)
SELECT t1.id, t2.id, t3.id FROM typetest1  t1 LEFT  JOIN typetest1  t2 ON t1.d = t2.d INNER JOIN typetest1  t3 ON t2.d = t3.d ORDER BY t1.id, t2.id;
--Testcase 102:
SELECT t1.id, t2.id, t3.id FROM typetest1  t1 LEFT  JOIN typetest1  t2 ON t1.d = t2.d INNER JOIN typetest1  t3 ON t2.d = t3.d ORDER BY t1.id, t2.id;
---- left outer join + right outer join
--Testcase 103:
EXPLAIN (COSTS off)
SELECT t1.id, t2.id, t3.id FROM typetest1  t1 LEFT  JOIN typetest1  t2 ON t1.d = t2.d RIGHT JOIN typetest1  t3 ON t2.d = t3.d ORDER BY t1.id, t2.id;
--Testcase 104:
SELECT t1.id, t2.id, t3.id FROM typetest1  t1 LEFT  JOIN typetest1  t2 ON t1.d = t2.d RIGHT JOIN typetest1  t3 ON t2.d = t3.d ORDER BY t1.id, t2.id;
---- left outer join + full outer join
--Testcase 105:
EXPLAIN (COSTS off)
SELECT t1.id, t2.id, t3.id FROM typetest1  t1 LEFT  JOIN typetest1  t2 ON t1.d = t2.d FULL  JOIN typetest1  t3 ON t2.d = t3.d ORDER BY t1.id, t2.id;
--Testcase 106:
SELECT t1.id, t2.id, t3.id FROM typetest1  t1 LEFT  JOIN typetest1  t2 ON t1.d = t2.d FULL  JOIN typetest1  t3 ON t2.d = t3.d ORDER BY t1.id, t2.id;
---- right outer join three tables
--Testcase 107:
EXPLAIN (COSTS off)
SELECT t1.id, t2.id, t3.id FROM typetest1  t1 RIGHT JOIN typetest1  t2 ON t1.d = t2.d RIGHT JOIN typetest1  t3 ON t2.d = t3.d ORDER BY t1.id, t2.id;
--Testcase 108:
SELECT t1.id, t2.id, t3.id FROM typetest1  t1 RIGHT JOIN typetest1  t2 ON t1.d = t2.d RIGHT JOIN typetest1  t3 ON t2.d = t3.d ORDER BY t1.id, t2.id;
---- right outer join + inner outer join
--Testcase 109:
EXPLAIN (COSTS off)
SELECT t1.id, t2.id, t3.id FROM typetest1  t1 RIGHT JOIN typetest1  t2 ON t1.d = t2.d INNER JOIN typetest1  t3 ON t2.d = t3.d ORDER BY t1.id, t2.id;
--Testcase 110:
SELECT t1.id, t2.id, t3.id FROM typetest1  t1 RIGHT JOIN typetest1  t2 ON t1.d = t2.d INNER JOIN typetest1  t3 ON t2.d = t3.d ORDER BY t1.id, t2.id;
---- right outer join + left outer join
--Testcase 111:
EXPLAIN (COSTS off)
SELECT t1.id, t2.id, t3.id FROM typetest1  t1 RIGHT JOIN typetest1  t2 ON t1.d = t2.d LEFT  JOIN typetest1  t3 ON t2.d = t3.d ORDER BY t1.id, t2.id;
--Testcase 112:
SELECT t1.id, t2.id, t3.id FROM typetest1  t1 RIGHT JOIN typetest1  t2 ON t1.d = t2.d LEFT  JOIN typetest1  t3 ON t2.d = t3.d ORDER BY t1.id, t2.id;
---- right outer join + full outer join
--Testcase 113:
EXPLAIN (COSTS off)
SELECT t1.id, t2.id, t3.id FROM typetest1  t1 RIGHT JOIN typetest1  t2 ON t1.d = t2.d FULL  JOIN typetest1  t3 ON t2.d = t3.d ORDER BY t1.id, t2.id;
--Testcase 114:
SELECT t1.id, t2.id, t3.id FROM typetest1  t1 RIGHT JOIN typetest1  t2 ON t1.d = t2.d FULL  JOIN typetest1  t3 ON t2.d = t3.d ORDER BY t1.id, t2.id;
---- full outer join three tables
--Testcase 115:
EXPLAIN (COSTS off)
SELECT t1.id, t2.id, t3.id FROM typetest1  t1 FULL  JOIN typetest1  t2 ON t1.d = t2.d FULL  JOIN typetest1  t3 ON t2.d = t3.d ORDER BY t1.id, t2.id;
--Testcase 116:
SELECT t1.id, t2.id, t3.id FROM typetest1  t1 FULL  JOIN typetest1  t2 ON t1.d = t2.d FULL  JOIN typetest1  t3 ON t2.d = t3.d ORDER BY t1.id, t2.id;
---- full outer join + inner join
--Testcase 117:
EXPLAIN (COSTS off)
SELECT t1.id, t2.id, t3.id FROM typetest1  t1 FULL  JOIN typetest1  t2 ON t1.d = t2.d INNER JOIN typetest1  t3 ON t2.d = t3.d ORDER BY t1.id, t2.id;
--Testcase 118:
SELECT t1.id, t2.id, t3.id FROM typetest1  t1 FULL  JOIN typetest1  t2 ON t1.d = t2.d INNER JOIN typetest1  t3 ON t2.d = t3.d ORDER BY t1.id, t2.id;
---- full outer join + left outer join
--Testcase 119:
EXPLAIN (COSTS off)
SELECT t1.id, t2.id, t3.id FROM typetest1  t1 FULL  JOIN typetest1  t2 ON t1.d = t2.d LEFT  JOIN typetest1  t3 ON t2.d = t3.d ORDER BY t1.id, t2.id;
--Testcase 120:
SELECT t1.id, t2.id, t3.id FROM typetest1  t1 FULL  JOIN typetest1  t2 ON t1.d = t2.d LEFT  JOIN typetest1  t3 ON t2.d = t3.d ORDER BY t1.id, t2.id;
---- full outer join + right outer join
--Testcase 121:
EXPLAIN (COSTS off)
SELECT t1.id, t2.id, t3.id FROM typetest1  t1 FULL  JOIN typetest1  t2 ON t1.d = t2.d RIGHT JOIN typetest1  t3 ON t2.d = t3.d ORDER BY t1.id, t2.id;
--Testcase 122:
SELECT t1.id, t2.id, t3.id FROM typetest1  t1 FULL  JOIN typetest1  t2 ON t1.d = t2.d RIGHT JOIN typetest1  t3 ON t2.d = t3.d ORDER BY t1.id, t2.id;
-- join with LATERAL reference
--Testcase 123:
EXPLAIN (COSTS off)
SELECT t1.id, sl.c FROM typetest1  t1, LATERAL (SELECT DISTINCT s.c FROM shorty s,   longy l WHERE s.id = l.id AND l.c = t1.c) sl ORDER BY t1.id, sl.c;
--Testcase 124:
SELECT t1.id, sl.c FROM typetest1  t1, LATERAL (SELECT DISTINCT s.c FROM shorty s,   longy l WHERE s.id = l.id AND l.c = t1.c) sl ORDER BY t1.id, sl.c;
-- test for bug #279 fixed with 839b125e1bdc63b71220ccd675fa852c028de9ea
--Testcase 125:
SELECT 1
FROM typetest1 a
   LEFT JOIN typetest1 b ON (a.id IS NOT NULL)
WHERE (a.c = a.vc) = (b.id IS NOT NULL);

/*
 * Cost estimates.
 */
-- gather statistics
ANALYZE typetest1;
-- costs with statistics
--Testcase 126:
EXPLAIN SELECT t1.id, t2.id FROM typetest1 t1, typetest1 t2 WHERE t1.c = t2.c;
--Testcase 127:
EXPLAIN SELECT t1.id, t2.id FROM typetest1 t1, typetest1 t2 WHERE t1.c <> t2.c;

-- clean up
--Testcase 128:
DROP EXTENSION oracle_fdw CASCADE;
