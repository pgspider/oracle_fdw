SET client_min_messages = WARNING;
CREATE EXTENSION oracle_fdw;

CREATE SERVER oracle_srv FOREIGN DATA WRAPPER oracle_fdw OPTIONS (dbserver '', isolation_level 'read_committed', nchar 'true');

CREATE USER MAPPING FOR CURRENT_USER SERVER oracle_srv OPTIONS (user 'test', password 'test');

-- Init data for numeric function
SELECT oracle_execute('oracle_srv', 'DROP TABLE test.numeric_tbl PURGE');

SELECT oracle_execute(
          'oracle_srv',
          E'CREATE TABLE NUMERIC_TBL (\n'
          '   id        NUMBER(5) PRIMARY KEY,\n'
          '   tag1      VARCHAR(10),\n'
          '   value1    float,\n'
          '   value2    NUMBER(5),\n'
          '   value3    float,\n'
          '   value4    NUMBER(5),\n'
          '   value5    float,\n'
          '   value6    float,\n'
          '   value7    float,\n'
          '   value8    NUMBER(5),\n'
          '   str1      VARCHAR(10),\n'
          '   str2      VARCHAR(10),\n'
          '   str3      VARCHAR(20),\n'
          '   str4      VARCHAR(20),\n'
          '   str5      VARCHAR(20)\n'
          ') SEGMENT CREATION IMMEDIATE'
       );

CREATE FOREIGN TABLE numeric_tbl (
  id int OPTIONS (key 'yes') NOT NULL ,
	tag1 text, 
  value1 float, 
  value2 int, 
  value3 float, 
  value4 int,
  value5 float,
  value6 float,
  value7 float,
  value8 int,
  str1 text, 
  str2 text,
  str3 text,
  str4 text,
  str5 text
) SERVER oracle_srv OPTIONS (table 'NUMERIC_TBL');

INSERT INTO numeric_tbl VALUES (0, 'a', 0.1, 100, -0.1, -100, 0.1, 1.2, 5.0, 1 ,  '---XYZ---', '   abc   ', 'This is',           '2017-03-31 9:30:20', '12,345.6-');
INSERT INTO numeric_tbl VALUES (1, 'a', 0.2, 100, -0.2, -100, 0.2, 2.3, 6.0, 2 ,  '---XYZ---', '   abc   ', 'the test string',   '2017-03-31 9:30:20', '12,345.6-');
INSERT INTO numeric_tbl VALUES (2, 'a', 0.3, 100, -0.3, -100, 0.3, 3.4, 7.5, 3 ,  '---XYZ---', '   abc   ', 'containing space',  '2017-03-31 9:30:20', '12,345.6-');
INSERT INTO numeric_tbl VALUES (3, 'b', 1.1, 200, -1.1, -200, 0.4, 4.5, 8.0, 1 ,  '---XYZ---', '   abc   ', 'between the words', '2017-03-31 9:30:20', '12,345.6-');
INSERT INTO numeric_tbl VALUES (4, 'b', 2.2, 200, -2.2, -200, 0.5, 5.6, 9.0, 2 ,  '---XYZ---', '   abc   ', 'reserved string',   '2017-03-31 9:30:20', '12,345.6-');
INSERT INTO numeric_tbl VALUES (5, 'b', 3.3, 200, -3.3, -200, 0.6, 6.7, 10.5, 3 , '---XYZ---', '   abc   ', 'reserved string2',  '2017-03-31 9:30:20', '12,345.6-');

SELECT * FROM numeric_tbl;

--
-- Init data for date/Time function
--
SELECT oracle_execute('oracle_srv', 'DROP TABLE test.time_tbl PURGE');

SELECT oracle_execute(
          'oracle_srv',
          E'CREATE TABLE test.time_tbl (\n'
          '   id   NUMBER(5) PRIMARY KEY,\n'
          '   c1   TIMESTAMP WITH TIME ZONE,\n'
          '   c2   DATE,\n'
          '   c3   TIMESTAMP\n'
          ') SEGMENT CREATION IMMEDIATE'
       );

CREATE FOREIGN TABLE time_tbl(id int OPTIONS (key 'yes'), 
                              c1 timestamp with time zone, 
                              c2 date, 
                              c3 timestamp)
  SERVER oracle_srv OPTIONS(table 'TIME_TBL');

SELECT oracle_execute(
          'oracle_srv',
          E'INSERT INTO test.time_tbl VALUES (0, \n'
          ' TIMESTAMP ''2021-01-02 12:10:30.123456 +02:00'', \n'
          ' DATE ''2021-01-02'', \n'
          ' TIMESTAMP ''2021-01-03 12:10:30.123456'')'
        );
SELECT oracle_execute(
          'oracle_srv',
          E'INSERT INTO time_tbl VALUES (1, \n'
          ' TIMESTAMP ''2021-01-01 23:12:12.654321 -03:00'', \n'
          ' DATE ''2021-01-01'', \n'
          ' TIMESTAMP ''2021-01-04 23:12:12.654321'')'
        );
SELECT oracle_execute(
          'oracle_srv',
          E'INSERT INTO time_tbl VALUES (2, \n'
          ' TIMESTAMP ''2021-01-10 11:12:12.112233 +04:00'', \n'
          ' DATE ''2021-01-10'', \n'
          ' TIMESTAMP ''2021-01-05 11:12:12.112233'')'
        );
SELECT oracle_execute(
          'oracle_srv',
          E'INSERT INTO time_tbl VALUES (3, \n'
          ' TIMESTAMP ''2021-01-15 05:59:59.654321 -05:00'', \n'
          ' DATE ''2021-01-15'', \n'
          ' TIMESTAMP ''2021-01-06 15:59:59.654321'')'
        );
SELECT oracle_execute(
          'oracle_srv',
          E'INSERT INTO time_tbl VALUES (4, \n'
          ' TIMESTAMP ''2021-01-29 00:59:59.000102 +06:00'', \n'
          ' DATE ''2021-01-29'', \n'
          ' TIMESTAMP ''2021-01-07 00:59:59.000102'')'
        );

SELECT * FROM time_tbl;

--
-- End init data for date/time function
--

--
-- Init data for character function
--

-- drop the Oracle tables if they exist
SELECT oracle_execute('oracle_srv', 'DROP TABLE test.character_tbl PURGE');

SELECT oracle_execute(
          'oracle_srv',
          E'CREATE TABLE test.character_tbl (\n'
          '   id  NUMBER(5) PRIMARY KEY,\n'
          '   c   CHAR(10 CHAR),\n'
          '   nc  NCHAR(10),\n'
          '   vc  VARCHAR2(10 CHAR),\n'
          '   nvc NVARCHAR2(10),\n'
          '   lc  CLOB,\n'
          '   n   NUMBER(5),\n'
          '   fl  BINARY_FLOAT,\n'
          '   db  BINARY_DOUBLE,\n'
          '   itv  INTERVAL YEAR TO MONTH,\n'
          '   timetz  TIMESTAMP WITH TIME ZONE,\n'
          '   dt  TIMESTAMP,\n'
          '   dt_text  CHAR(30 CHAR)\n'
          ') SEGMENT CREATION IMMEDIATE'
       );

CREATE FOREIGN TABLE character_tbl (
   id  integer OPTIONS (key 'yes') NOT NULL,
   c   character(10),
   nc  character(10),
   vc  character varying(10),
   nvc character varying(10),
   lc  text,
   n   int,
   fl  float,
   db  double precision,
   itv interval,
   timetz  timestamptz,
	 dt  timestamp,
   dt_text character(30)
) SERVER oracle_srv OPTIONS (table 'CHARACTER_TBL');

INSERT INTO character_tbl VALUES (
    60,
   'fixed char',
   'nat''l char',
   '   varlena',
   'nat''l var  ',
   'character large object',
   100,
   3.14159,
   7.3,
   '2 YEARS',
   '1999-12-02 10:00:00 -8:00',
   '1999-12-01 10:00:00',
   '10-Sep-02 14:10:10'
);

INSERT INTO character_tbl VALUES (
    79,
   's1mple',
   'perfecto',
   '  b1t  ',
   'Boombl4',
   'Natus Vincere',
   121,
   -3.14159,
   -2.63,
   '4 YEARS 5 MONTHS',
   '1999-11-03 11:11:11 -9:00',
   '1999-12-04 12:12:12',
   '12-Sep-02 15:10:10.123456'
);

SELECT * FROM character_tbl;

--
-- End init data for character function
--

-- Test for Numeric

-- ===============================================================================
-- test abs()
-- ===============================================================================
-- select abs (builtin function, EXPLAIN (COSTS OFF))
EXPLAIN (COSTS OFF)
SELECT abs(value1), abs(value2), abs(value3), abs(value4) FROM numeric_tbl;

-- select abs (buitin function, result)
SELECT abs(value1), abs(value2), abs(value3), abs(value4) FROM numeric_tbl;

-- select abs (builtin function, not pushdown constraints, EXPLAIN (COSTS OFF))
EXPLAIN (COSTS OFF)
SELECT abs(value1), abs(value2), abs(value3), abs(value4) FROM numeric_tbl WHERE to_hex(value2) != '64';

-- select abs (builtin function, not pushdown constraints, result)
SELECT abs(value1), abs(value2), abs(value3), abs(value4) FROM numeric_tbl WHERE to_hex(value2) != '64';

-- select abs (builtin function, pushdown constraints, EXPLAIN (COSTS OFF))
EXPLAIN (COSTS OFF)
SELECT abs(value1), abs(value2), abs(value3), abs(value4) FROM numeric_tbl WHERE value2 != 200;

-- select abs (builtin function, pushdown constraints, result)
SELECT abs(value1), abs(value2), abs(value3), abs(value4) FROM numeric_tbl WHERE value2 != 200;

-- ===============================================================================
-- test acos()
-- ===============================================================================
-- select acos (builtin function, EXPLAIN (COSTS OFF))
EXPLAIN (COSTS OFF)
SELECT value1, acos(value5), acos(0.5) FROM numeric_tbl;

-- select acos (builtin function, result)
SELECT value1, acos(value5), acos(0.5) FROM numeric_tbl;

-- select acos (builtin function, not pushdown constraints, EXPLAIN (COSTS OFF))
EXPLAIN (COSTS OFF)
SELECT value1, acos(value5), acos(0.5) FROM numeric_tbl WHERE to_hex(value2) = '64';

-- select acos (builtin function, not pushdown constraints, result)
SELECT value1, acos(value5), acos(0.5) FROM numeric_tbl WHERE to_hex(value2) = '64';

-- select acos (builtin function, pushdown constraints, EXPLAIN (COSTS OFF))
EXPLAIN (COSTS OFF)
SELECT value1, acos(value5), acos(0.5) FROM numeric_tbl WHERE value2 != 200;

-- select acos (builtin function, pushdown constraints, result)
SELECT value1, acos(value5), acos(0.5) FROM numeric_tbl WHERE value2 != 200;

-- select acos (builtin function, acos in constraints, EXPLAIN (COSTS OFF))
EXPLAIN (COSTS OFF)
SELECT value1, acos(value5), acos(0.5) FROM numeric_tbl WHERE acos(value5) != 1;

-- select acos (builtin function, acos in constraints, result)
SELECT value1, acos(value5), acos(0.5) FROM numeric_tbl WHERE acos(value5) != 1;

-- select acos (builtin function, acos in constraints, EXPLAIN (COSTS OFF))
EXPLAIN (COSTS OFF)
SELECT value1, acos(value5), acos(0.5) FROM numeric_tbl WHERE acos(0.5) > value1;

-- select acos (builtin function, acos in constraints, result)
SELECT value1, acos(value5), acos(0.5) FROM numeric_tbl WHERE acos(0.5) > value1;

-- select acos as nest function with agg (pushdown, EXPLAIN (COSTS OFF))
EXPLAIN (COSTS OFF)
SELECT sum(value3),acos(sum(value1)) FROM numeric_tbl WHERE value2 != 200;

-- select acos as nest function with agg (pushdown, result)
SELECT sum(value3),acos(sum(value1)) FROM numeric_tbl WHERE value2 != 200;

-- select acos as nest with log2 (pushdown, EXPLAIN (COSTS OFF))
-- should return failure because input is out of range
EXPLAIN (COSTS OFF)
SELECT value1, acos(log(2, value2)) FROM numeric_tbl;

-- select acos as nest with log2 (pushdown, result)
SELECT value1, acos(log(2, value2)) FROM numeric_tbl;

-- select acos with non pushdown func and explicit constant (EXPLAIN (COSTS OFF))
EXPLAIN (COSTS OFF)
SELECT acos(value5), pi(), 4.1 FROM numeric_tbl;

-- select acos with non pushdown func and explicit constant (result)
SELECT acos(value5), pi(), 4.1 FROM numeric_tbl;

-- select acos with order by (EXPLAIN (COSTS OFF))
EXPLAIN (COSTS OFF)
SELECT value1, acos(1-value5) FROM numeric_tbl ORDER BY acos(1-value5);

-- select acos with order by (result)
SELECT value1, acos(1-value5) FROM numeric_tbl ORDER BY acos(1-value5);

-- select acos with order by index (result)
SELECT value1, acos(1-value5) FROM numeric_tbl ORDER BY 2,1;

-- select acos with order by index (result)
SELECT value1, acos(1-value5) FROM numeric_tbl ORDER BY 1,2;

-- select acos with group by (EXPLAIN (COSTS OFF))
EXPLAIN (COSTS OFF)
SELECT count(value1), acos(1-value5) FROM numeric_tbl GROUP BY acos(1-value5);

-- select acos with group by (result)
SELECT count(value1), acos(1-value5) FROM numeric_tbl GROUP BY acos(1-value5);

-- select acos with group by index (result)
SELECT value1, acos(1-value5) FROM numeric_tbl GROUP BY 2,1;

-- select acos with group by index (result)
SELECT value1, acos(1-value5) FROM numeric_tbl GROUP BY 1,2;

-- select acos with group by having (EXPLAIN (COSTS OFF))
EXPLAIN (COSTS OFF)
SELECT count(value1), acos(1-value5) FROM numeric_tbl GROUP BY acos(1-value5) HAVING avg(value1) > 0;

-- select acos with group by having (result)
SELECT count(value1), acos(1-value5) FROM numeric_tbl GROUP BY acos(1-value5) HAVING avg(value1) > 0;

-- select acos with group by index having (result)
SELECT value1, acos(1-value5) FROM numeric_tbl GROUP BY 2,1 HAVING acos(1-value5) > 0;

-- select acos with group by index having (result)
SELECT value1, acos(1-value5) FROM numeric_tbl GROUP BY 1,2 HAVING value1 > 1;

-- select acos and as
SELECT acos(value5) as acos1 FROM numeric_tbl;

-- ===============================================================================
-- test asin()
-- ===============================================================================
-- select asin (builtin function, EXPLAIN (COSTS OFF))
EXPLAIN (COSTS OFF)
SELECT value1, asin(value5), asin(0.5) FROM numeric_tbl;

-- select asin (builtin function, result)
SELECT value1, asin(value5), asin(0.5) FROM numeric_tbl;

-- select asin (builtin function, not pushdown constraints, EXPLAIN (COSTS OFF))
EXPLAIN (COSTS OFF)
SELECT value1, asin(value5), asin(0.5) FROM numeric_tbl WHERE to_hex(value2) = '64';

-- select asin (builtin function, not pushdown constraints, result)
SELECT value1, asin(value5), asin(0.5) FROM numeric_tbl WHERE to_hex(value2) = '64';

-- select asin (builtin function, pushdown constraints, EXPLAIN (COSTS OFF))
EXPLAIN (COSTS OFF)
SELECT value1, asin(value5), asin(0.5) FROM numeric_tbl WHERE value2 != 200;

-- select asin (builtin function, pushdown constraints, result)
SELECT value1, asin(value5), asin(0.5) FROM numeric_tbl WHERE value2 != 200;

-- select asin (builtin function, asin in constraints, EXPLAIN (COSTS OFF))
EXPLAIN (COSTS OFF)
SELECT value1, asin(value5), asin(0.5) FROM numeric_tbl WHERE value1 != 1;

-- select asin (builtin function, asin in constraints, result)
SELECT value1, asin(value5), asin(0.5) FROM numeric_tbl WHERE value1 != 1;

-- select asin (builtin function, asin in constraints, EXPLAIN (COSTS OFF))
EXPLAIN (COSTS OFF)
SELECT value1, asin(value5), asin(0.5) FROM numeric_tbl WHERE asin(0.5) > value1;

-- select asin (builtin function, asin in constraints, result)
SELECT value1, asin(value5), asin(0.5) FROM numeric_tbl WHERE asin(0.5) > value1;

-- select asin as nest function with agg (pushdown, EXPLAIN (COSTS OFF))
EXPLAIN (COSTS OFF)
SELECT sum(value3),asin(sum(value1)) FROM numeric_tbl WHERE value2 != 200;

-- select asin as nest function with agg (pushdown, result)
SELECT sum(value3),asin(sum(value1)) FROM numeric_tbl WHERE value2 != 200;

-- select asin as nest with log2 (pushdown, EXPLAIN (COSTS OFF))
EXPLAIN (COSTS OFF)
SELECT value1, asin(log(2, value2)) FROM numeric_tbl;

-- select asin as nest with log2 (pushdown, result)
SELECT value1, asin(log(2, value2)) FROM numeric_tbl;

-- select asin with non pushdown func and explicit constant (EXPLAIN (COSTS OFF))
EXPLAIN (COSTS OFF)
SELECT value1, asin(value5), pi(), 4.1 FROM numeric_tbl;

-- select asin with non pushdown func and explicit constant (result)
SELECT value1, asin(value5), pi(), 4.1 FROM numeric_tbl;

-- select asin with order by (EXPLAIN (COSTS OFF))
EXPLAIN (COSTS OFF)
SELECT value1, asin(1-value5) FROM numeric_tbl ORDER BY asin(1-value5);

-- select asin with order by (result)
SELECT value1, asin(1-value5) FROM numeric_tbl ORDER BY asin(1-value5);

-- select asin with order by index (result)
SELECT value1, asin(1-value5) FROM numeric_tbl ORDER BY 2,1;

-- select asin with order by index (result)
SELECT value1, asin(1-value5) FROM numeric_tbl ORDER BY 1,2;

-- select asin with group by (EXPLAIN (COSTS OFF))
EXPLAIN (COSTS OFF)
SELECT value1, asin(1-value5) FROM numeric_tbl GROUP BY value1, asin(1-value5);

-- select asin with group by (result)
SELECT value1, asin(1-value5) FROM numeric_tbl GROUP BY value1, asin(1-value5);

-- select asin with group by index (result)
SELECT value1, asin(1-value5) FROM numeric_tbl GROUP BY 2,1;

-- select asin with group by index (result)
SELECT value1, asin(1-value5) FROM numeric_tbl GROUP BY 1,2;

-- select asin with group by having (EXPLAIN (COSTS OFF))
EXPLAIN (COSTS OFF)
SELECT value1, asin(1-value5) FROM numeric_tbl GROUP BY value1, asin(1-value5) HAVING avg(value1) > 0;

-- select asin with group by having (result)
SELECT value1, asin(1-value5) FROM numeric_tbl GROUP BY value1, asin(1-value5) HAVING avg(value1) > 0;

-- select asin with group by index having (result)
SELECT value1, asin(1-value5) FROM numeric_tbl GROUP BY 2,1 HAVING asin(1-value5) > 0;

-- select asin with group by index having (result)
SELECT value1, asin(1-value5) FROM numeric_tbl GROUP BY 1,2 HAVING value1 > 1;

-- select asin and as
SELECT value1, asin(value5) as asin1 FROM numeric_tbl;

-- ===============================================================================
-- test atan()
-- ===============================================================================
-- select atan (builtin function, EXPLAIN (COSTS OFF))
EXPLAIN (COSTS OFF)
SELECT atan(value1), atan(value2), atan(value3), atan(value4), atan(0.5) FROM numeric_tbl;

-- select atan (builtin function, result)
SELECT atan(value1), atan(value2), atan(value3), atan(value4), atan(0.5) FROM numeric_tbl;

-- select atan (builtin function, not pushdown constraints, EXPLAIN (COSTS OFF))
EXPLAIN (COSTS OFF)
SELECT atan(value1), atan(value2), atan(value3), atan(value4), atan(0.5) FROM numeric_tbl WHERE to_hex(value2) = '64';

-- select atan (builtin function, not pushdown constraints, result)
SELECT atan(value1), atan(value2), atan(value3), atan(value4), atan(0.5) FROM numeric_tbl WHERE to_hex(value2) = '64';

-- select atan (builtin function, pushdown constraints, EXPLAIN (COSTS OFF))
EXPLAIN (COSTS OFF)
SELECT atan(value1), atan(value2), atan(value3), atan(value4), atan(0.5) FROM numeric_tbl WHERE value2 != 200;

-- select atan (builtin function, pushdown constraints, result)
SELECT atan(value1), atan(value2), atan(value3), atan(value4), atan(0.5) FROM numeric_tbl WHERE value2 != 200;

-- select atan (builtin function, atan in constraints, EXPLAIN (COSTS OFF))
EXPLAIN (COSTS OFF)
SELECT atan(value1), atan(value2), atan(value3), atan(value4), atan(0.5) FROM numeric_tbl WHERE atan(value1) != 1;

-- select atan (builtin function, atan in constraints, result)
SELECT atan(value1), atan(value2), atan(value3), atan(value4), atan(0.5) FROM numeric_tbl WHERE atan(value1) != 1;

-- select atan (builtin function, atan in constraints, EXPLAIN (COSTS OFF))
EXPLAIN (COSTS OFF)
SELECT atan(value1), atan(value2), atan(value3), atan(value4), atan(0.5) FROM numeric_tbl WHERE atan(0.5) > value1;

-- select atan (builtin function, atan in constraints, result)
SELECT atan(value1), atan(value2), atan(value3), atan(value4), atan(0.5) FROM numeric_tbl WHERE atan(0.5) > value1;

-- select atan as nest function with agg (pushdown, EXPLAIN (COSTS OFF))
EXPLAIN (COSTS OFF)
SELECT sum(value3),atan(sum(value3)) FROM numeric_tbl;

-- select atan as nest function with agg (pushdown, result)
SELECT sum(value3),atan(sum(value3)) FROM numeric_tbl;

-- select atan as nest with log2 (pushdown, EXPLAIN (COSTS OFF))
EXPLAIN (COSTS OFF)
SELECT atan(log(2, value2)) FROM numeric_tbl;

-- select atan as nest with log2 (pushdown, result)
SELECT atan(log(2, value2)) FROM numeric_tbl;

-- select atan with non pushdown func and explicit constant (EXPLAIN (COSTS OFF))
EXPLAIN (COSTS OFF)
SELECT atan(value3), pi(), 4.1 FROM numeric_tbl;

-- select atan with non pushdown func and explicit constant (result)
SELECT atan(value3), pi(), 4.1 FROM numeric_tbl;

-- select atan with order by (EXPLAIN (COSTS OFF))
EXPLAIN (COSTS OFF)
SELECT value1, atan(1-value3) FROM numeric_tbl ORDER BY atan(1-value3);

-- select atan with order by (result)
SELECT value1, atan(1-value3) FROM numeric_tbl ORDER BY atan(1-value3);

-- select atan with order by index (result)
SELECT value1, atan(1-value3) FROM numeric_tbl ORDER BY 2,1;

-- select atan with order by index (result)
SELECT value1, atan(1-value3) FROM numeric_tbl ORDER BY 1,2;

-- select atan with group by (EXPLAIN (COSTS OFF))
EXPLAIN (COSTS OFF)
SELECT count(value1), atan(1-value3) FROM numeric_tbl GROUP BY atan(1-value3);

-- select atan with group by (result)
SELECT count(value1), atan(1-value3) FROM numeric_tbl GROUP BY atan(1-value3);

-- select atan with group by index (result)
SELECT value1, atan(1-value3) FROM numeric_tbl GROUP BY 2,1;

-- select atan with group by index (result)
SELECT value1, atan(1-value3) FROM numeric_tbl GROUP BY 1,2;

-- select atan with group by having (EXPLAIN (COSTS OFF))
EXPLAIN (COSTS OFF)
SELECT count(value1), atan(1-value3) FROM numeric_tbl GROUP BY atan(1-value3) HAVING atan(avg(value1)) > 0;

-- select atan with group by having (result)
SELECT count(value1), atan(1-value3) FROM numeric_tbl GROUP BY atan(1-value3) HAVING atan(avg(value1)) > 0;

-- select atan with group by index having (result)
SELECT value1, atan(1-value3) FROM numeric_tbl GROUP BY 2,1 HAVING atan(1-value3) > 0;

-- select atan with group by index having (result)
SELECT value1, atan(1-value3) FROM numeric_tbl GROUP BY 1,2 HAVING value1 > 1;

-- select atan and as
SELECT atan(value3) as atan1 FROM numeric_tbl;

-- ===============================================================================
-- test atan2()
-- ===============================================================================
-- select atan2 (builtin function, EXPLAIN (COSTS OFF))
EXPLAIN (COSTS OFF)
SELECT atan2(value1, 2), atan2(value2, 2), atan2(value3, 2), atan2(value4, 2), atan2(5, 2) FROM numeric_tbl;

-- select atan2 (builtin function, result)
SELECT atan2(value1, 2), atan2(value2, 2), atan2(value3, 2), atan2(value4, 2), atan2(5, 2) FROM numeric_tbl;

-- select atan2 (builtin function, not pushdown constraints, EXPLAIN (COSTS OFF))
EXPLAIN (COSTS OFF)
SELECT atan2(value1, 2), atan2(value2, 2), atan2(value3, 2), atan2(value4, 2), atan2(5, 2) FROM numeric_tbl WHERE to_hex(value2) = '64';

-- select atan2 (builtin function, not pushdown constraints, result)
SELECT atan2(value1, 2), atan2(value2, 2), atan2(value3, 2), atan2(value4, 2), atan2(5, 2) FROM numeric_tbl WHERE to_hex(value2) = '64';

-- select atan2 (builtin function, pushdown constraints, EXPLAIN (COSTS OFF))
EXPLAIN (COSTS OFF)
SELECT atan2(value1, 2), atan2(value2, 2), atan2(value3, 2), atan2(value4, 2), atan2(5, 2) FROM numeric_tbl WHERE value2 != 200;

-- select atan2 (builtin function, pushdown constraints, result)
SELECT atan2(value1, 2), atan2(value2, 2), atan2(value3, 2), atan2(value4, 2), atan2(5, 2) FROM numeric_tbl WHERE value2 != 200;

-- select atan2 (builtin function, atan2 in constraints, EXPLAIN (COSTS OFF))
EXPLAIN (COSTS OFF)
SELECT atan2(value1, 2), atan2(value2, 2), atan2(value3, 2), atan2(value4, 2), atan2(5, 2) FROM numeric_tbl WHERE atan2(value1, 2) != 1;

-- select atan2 (builtin function, atan2 in constraints, result)
SELECT atan2(value1, 2), atan2(value2, 2), atan2(value3, 2), atan2(value4, 2), atan2(5, 2) FROM numeric_tbl WHERE atan2(value1, 2) != 1;

-- select atan2 (builtin function, atan2 in constraints, EXPLAIN (COSTS OFF))
EXPLAIN (COSTS OFF)
SELECT atan2(value1, 2), atan2(value2, 2), atan2(value3, 2), atan2(value4, 2), atan2(5, 2) FROM numeric_tbl WHERE atan2(5, 2) > value1;

-- select atan2 (builtin function, atan2 in constraints, result)
SELECT atan2(value1, 2), atan2(value2, 2), atan2(value3, 2), atan2(value4, 2), atan2(5, 2) FROM numeric_tbl WHERE atan2(5, 2) > value1;

-- select atan2 as nest function with agg (pushdown, EXPLAIN (COSTS OFF))
EXPLAIN (COSTS OFF)
SELECT sum(value3),atan2(sum(value3), 2) FROM numeric_tbl;

-- select atan2 as nest function with agg (pushdown, result)
SELECT sum(value3),atan2(sum(value3), 2) FROM numeric_tbl;

-- select atan2 as nest with log2 (pushdown, EXPLAIN (COSTS OFF))
EXPLAIN (COSTS OFF)
SELECT atan2(log(2, value2), 2) FROM numeric_tbl;

-- select atan2 as nest with log2 (pushdown, result)
SELECT atan2(log(2, value2), 2) FROM numeric_tbl;

-- select atan2 with non pushdown func and atan2licit constant (EXPLAIN (COSTS OFF))
EXPLAIN (COSTS OFF)
SELECT atan2(value3, 2), pi(), 4.1 FROM numeric_tbl;

-- select atan2 with non pushdown func and atan2licit constant (result)
SELECT atan2(value3, 2), pi(), 4.1 FROM numeric_tbl;

-- select atan2 with order by (EXPLAIN (COSTS OFF))
EXPLAIN (COSTS OFF)
SELECT value1, atan2(1-value3, 2) FROM numeric_tbl ORDER BY atan2(1-value3, 2);

-- select atan2 with order by (result)
SELECT value1, atan2(1-value3, 2) FROM numeric_tbl ORDER BY atan2(1-value3, 2);

-- select atan2 with order by index (result)
SELECT value1, atan2(1-value3, 2) FROM numeric_tbl ORDER BY 2,1;

-- select atan2 with order by index (result)
SELECT value1, atan2(1-value3, 2) FROM numeric_tbl ORDER BY 1,2;

-- select atan2 with group by (EXPLAIN (COSTS OFF))
EXPLAIN (COSTS OFF)
SELECT count(value1), atan2(1-value3, 2) FROM numeric_tbl GROUP BY atan2(1-value3, 2);

-- select atan2 with group by (result)
SELECT count(value1), atan2(1-value3, 2) FROM numeric_tbl GROUP BY atan2(1-value3, 2);

-- select atan2 with group by index (result)
SELECT value1, atan2(1-value3, 2) FROM numeric_tbl GROUP BY 2,1;

-- select atan2 with group by index (result)
SELECT value1, atan2(1-value3, 2) FROM numeric_tbl GROUP BY 1,2;

-- select atan2 with group by having (EXPLAIN (COSTS OFF))
EXPLAIN (COSTS OFF)
SELECT count(value1), atan2(1-value3, 2) FROM numeric_tbl GROUP BY atan2(1-value3, 2) HAVING atan2(avg(value1), 2) > 0;

-- select atan2 with group by having (result)
SELECT count(value1), atan2(1-value3, 2) FROM numeric_tbl GROUP BY atan2(1-value3, 2) HAVING atan2(avg(value1), 2) > 0;

-- select atan2 with group by index having (result)
SELECT value1, atan2(1-value3, 2) FROM numeric_tbl GROUP BY 2,1 HAVING atan2(1-value3, 2) > 0;

-- select atan2 with group by index having (result)
SELECT value1, atan2(1-value3, 2) FROM numeric_tbl GROUP BY 1,2 HAVING value1 > 1;

-- select atan2 and as
SELECT atan2(value3, 2) as atan21 FROM numeric_tbl;

-- ===============================================================================
-- test ceil()
-- ===============================================================================
-- select ceil (builtin function, EXPLAIN (COSTS OFF))
EXPLAIN (COSTS OFF)
SELECT ceil(value1), ceil(value2), ceil(value3), ceil(value4), ceil(0.5) FROM numeric_tbl;

-- select ceil (builtin function, result)
SELECT ceil(value1), ceil(value2), ceil(value3), ceil(value4), ceil(0.5) FROM numeric_tbl;

-- select ceil (builtin function, not pushdown constraints, EXPLAIN (COSTS OFF))
EXPLAIN (COSTS OFF)
SELECT ceil(value1), ceil(value2), ceil(value3), ceil(value4), ceil(0.5) FROM numeric_tbl WHERE to_hex(value2) = '64';

-- select ceil (builtin function, not pushdown constraints, result)
SELECT ceil(value1), ceil(value2), ceil(value3), ceil(value4), ceil(0.5) FROM numeric_tbl WHERE to_hex(value2) = '64';

-- select ceil (builtin function, pushdown constraints, EXPLAIN (COSTS OFF))
EXPLAIN (COSTS OFF)
SELECT ceil(value1), ceil(value2), ceil(value3), ceil(value4), ceil(0.5) FROM numeric_tbl WHERE value2 != 200;

-- select ceil (builtin function, pushdown constraints, result)
SELECT ceil(value1), ceil(value2), ceil(value3), ceil(value4), ceil(0.5) FROM numeric_tbl WHERE value2 != 200;

-- select ceil (builtin function, ceil in constraints, EXPLAIN (COSTS OFF))
EXPLAIN (COSTS OFF)
SELECT ceil(value1), ceil(value2), ceil(value3), ceil(value4), ceil(0.5) FROM numeric_tbl WHERE ceil(value1) != 1;

-- select ceil (builtin function, ceil in constraints, result)
SELECT ceil(value1), ceil(value2), ceil(value3), ceil(value4), ceil(0.5) FROM numeric_tbl WHERE ceil(value1) != 1;

-- select ceil (builtin function, ceil in constraints, EXPLAIN (COSTS OFF))
EXPLAIN (COSTS OFF)
SELECT ceil(value1), ceil(value2), ceil(value3), ceil(value4), ceil(0.5) FROM numeric_tbl WHERE ceil(0.5) > value1;

-- select ceil (builtin function, ceil in constraints, result)
SELECT ceil(value1), ceil(value2), ceil(value3), ceil(value4), ceil(0.5) FROM numeric_tbl WHERE ceil(0.5) > value1;

-- select ceil as nest function with agg (pushdown, EXPLAIN (COSTS OFF))
EXPLAIN (COSTS OFF)
SELECT sum(value3),ceil(sum(value3)) FROM numeric_tbl;

-- select ceil as nest function with agg (pushdown, result)
SELECT sum(value3),ceil(sum(value3)) FROM numeric_tbl;

-- select ceil as nest with log2 (pushdown, EXPLAIN (COSTS OFF))
EXPLAIN (COSTS OFF)
SELECT ceil(log(2, value2)) FROM numeric_tbl;

-- select ceil as nest with log2 (pushdown, result)
SELECT ceil(log(2, value2)) FROM numeric_tbl;

-- select ceil with non pushdown func and explicit constant (EXPLAIN (COSTS OFF))
EXPLAIN (COSTS OFF)
SELECT ceil(value3), pi(), 4.1 FROM numeric_tbl;

-- select ceil with non pushdown func and explicit constant (result)
SELECT ceil(value3), pi(), 4.1 FROM numeric_tbl;

-- select ceil with order by (EXPLAIN (COSTS OFF))
EXPLAIN (COSTS OFF)
SELECT value1, ceil(1-value3) FROM numeric_tbl ORDER BY ceil(1-value3);

-- select ceil with order by (result)
SELECT value1, ceil(1-value3) FROM numeric_tbl ORDER BY ceil(1-value3);

-- select ceil with order by index (result)
SELECT value1, ceil(1-value3) FROM numeric_tbl ORDER BY 2,1;

-- select ceil with order by index (result)
SELECT value1, ceil(1-value3) FROM numeric_tbl ORDER BY 1,2;

-- select ceil with group by (EXPLAIN (COSTS OFF))
EXPLAIN (COSTS OFF)
SELECT value1, ceil(1-value3) FROM numeric_tbl GROUP BY value1, ceil(1-value3);

-- select ceil with group by (result)
SELECT value1, ceil(1-value3) FROM numeric_tbl GROUP BY value1, ceil(1-value3);

-- select ceil with group by index (result)
SELECT value1, ceil(1-value3) FROM numeric_tbl GROUP BY 2,1;

-- select ceil with group by index (result)
SELECT value1, ceil(1-value3) FROM numeric_tbl GROUP BY 1,2;

-- select ceil with group by having (EXPLAIN (COSTS OFF))
EXPLAIN (COSTS OFF)
SELECT value1, ceil(1-value3) FROM numeric_tbl GROUP BY value1, ceil(1-value3) HAVING ceil(avg(value1)) > 0;

-- select ceil with group by having (result)
SELECT value1, ceil(1-value3) FROM numeric_tbl GROUP BY value1, ceil(1-value3) HAVING ceil(avg(value1)) > 0;

-- select ceil with group by index having (result)
SELECT value1, ceil(1-value3) FROM numeric_tbl GROUP BY 2,1 HAVING ceil(1-value3) > 0;

-- select ceil with group by index having (result)
SELECT value1, ceil(1-value3) FROM numeric_tbl GROUP BY 1,2 HAVING value1 > 1;

-- select ceil and as
SELECT ceil(value3) as ceil1 FROM numeric_tbl;

-- ===============================================================================
-- test ceiling()
-- ===============================================================================
-- select ceiling (builtin function, EXPLAIN (COSTS OFF))
EXPLAIN (COSTS OFF)
SELECT ceiling(value1), ceiling(value2), ceiling(value3), ceiling(value4), ceiling(0.5) FROM numeric_tbl;

-- select ceiling (builtin function, result)
SELECT ceiling(value1), ceiling(value2), ceiling(value3), ceiling(value4), ceiling(0.5) FROM numeric_tbl;

-- select ceiling (builtin function, not pushdown constraints, EXPLAIN (COSTS OFF))
EXPLAIN (COSTS OFF)
SELECT ceiling(value1), ceiling(value2), ceiling(value3), ceiling(value4), ceiling(0.5) FROM numeric_tbl WHERE to_hex(value2) = '64';

-- select ceiling (builtin function, not pushdown constraints, result)
SELECT ceiling(value1), ceiling(value2), ceiling(value3), ceiling(value4), ceiling(0.5) FROM numeric_tbl WHERE to_hex(value2) = '64';

-- select ceiling (builtin function, pushdown constraints, EXPLAIN (COSTS OFF))
EXPLAIN (COSTS OFF)
SELECT ceiling(value1), ceiling(value2), ceiling(value3), ceiling(value4), ceiling(0.5) FROM numeric_tbl WHERE value2 != 200;

-- select ceiling (builtin function, pushdown constraints, result)
SELECT ceiling(value1), ceiling(value2), ceiling(value3), ceiling(value4), ceiling(0.5) FROM numeric_tbl WHERE value2 != 200;

-- select ceiling (builtin function, ceiling in constraints, EXPLAIN (COSTS OFF))
EXPLAIN (COSTS OFF)
SELECT ceiling(value1), ceiling(value2), ceiling(value3), ceiling(value4), ceiling(0.5) FROM numeric_tbl WHERE ceiling(value1) != 1;

-- select ceiling (builtin function, ceiling in constraints, result)
SELECT ceiling(value1), ceiling(value2), ceiling(value3), ceiling(value4), ceiling(0.5) FROM numeric_tbl WHERE ceiling(value1) != 1;

-- select ceiling (builtin function, ceiling in constraints, EXPLAIN (COSTS OFF))
EXPLAIN (COSTS OFF)
SELECT ceiling(value1), ceiling(value2), ceiling(value3), ceiling(value4), ceiling(0.5) FROM numeric_tbl WHERE ceiling(0.5) > value1;

-- select ceiling (builtin function, ceiling in constraints, result)
SELECT ceiling(value1), ceiling(value2), ceiling(value3), ceiling(value4), ceiling(0.5) FROM numeric_tbl WHERE ceiling(0.5) > value1;

-- select ceiling as nest function with agg (pushdown, EXPLAIN (COSTS OFF))
EXPLAIN (COSTS OFF)
SELECT sum(value3),ceiling(sum(value3)) FROM numeric_tbl;

-- select ceiling as nest function with agg (pushdown, result)
SELECT sum(value3),ceiling(sum(value3)) FROM numeric_tbl;

-- select ceiling as nest with log2 (pushdown, EXPLAIN (COSTS OFF))
EXPLAIN (COSTS OFF)
SELECT ceiling(log(2, value2)) FROM numeric_tbl;

-- select ceiling as nest with log2 (pushdown, result)
SELECT ceiling(log(2, value2)) FROM numeric_tbl;

-- select ceiling with non pushdown func and explicit constant (EXPLAIN (COSTS OFF))
EXPLAIN (COSTS OFF)
SELECT ceiling(value3), pi(), 4.1 FROM numeric_tbl;

-- select ceiling with non pushdown func and explicit constant (result)
SELECT ceiling(value3), pi(), 4.1 FROM numeric_tbl;

-- select ceiling with order by (EXPLAIN (COSTS OFF))
EXPLAIN (COSTS OFF)
SELECT value1, ceiling(1-value3) FROM numeric_tbl ORDER BY ceiling(1-value3);

-- select ceiling with order by (result)
SELECT value1, ceiling(1-value3) FROM numeric_tbl ORDER BY ceiling(1-value3);

-- select ceiling with order by index (result)
SELECT value1, ceiling(1-value3) FROM numeric_tbl ORDER BY 2,1;

-- select ceiling with order by index (result)
SELECT value1, ceiling(1-value3) FROM numeric_tbl ORDER BY 1,2;

-- select ceiling with group by (EXPLAIN (COSTS OFF))
EXPLAIN (COSTS OFF)
SELECT count(value1), ceiling(1-value3) FROM numeric_tbl GROUP BY ceiling(1-value3);

-- select ceiling with group by (result)
SELECT count(value1), ceiling(1-value3) FROM numeric_tbl GROUP BY ceiling(1-value3);

-- select ceiling with group by index (result)
SELECT value1, ceiling(1-value3) FROM numeric_tbl GROUP BY 2,1;

-- select ceiling with group by index (result)
SELECT value1, ceiling(1-value3) FROM numeric_tbl GROUP BY 1,2;

-- select ceiling with group by having (EXPLAIN (COSTS OFF))
EXPLAIN (COSTS OFF)
SELECT count(value1), ceiling(1-value3) FROM numeric_tbl GROUP BY ceiling(1-value3) HAVING ceiling(avg(value1)) > 0;

-- select ceiling with group by having (result)
SELECT count(value1), ceiling(1-value3) FROM numeric_tbl GROUP BY ceiling(1-value3) HAVING ceiling(avg(value1)) > 0;

-- select ceiling with group by index having (result)
SELECT value1, ceiling(1-value3) FROM numeric_tbl GROUP BY 2,1 HAVING ceiling(1-value3) > 0;

-- select ceiling with group by index having (result)
SELECT value1, ceiling(1-value3) FROM numeric_tbl GROUP BY 1,2 HAVING value1 > 1;

-- select ceiling and as
SELECT ceiling(value3) as ceiling1 FROM numeric_tbl;

-- ===============================================================================
-- test char_length()
-- ===============================================================================
-- select char_length (stub function, EXPLAIN (COSTS OFF))
EXPLAIN (COSTS OFF)
SELECT char_length(str4), char_length(str1), char_length(str2) FROM numeric_tbl;
-- select char_length (stub function, result)
SELECT char_length(str4), char_length(str1), char_length(str2) FROM numeric_tbl;

-- select char_length (stub function, not pushdown constraints, EXPLAIN (COSTS OFF))
EXPLAIN (COSTS OFF)
SELECT id, char_length(str4) FROM numeric_tbl WHERE to_hex(value2) = '64';
-- select char_length (stub function, not pushdown constraints, EXPLAIN (COSTS OFF))
SELECT id, char_length(str4) FROM numeric_tbl WHERE to_hex(value2) = '64';

-- select char_length (stub function, char_length in constraints, EXPLAIN (COSTS OFF))
EXPLAIN (COSTS OFF)
SELECT id, char_length(str4) FROM numeric_tbl WHERE char_length(str4) > 0;
-- select char_length (stub function, char_length in constraints, result)
SELECT id, char_length(str4) FROM numeric_tbl WHERE char_length(str4) > 0;

-- select char_length with non pushdown func and explicit constant (EXPLAIN (COSTS OFF))
EXPLAIN (COSTS OFF)
SELECT char_length(str4), pi(), 4.1 FROM numeric_tbl;
-- select char_length with non pushdown func and explicit constant (result)
SELECT char_length(str4), pi(), 4.1 FROM numeric_tbl;

-- select char_length with order by (EXPLAIN (COSTS OFF))
EXPLAIN (COSTS OFF)
SELECT value1, char_length(str4) FROM numeric_tbl ORDER BY char_length(str4), 1 DESC;
-- select char_length with order by (result)
SELECT value1, char_length(str4) FROM numeric_tbl ORDER BY char_length(str4), 1 DESC;

-- select char_length with group by (EXPLAIN (COSTS OFF))
EXPLAIN (COSTS OFF)
SELECT count(value1), char_length(str4) FROM numeric_tbl GROUP BY char_length(str4);
-- select char_length with group by (result)
SELECT count(value1), char_length(str4) FROM numeric_tbl GROUP BY char_length(str4);

-- select char_length with group by index (result)
SELECT value1, char_length(str4) FROM numeric_tbl GROUP BY 2,1;

-- select char_length with group by having (EXPLAIN (COSTS OFF))
EXPLAIN (COSTS OFF)
SELECT count(value1), char_length(str4) FROM numeric_tbl GROUP BY char_length(str4), str1 HAVING char_length(str4) > 0;
-- select char_length with group by having (result)
SELECT count(value1), char_length(str4) FROM numeric_tbl GROUP BY char_length(str4), str1 HAVING char_length(str4) > 0;

-- select char_length with group by index having (result)
SELECT value1, char_length(str4) FROM numeric_tbl GROUP BY 1,2 HAVING value1 > 1;

-- ===============================================================================
-- test character_length()
-- ===============================================================================
-- select character_length (stub function, EXPLAIN (COSTS OFF))
EXPLAIN (COSTS OFF)
SELECT character_length(tag1), character_length(str1), character_length(str2) FROM numeric_tbl;
-- select character_length (stub function, result)
SELECT character_length(tag1), character_length(str1), character_length(str2) FROM numeric_tbl;

-- select character_length (stub function, not pushdown constraints, EXPLAIN (COSTS OFF))
EXPLAIN (COSTS OFF)
SELECT id, character_length(str1) FROM numeric_tbl WHERE to_hex(value2) = '64';
-- select character_length (stub function, not pushdown constraints, EXPLAIN (COSTS OFF))
SELECT id, character_length(str1) FROM numeric_tbl WHERE to_hex(value2) = '64';

-- select character_length (stub function, character_length in constraints, EXPLAIN (COSTS OFF))
EXPLAIN (COSTS OFF)
SELECT id, character_length(str1) FROM numeric_tbl WHERE character_length(str1) > 0;
-- select character_length (stub function, character_length in constraints, result)
SELECT id, character_length(str1) FROM numeric_tbl WHERE character_length(str1) > 0;

-- select character_length with non pushdown func and explicit constant (EXPLAIN (COSTS OFF))
EXPLAIN (COSTS OFF)
SELECT character_length(str1), pi(), 4.1 FROM numeric_tbl;
-- select character_length with non pushdown func and explicit constant (result)
SELECT character_length(str1), pi(), 4.1 FROM numeric_tbl;

-- select character_length with order by (EXPLAIN (COSTS OFF))
EXPLAIN (COSTS OFF)
SELECT value1, character_length(str1) FROM numeric_tbl ORDER BY character_length(str1), 1 DESC;
-- select character_length with order by (result)
SELECT value1, character_length(str1) FROM numeric_tbl ORDER BY character_length(str1), 1 DESC;

-- select character_length with group by (EXPLAIN (COSTS OFF))
EXPLAIN (COSTS OFF)
SELECT count(value1), character_length(str1) FROM numeric_tbl GROUP BY character_length(str1);
-- select character_length with group by (result)
SELECT count(value1), character_length(str1) FROM numeric_tbl GROUP BY character_length(str1);

-- select character_length with group by index (result)
SELECT value1, character_length(str1) FROM numeric_tbl GROUP BY 2,1;

-- select character_length with group by having (EXPLAIN (COSTS OFF))
EXPLAIN (COSTS OFF)
SELECT count(value1), character_length(str1) FROM numeric_tbl GROUP BY character_length(str1), str1 HAVING character_length(str1) > 0;
-- select character_length with group by having (result)
SELECT count(value1), character_length(str1) FROM numeric_tbl GROUP BY character_length(str1), str1 HAVING character_length(str1) > 0;

-- select character_length with group by index having (result)
SELECT value1, character_length(str1) FROM numeric_tbl GROUP BY 1,2 HAVING value1 > 1;

-- ===============================================================================
-- test concat()
-- ===============================================================================
-- select concat (stub function, EXPLAIN (COSTS OFF))
EXPLAIN (COSTS OFF)
SELECT concat(id), concat(tag1), concat(value1), concat(value2), concat(str1) FROM numeric_tbl;
-- select concat (stub function, result)
SELECT concat(id), concat(tag1), concat(value1), concat(value2), concat(str1) FROM numeric_tbl;

-- select concat (stub function, pushdown constraints, EXPLAIN (COSTS OFF))
EXPLAIN (COSTS OFF)
SELECT id, concat(str1, str2) FROM numeric_tbl WHERE value2 != 100;
-- select concat (stub function, pushdown constraints, result)
SELECT id, concat(str1, str2) FROM numeric_tbl WHERE value2 != 100;

-- select concat (stub function, concat in constraints, EXPLAIN (COSTS OFF))
EXPLAIN (COSTS OFF)
SELECT id, concat(str1, str2) FROM numeric_tbl WHERE concat(str1, str2) != 'XYZ';
-- select concat (stub function, concat in constraints, EXPLAIN (COSTS OFF))
SELECT id, concat(str1, str2) FROM numeric_tbl WHERE concat(str1, str2) != 'XYZ';

-- select concat as nest function with agg (pushdown, EXPLAIN (COSTS OFF))
EXPLAIN (COSTS OFF)
SELECT id, concat(sum(value1), str1) FROM numeric_tbl GROUP BY id, str1;
-- select concat as nest function with agg (pushdown, result)
SELECT id, concat(sum(value1), str1) FROM numeric_tbl GROUP BY id, str1;

-- select concat with non pushdown func and explicit constant (EXPLAIN (COSTS OFF))
EXPLAIN (COSTS OFF)
SELECT concat(str1, str2), pi(), 4.1 FROM numeric_tbl;
-- select concat with non pushdown func and explicit constant (result)
SELECT concat(str1, str2), pi(), 4.1 FROM numeric_tbl;

-- select concat with order by (EXPLAIN (COSTS OFF))
EXPLAIN (COSTS OFF)
SELECT value1, concat(value2, str2) FROM numeric_tbl ORDER BY concat(value2, str2);
-- select concat with order by (result)
SELECT value1, concat(value2, str2) FROM numeric_tbl ORDER BY concat(value2, str2);

-- select concat with order by index (result)
SELECT value1, concat(value2, str2) FROM numeric_tbl ORDER BY 2,1;

-- select concat with group by (EXPLAIN (COSTS OFF))
EXPLAIN (COSTS OFF)
SELECT count(value1), concat(str1, str2) FROM numeric_tbl GROUP BY concat(str1, str2);
-- select concat with group by (result)
SELECT count(value1), concat(str1, str2) FROM numeric_tbl GROUP BY concat(str1, str2);

-- select concat with group by index (explain)
EXPLAIN (COSTS OFF)
SELECT value1, concat(str1, str2) FROM numeric_tbl GROUP BY 2,1;
-- select concat with group by index (result)
SELECT value1, concat(str1, str2) FROM numeric_tbl GROUP BY 2,1;

-- select concat with group by having (EXPLAIN (COSTS OFF))
EXPLAIN (COSTS OFF)
SELECT count(value1), concat(str1, str2) FROM numeric_tbl GROUP BY concat(str1, str2) HAVING concat(str1, str2) IS NOT NULL;
-- select concat with group by having (EXPLAIN (COSTS OFF))
SELECT count(value1), concat(str1, str2) FROM numeric_tbl GROUP BY concat(str1, str2) HAVING concat(str1, str2) IS NOT NULL;

-- select concat with group by index having (explain)
EXPLAIN (COSTS OFF)
SELECT value1, concat(str1, str2, value1, value2) FROM numeric_tbl GROUP BY 1,2 HAVING value1 > 1;
-- select concat with group by index having (result)
SELECT value1, concat(str1, str2, value1, value2) FROM numeric_tbl GROUP BY 1,2 HAVING value1 > 1;


-- ===============================================================================
-- test cos()
-- ===============================================================================
-- select cos (builtin function, EXPLAIN (COSTS OFF))
EXPLAIN (COSTS OFF)
SELECT value1, cos(value2), cos(value3), cos(value4), cos(0.5) FROM numeric_tbl;

-- select cos (builtin function, result)
SELECT value1, cos(value2), cos(value3), cos(value4), cos(0.5) FROM numeric_tbl;

-- select cos (builtin function, not pushdown constraints, EXPLAIN (COSTS OFF))
EXPLAIN (COSTS OFF)
SELECT cos(value1), cos(value2), cos(value3), cos(value4), cos(0.5) FROM numeric_tbl WHERE to_hex(value2) = '64';

-- select cos (builtin function, not pushdown constraints, result)
SELECT cos(value1), cos(value2), cos(value3), cos(value4), cos(0.5) FROM numeric_tbl WHERE to_hex(value2) = '64';

-- select cos (builtin function, pushdown constraints, EXPLAIN (COSTS OFF))
EXPLAIN (COSTS OFF)
SELECT cos(value1), cos(value2), cos(value3), cos(value4), cos(0.5) FROM numeric_tbl WHERE value2 != 200;

-- select cos (builtin function, pushdown constraints, result)
SELECT cos(value1), cos(value2), cos(value3), cos(value4), cos(0.5) FROM numeric_tbl WHERE value2 != 200;

-- select cos (builtin function, cos in constraints, EXPLAIN (COSTS OFF))
EXPLAIN (COSTS OFF)
SELECT value1, cos(value2), cos(value3), cos(value4), cos(0.5) FROM numeric_tbl WHERE cos(value1) != 1;

-- select cos (builtin function, cos in constraints, result)
SELECT value1, cos(value2), cos(value3), cos(value4), cos(0.5) FROM numeric_tbl WHERE cos(value1) != 1;

-- select cos (builtin function, cos in constraints, EXPLAIN (COSTS OFF))
EXPLAIN (COSTS OFF)
SELECT cos(value1), cos(value2), cos(value3), cos(value4), cos(0.5) FROM numeric_tbl WHERE cos(0.5) > value1;

-- select cos (builtin function, cos in constraints, result)
SELECT cos(value1), cos(value2), cos(value3), cos(value4), cos(0.5) FROM numeric_tbl WHERE cos(0.5) > value1;

-- select cos as nest function with agg (pushdown, EXPLAIN (COSTS OFF))
EXPLAIN (COSTS OFF)
SELECT sum(value3),cos(sum(value3)) FROM numeric_tbl;

-- select cos as nest function with agg (pushdown, result)
SELECT sum(value3),cos(sum(value3)) FROM numeric_tbl;

-- select cos as nest with log2 (pushdown, EXPLAIN (COSTS OFF))
EXPLAIN (COSTS OFF)
SELECT value1, cos(log(2, value2)) FROM numeric_tbl;

-- select cos as nest with log2 (pushdown, result)
SELECT value1, cos(log(2, value2)) FROM numeric_tbl;

-- select cos with non pushdown func and explicit constant (EXPLAIN (COSTS OFF))
EXPLAIN (COSTS OFF)
SELECT cos(value3), pi(), 4.1 FROM numeric_tbl;

-- select cos with non pushdown func and explicit constant (result)
SELECT cos(value3), pi(), 4.1 FROM numeric_tbl;

-- select cos with order by (EXPLAIN (COSTS OFF))
EXPLAIN (COSTS OFF)
SELECT value1, cos(1-value3) FROM numeric_tbl ORDER BY cos(1-value3);

-- select cos with order by (result)
SELECT value1, cos(1-value3) FROM numeric_tbl ORDER BY cos(1-value3);

-- select cos with order by index (result)
SELECT value1, cos(1-value3) FROM numeric_tbl ORDER BY 2,1;

-- select cos with order by index (result)
SELECT value1, cos(1-value3) FROM numeric_tbl ORDER BY 1,2;

-- select cos with group by (EXPLAIN (COSTS OFF))
EXPLAIN (COSTS OFF)
SELECT value1, cos(1-value3) FROM numeric_tbl GROUP BY value1, cos(1-value3);

-- select cos with group by (result)
SELECT value1, cos(1-value3) FROM numeric_tbl GROUP BY value1, cos(1-value3);

-- select cos with group by index (result)
SELECT value1, cos(1-value3) FROM numeric_tbl GROUP BY 2,1;

-- select cos with group by index (result)
SELECT value1, cos(1-value3) FROM numeric_tbl GROUP BY 1,2;

-- select cos with group by having (EXPLAIN (COSTS OFF))
EXPLAIN (COSTS OFF)
SELECT value1, cos(1-value3) FROM numeric_tbl GROUP BY value1, cos(1-value3) HAVING cos(avg(value1)) > 0;

-- select cos with group by having (result)
SELECT value1, cos(1-value3) FROM numeric_tbl GROUP BY value1, cos(1-value3) HAVING cos(avg(value1)) > 0;

-- select cos with group by index having (result)
SELECT value1, cos(1-value3) FROM numeric_tbl GROUP BY 2,1 HAVING cos(1-value3) > 0;

-- select cos with group by index having (result)
SELECT value1, cos(1-value3) FROM numeric_tbl GROUP BY 1,2 HAVING value1 > 1;

-- select cos and as
SELECT cos(value3) as cos1 FROM numeric_tbl;

-- ===============================================================================
-- test exp()
-- ===============================================================================
-- select exp (builtin function, EXPLAIN (COSTS OFF))
EXPLAIN (COSTS OFF)
SELECT exp(value1), exp(value2), exp(value3), exp(value4), exp(0.5) FROM numeric_tbl;

-- select exp (builtin function, result)
SELECT exp(value1), exp(value2), exp(value3), exp(value4), exp(0.5) FROM numeric_tbl;

-- select exp (builtin function, not pushdown constraints, EXPLAIN (COSTS OFF))
EXPLAIN (COSTS OFF)
SELECT exp(value1), exp(value2), exp(value3), exp(value4), exp(0.5) FROM numeric_tbl WHERE to_hex(value2) = '64';

-- select exp (builtin function, not pushdown constraints, result)
SELECT exp(value1), exp(value2), exp(value3), exp(value4), exp(0.5) FROM numeric_tbl WHERE to_hex(value2) = '64';

-- select exp (builtin function, pushdown constraints, EXPLAIN (COSTS OFF))
EXPLAIN (COSTS OFF)
SELECT exp(value1), exp(value2), exp(value3), exp(value4), exp(0.5) FROM numeric_tbl WHERE value2 != 200;

-- select exp (builtin function, pushdown constraints, result)
SELECT exp(value1), exp(value2), exp(value3), exp(value4), exp(0.5) FROM numeric_tbl WHERE value2 != 200;

-- select exp (builtin function, exp in constraints, EXPLAIN (COSTS OFF))
EXPLAIN (COSTS OFF)
SELECT exp(value1), exp(value2), exp(value3), exp(value4), exp(0.5) FROM numeric_tbl WHERE exp(value1) != 1;

-- select exp (builtin function, exp in constraints, result)
SELECT exp(value1), exp(value2), exp(value3), exp(value4), exp(0.5) FROM numeric_tbl WHERE exp(value1) != 1;

-- select exp (builtin function, exp in constraints, EXPLAIN (COSTS OFF))
EXPLAIN (COSTS OFF)
SELECT exp(value1), exp(value2), exp(value3), exp(value4), exp(0.5) FROM numeric_tbl WHERE exp(0.5) > value1;

-- select exp (builtin function, exp in constraints, result)
SELECT exp(value1), exp(value2), exp(value3), exp(value4), exp(0.5) FROM numeric_tbl WHERE exp(0.5) > value1;

-- select exp as nest function with agg (pushdown, EXPLAIN (COSTS OFF))
EXPLAIN (COSTS OFF)
SELECT sum(value3),exp(sum(value3)) FROM numeric_tbl;

-- select exp as nest function with agg (pushdown, result)
SELECT sum(value3),exp(sum(value3)) FROM numeric_tbl;

-- select exp as nest with log2 (pushdown, EXPLAIN (COSTS OFF))
EXPLAIN (COSTS OFF)
SELECT exp(log(2, value2)) FROM numeric_tbl;

-- select exp as nest with log2 (pushdown, result)
SELECT exp(log(2, value2)) FROM numeric_tbl;

-- select exp with non pushdown func and explicit constant (EXPLAIN (COSTS OFF))
EXPLAIN (COSTS OFF)
SELECT exp(value3), pi(), 4.1 FROM numeric_tbl;

-- select exp with non pushdown func and explicit constant (result)
SELECT exp(value3), pi(), 4.1 FROM numeric_tbl;

-- select exp with order by (EXPLAIN (COSTS OFF))
EXPLAIN (COSTS OFF)
SELECT value1, exp(1-value3) FROM numeric_tbl ORDER BY exp(1-value3);

-- select exp with order by (result)
SELECT value1, exp(1-value3) FROM numeric_tbl ORDER BY exp(1-value3);

-- select exp with order by index (result)
SELECT value1, exp(1-value3) FROM numeric_tbl ORDER BY 2,1;

-- select exp with order by index (result)
SELECT value1, exp(1-value3) FROM numeric_tbl ORDER BY 1,2;

-- select exp with group by (EXPLAIN (COSTS OFF))
EXPLAIN (COSTS OFF)
SELECT count(value1), exp(1-value3) FROM numeric_tbl GROUP BY exp(1-value3);

-- select exp with group by (result)
SELECT count(value1), exp(1-value3) FROM numeric_tbl GROUP BY exp(1-value3);

-- select exp with group by index (result)
SELECT value1, exp(1-value3) FROM numeric_tbl GROUP BY 2,1;

-- select exp with group by index (result)
SELECT value1, exp(1-value3) FROM numeric_tbl GROUP BY 1,2;

-- select exp with group by having (EXPLAIN (COSTS OFF))
EXPLAIN (COSTS OFF)
SELECT count(value1), exp(1-value3) FROM numeric_tbl GROUP BY exp(1-value3) HAVING exp(avg(value1)) > 0;

-- select exp with group by having (result)
SELECT count(value1), exp(1-value3) FROM numeric_tbl GROUP BY exp(1-value3) HAVING exp(avg(value1)) > 0;

-- select exp with group by index having (result)
SELECT value1, exp(1-value3) FROM numeric_tbl GROUP BY 2,1 HAVING exp(1-value3) > 0;

-- select exp with group by index having (result)
SELECT value1, exp(1-value3) FROM numeric_tbl GROUP BY 1,2 HAVING value1 > 1;

-- select exp and as
SELECT exp(value3) as exp1 FROM numeric_tbl;

-- ===============================================================================
-- test length()
-- ===============================================================================
-- select length (stub function, EXPLAIN (COSTS OFF))
EXPLAIN (COSTS OFF)
SELECT length(str1), length(str2) FROM numeric_tbl;
-- select length (stub function, result)
SELECT length(str1), length(str2) FROM numeric_tbl;

-- select length (stub function, not pushdown constraints, EXPLAIN (COSTS OFF))
EXPLAIN (COSTS OFF)
SELECT value1, length(str1) FROM numeric_tbl WHERE to_hex(value2) = '64';
-- select length (stub function, not pushdown constraints, result)
SELECT value1, length(str1) FROM numeric_tbl WHERE to_hex(value2) = '64';

-- select length (stub function, pushdown constraints, EXPLAIN (COSTS OFF))
EXPLAIN (COSTS OFF)
SELECT value1, length(str1) FROM numeric_tbl WHERE value2 != 200;
-- select length (stub function, pushdown constraints, result)
SELECT value1, length(str1) FROM numeric_tbl WHERE value2 != 200;

-- select length with non pushdown func and explicit constant (EXPLAIN (COSTS OFF))
EXPLAIN (COSTS OFF)
SELECT length(str1), pi(), 4.1 FROM numeric_tbl;
-- select length with non pushdown func and explicit constant (result)
SELECT length(str1), pi(), 4.1 FROM numeric_tbl;

-- select length with order by (EXPLAIN (COSTS OFF))
EXPLAIN (COSTS OFF)
SELECT value1, length(str1) FROM numeric_tbl ORDER BY length(str1);
-- select length with order by (result)
SELECT value1, length(str1) FROM numeric_tbl ORDER BY length(str1);

-- select length with order by index (result)
SELECT value1, length(str1) FROM numeric_tbl ORDER BY 2,1;
-- select length with order by index (result)
SELECT value1, length(str1) FROM numeric_tbl ORDER BY 1,2;

-- select length with group by (EXPLAIN (COSTS OFF))
EXPLAIN (COSTS OFF)
SELECT count(value1), length(str1) FROM numeric_tbl GROUP BY length(str1);
-- select length with group by (result)
SELECT count(value1), length(str1) FROM numeric_tbl GROUP BY length(str1);

-- select length with group by index (result)
SELECT value1, length(str1) FROM numeric_tbl GROUP BY 2,1;

-- select length with group by having (EXPLAIN (COSTS OFF))
EXPLAIN (COSTS OFF)
SELECT count(value1), length(str1) FROM numeric_tbl GROUP BY length(str1), str1 HAVING length(str1) IS NOT NULL;
-- select length with group by having (result)
SELECT count(value1), length(str1) FROM numeric_tbl GROUP BY length(str1), str1 HAVING length(str1) IS NOT NULL;

-- select length with group by index having (result)
SELECT value1, length(str1) FROM numeric_tbl GROUP BY 1,2 HAVING value1 > 1;

-- ===============================================================================
-- test lower()
-- ===============================================================================
-- select lower (stub function, EXPLAIN (COSTS OFF))
EXPLAIN (COSTS OFF)
SELECT lower(str1), lower(str2) FROM numeric_tbl;
-- select lower (stub function, result)
SELECT lower(str1), lower(str2) FROM numeric_tbl;

-- select lower (stub function, not pushdown constraints, EXPLAIN (COSTS OFF))
EXPLAIN (COSTS OFF)
SELECT value1, lower(str1) FROM numeric_tbl WHERE to_hex(value2) = '64';
-- select lower (stub function, not pushdown constraints, result)
SELECT value1, lower(str1) FROM numeric_tbl WHERE to_hex(value2) = '64';

-- select lower (stub function, pushdown constraints, EXPLAIN (COSTS OFF))
EXPLAIN (COSTS OFF)
SELECT value1, lower(str1) FROM numeric_tbl WHERE value2 != 200;
-- select lower (stub function, pushdown constraints, result)
SELECT value1, lower(str1) FROM numeric_tbl WHERE value2 != 200;

-- select lower with non pushdown func and explicit constant (EXPLAIN (COSTS OFF))
EXPLAIN (COSTS OFF)
SELECT lower(str1), pi(), 4.1 FROM numeric_tbl;
-- select lower with non pushdown func and explicit constant (result)
SELECT lower(str1), pi(), 4.1 FROM numeric_tbl;

-- select lower with order by (EXPLAIN (COSTS OFF))
EXPLAIN (COSTS OFF)
SELECT value1, lower(str1) FROM numeric_tbl ORDER BY lower(str1);
-- select lower with order by (result)
SELECT value1, lower(str1) FROM numeric_tbl ORDER BY lower(str1);

-- select lower with order by index (result)
SELECT value1, lower(str1) FROM numeric_tbl ORDER BY 2,1;
-- select lower with order by index (result)
SELECT value1, lower(str1) FROM numeric_tbl ORDER BY 1,2;

-- select lower with group by (EXPLAIN (COSTS OFF))
EXPLAIN (COSTS OFF)
SELECT count(value1), lower(str1) FROM numeric_tbl GROUP BY lower(str1);
-- select lower with group by (result)
SELECT count(value1), lower(str1) FROM numeric_tbl GROUP BY lower(str1);

-- select lower with group by index (result)
SELECT value1, lower(str1) FROM numeric_tbl GROUP BY 2,1;

-- select lower with group by having (EXPLAIN (COSTS OFF))
EXPLAIN (COSTS OFF)
SELECT count(value1), lower(str1) FROM numeric_tbl GROUP BY lower(str1), str1 HAVING lower(str1) IS NOT NULL;
-- select lower with group by having (result)
SELECT count(value1), lower(str1) FROM numeric_tbl GROUP BY lower(str1), str1 HAVING lower(str1) IS NOT NULL;

-- select lower with group by index having (result)
SELECT value1, lower(str1) FROM numeric_tbl GROUP BY 1,2 HAVING value1 > 1;

-- ===============================================================================
-- test lpad()
-- ===============================================================================
-- select lpad (stub function, EXPLAIN (COSTS OFF))
EXPLAIN (COSTS OFF)
SELECT lpad(str1, 4, 'ABCD'), lpad(str2, 4, 'ABCD') FROM numeric_tbl;
-- select lpad (stub function, result)
SELECT lpad(str1, 4, 'ABCD'), lpad(str2, 4, 'ABCD') FROM numeric_tbl;

-- select lpad (stub function, not pushdown constraints, EXPLAIN (COSTS OFF))
EXPLAIN (COSTS OFF)
SELECT value1, lpad(str1, 4, 'ABCD') FROM numeric_tbl WHERE to_hex(value2) = '64';
-- select lpad (stub function, not pushdown constraints, result)
SELECT value1, lpad(str1, 4, 'ABCD') FROM numeric_tbl WHERE to_hex(value2) = '64';

-- select lpad (stub function, pushdown constraints, EXPLAIN (COSTS OFF))
EXPLAIN (COSTS OFF)
SELECT value1, lpad(str1, 4, 'ABCD') FROM numeric_tbl WHERE value2 != 200;
-- select lpad (stub function, pushdown constraints, result)
SELECT value1, lpad(str1, 4, 'ABCD') FROM numeric_tbl WHERE value2 != 200;

-- select lpad with non pushdown func and explicit constant (EXPLAIN (COSTS OFF))
EXPLAIN (COSTS OFF)
SELECT lpad(str1, 4, 'ABCD'), pi(), 4.1 FROM numeric_tbl;
-- select lpad with non pushdown func and explicit constant (result)
SELECT lpad(str1, 4, 'ABCD'), pi(), 4.1 FROM numeric_tbl;

-- select lpad with order by (EXPLAIN (COSTS OFF))
EXPLAIN (COSTS OFF)
SELECT value1, lpad(str1, 4, 'ABCD') FROM numeric_tbl ORDER BY lpad(str1, 4, 'ABCD');
-- select lpad with order by (result)
SELECT value1, lpad(str1, 4, 'ABCD') FROM numeric_tbl ORDER BY lpad(str1, 4, 'ABCD');

-- select lpad with order by index (result)
SELECT value1, lpad(str1, 4, 'ABCD') FROM numeric_tbl ORDER BY 2,1;
-- select lpad with order by index (result)
SELECT value1, lpad(str1, 4, 'ABCD') FROM numeric_tbl ORDER BY 1,2;

-- select lpad with group by (EXPLAIN (COSTS OFF))
EXPLAIN (COSTS OFF)
SELECT count(value1), lpad(str1, 4, 'ABCD') FROM numeric_tbl GROUP BY lpad(str1, 4, 'ABCD');
-- select lpad with group by (result)
SELECT count(value1), lpad(str1, 4, 'ABCD') FROM numeric_tbl GROUP BY lpad(str1, 4, 'ABCD');

-- select lpad with group by index (result)
SELECT value1, lpad(str1, 4, 'ABCD') FROM numeric_tbl GROUP BY 2,1;

-- select lpad with group by having (EXPLAIN (COSTS OFF))
EXPLAIN (COSTS OFF)
SELECT count(value1), lpad(str1, 4, 'ABCD') FROM numeric_tbl GROUP BY lpad(str1, 4, 'ABCD'), str1 HAVING lpad(str1, 4, 'ABCD') IS NOT NULL;
-- select lpad with group by having (result)
SELECT count(value1), lpad(str1, 4, 'ABCD') FROM numeric_tbl GROUP BY lpad(str1, 4, 'ABCD'), str1 HAVING lpad(str1, 4, 'ABCD') IS NOT NULL;

-- select lpad with group by index having (result)
SELECT value1, lpad(str1, 4, 'ABCD') FROM numeric_tbl GROUP BY 1,2 HAVING value1 > 1;

-- ===============================================================================
-- test ltrim()
-- ===============================================================================
-- select ltrim (stub function, EXPLAIN (COSTS OFF))
EXPLAIN (COSTS OFF)
SELECT ltrim(str1), ltrim(str2, ' ') FROM numeric_tbl;
-- select ltrim (stub function, result)
SELECT ltrim(str1), ltrim(str2, ' ') FROM numeric_tbl;

-- select ltrim (stub function, not pushdown constraints, EXPLAIN (COSTS OFF))
EXPLAIN (COSTS OFF)
SELECT value1, ltrim(str1, '-') FROM numeric_tbl WHERE to_hex(value2) = '64';
-- select ltrim (stub function, not pushdown constraints, result)
SELECT value1, ltrim(str1, '-') FROM numeric_tbl WHERE to_hex(value2) = '64';

-- select ltrim (stub function, pushdown constraints, EXPLAIN (COSTS OFF))
EXPLAIN (COSTS OFF)
SELECT value1, ltrim(str1, '-') FROM numeric_tbl WHERE value2 != 200;
-- select ltrim (stub function, pushdown constraints, result)
SELECT value1, ltrim(str1, '-') FROM numeric_tbl WHERE value2 != 200;

-- select ltrim with non pushdown func and explicit constant (EXPLAIN (COSTS OFF))
EXPLAIN (COSTS OFF)
SELECT ltrim(str1, '-'), pi(), 4.1 FROM numeric_tbl;
-- select ltrim with non pushdown func and explicit constant (result)
SELECT ltrim(str1, '-'), pi(), 4.1 FROM numeric_tbl;

-- select ltrim with order by (EXPLAIN (COSTS OFF))
EXPLAIN (COSTS OFF)
SELECT value1, ltrim(str1, '-') FROM numeric_tbl ORDER BY ltrim(str1, '-');
-- select ltrim with order by (result)
SELECT value1, ltrim(str1, '-') FROM numeric_tbl ORDER BY ltrim(str1, '-');

-- select ltrim with order by index (result)
SELECT value1, ltrim(str1, '-') FROM numeric_tbl ORDER BY 2,1;
-- select ltrim with order by index (result)
SELECT value1, ltrim(str1, '-') FROM numeric_tbl ORDER BY 1,2;

-- select ltrim with group by (EXPLAIN (COSTS OFF))
EXPLAIN (COSTS OFF)
SELECT count(value1), ltrim(str1, '-') FROM numeric_tbl GROUP BY ltrim(str1, '-');
-- select ltrim with group by (result)
SELECT count(value1), ltrim(str1, '-') FROM numeric_tbl GROUP BY ltrim(str1, '-');

-- select ltrim with group by index (result)
SELECT value1, ltrim(str1, '-') FROM numeric_tbl GROUP BY 2,1;

-- select ltrim with group by having (EXPLAIN (COSTS OFF))
EXPLAIN (COSTS OFF)
SELECT count(value1), ltrim(str1, '-') FROM numeric_tbl GROUP BY ltrim(str1, '-'), str2 HAVING ltrim(str1, '-') IS NOT NULL;
-- select ltrim with group by having (result)
SELECT count(value1), ltrim(str1, '-') FROM numeric_tbl GROUP BY ltrim(str1, '-'), str2 HAVING ltrim(str1, '-') IS NOT NULL;

-- select ltrim with group by index having (result)
SELECT value1, ltrim(str2) FROM numeric_tbl GROUP BY 1,2 HAVING value1 > 1;

-- ===============================================================================
-- test mod()
-- ===============================================================================
-- select mod (builtin function, EXPLAIN (COSTS OFF))
EXPLAIN (COSTS OFF)
SELECT value1, mod(value1::numeric, 2), mod(value2::numeric, 2), mod(value3::numeric, 2), mod(value4::numeric, 2), mod(5, 2) FROM numeric_tbl;

-- select mod (builtin function, result)
SELECT value1, mod(value1::numeric, 2), mod(value2::numeric, 2), mod(value3::numeric, 2), mod(value4::numeric, 2), mod(5, 2) FROM numeric_tbl;

-- select mod (builtin function, not pushdown constraints, EXPLAIN (COSTS OFF))
EXPLAIN (COSTS OFF)
SELECT mod(value1::numeric, 2), mod(value2::numeric, 2), mod(value3::numeric, 2), mod(value4::numeric, 2), mod(5, 2) FROM numeric_tbl WHERE to_hex(value2) = '64';

-- select mod (builtin function, not pushdown constraints, result)
SELECT mod(value1::numeric, 2), mod(value2::numeric, 2), mod(value3::numeric, 2), mod(value4::numeric, 2), mod(5, 2) FROM numeric_tbl WHERE to_hex(value2) = '64';

-- select mod (builtin function, pushdown constraints, EXPLAIN (COSTS OFF))
EXPLAIN (COSTS OFF)
SELECT mod(value1::numeric, 2), mod(value2::numeric, 2), mod(value3::numeric, 2), mod(value4::numeric, 2), mod(5, 2) FROM numeric_tbl WHERE value2 != 200;

-- select mod (builtin function, pushdown constraints, result)
SELECT mod(value1::numeric, 2), mod(value2::numeric, 2), mod(value3::numeric, 2), mod(value4::numeric, 2), mod(5, 2) FROM numeric_tbl WHERE value2 != 200;

-- select mod (builtin function, mod in constraints, EXPLAIN (COSTS OFF))
EXPLAIN (COSTS OFF)
SELECT value1, mod(value1::numeric, 2), mod(value2::numeric, 2), mod(value3::numeric, 2), mod(value4::numeric, 2), mod(5, 2) FROM numeric_tbl WHERE mod(value1::numeric, 2) != 1;

-- select mod (builtin function, mod in constraints, result)
SELECT value1, mod(value1::numeric, 2), mod(value2::numeric, 2), mod(value3::numeric, 2), mod(value4::numeric, 2), mod(5, 2) FROM numeric_tbl WHERE mod(value1::numeric, 2) != 1;

-- select mod (builtin function, mod in constraints, EXPLAIN (COSTS OFF))
EXPLAIN (COSTS OFF)
SELECT mod(value1::numeric, 2), mod(value2::numeric, 2), mod(value3::numeric, 2), mod(value4::numeric, 2), mod(5, 2) FROM numeric_tbl WHERE mod(5, 2) > value1;

-- select mod (builtin function, mod in constraints, result)
SELECT mod(value1::numeric, 2), mod(value2::numeric, 2), mod(value3::numeric, 2), mod(value4::numeric, 2), mod(5, 2) FROM numeric_tbl WHERE mod(5, 2) > value1;

-- select mod as nest function with agg (pushdown, EXPLAIN (COSTS OFF))
EXPLAIN (COSTS OFF)
SELECT sum(value3),mod(sum(value3)::numeric, 2) FROM numeric_tbl;

-- select mod as nest function with agg (pushdown, result)
SELECT sum(value3),mod(sum(value3)::numeric, 2) FROM numeric_tbl;

-- select mod as nest with log2 (pushdown, EXPLAIN (COSTS OFF))
EXPLAIN (COSTS OFF)
SELECT value1, mod(log(2, value2)::numeric, 2) FROM numeric_tbl;

-- select mod as nest with log2 (pushdown, result)
SELECT value1, mod(log(2, value2)::numeric, 2) FROM numeric_tbl;

-- select mod with non pushdown func and explicit constant (EXPLAIN (COSTS OFF))
EXPLAIN (COSTS OFF)
SELECT value1, mod(value3::numeric, 2), pi(), 4.1 FROM numeric_tbl;

-- select mod with non pushdown func and explicit constant (result)
SELECT value1, mod(value3::numeric, 2), pi(), 4.1 FROM numeric_tbl;

-- select mod with order by (EXPLAIN (COSTS OFF))
EXPLAIN (COSTS OFF)
SELECT value1, mod((1-value3)::numeric, 2) FROM numeric_tbl ORDER BY mod((1-value3)::numeric, 2);

-- select mod with order by (result)
SELECT value1, mod((1-value3)::numeric, 2) FROM numeric_tbl ORDER BY mod((1-value3)::numeric, 2);

-- select mod with order by index (result)
SELECT value1, mod((1-value3)::numeric, 2) FROM numeric_tbl ORDER BY 2,1;

-- select mod with order by index (result)
SELECT value1, mod((1-value3)::numeric, 2) FROM numeric_tbl ORDER BY 1,2;

-- select mod with group by (EXPLAIN (COSTS OFF))
EXPLAIN (COSTS OFF)
SELECT value1, mod((1-value3)::numeric, 2) FROM numeric_tbl GROUP BY value1, mod((1-value3)::numeric, 2);

-- select mod with group by (result)
SELECT value1, mod((1-value3)::numeric, 2) FROM numeric_tbl GROUP BY value1, mod((1-value3)::numeric, 2);

-- select mod with group by index (result)
SELECT value1, mod((1-value3)::numeric, 2) FROM numeric_tbl GROUP BY 2,1;

-- select mod with group by index (result)
SELECT value1, mod((1-value3)::numeric, 2) FROM numeric_tbl GROUP BY 1,2;

-- select mod with group by having (EXPLAIN (COSTS OFF))
EXPLAIN (COSTS OFF)
SELECT value1, mod((1-value3)::numeric, 2) FROM numeric_tbl GROUP BY value1, mod((1-value3)::numeric, 2) HAVING avg(value1) > 0;

-- select mod with group by having (result)
SELECT value1, mod((1-value3)::numeric, 2) FROM numeric_tbl GROUP BY value1, mod((1-value3)::numeric, 2) HAVING avg(value1) > 0;

-- select mod with group by index having (result)
SELECT value1, mod((1-value3)::numeric, 2) FROM numeric_tbl GROUP BY 1,2 HAVING value1 > 1;

-- select mod and as
SELECT value1, mod(value3::numeric, 2) as mod1 FROM numeric_tbl;

-- ===============================================================================
-- test octet_length()
-- ===============================================================================
-- select octet_length (stub function, EXPLAIN (COSTS OFF))
EXPLAIN (COSTS OFF)
SELECT octet_length(str1), octet_length(str2) FROM numeric_tbl;
-- select octet_length (stub function, result)
SELECT octet_length(str1), octet_length(str2) FROM numeric_tbl;

-- select octet_length (stub function, not pushdown constraints, EXPLAIN (COSTS OFF))
EXPLAIN (COSTS OFF)
SELECT value1, octet_length(str1) FROM numeric_tbl WHERE to_hex(value2) = '64';
-- select octet_length (stub function, not pushdown constraints, result)
SELECT value1, octet_length(str1) FROM numeric_tbl WHERE to_hex(value2) = '64';

-- select octet_length (stub function, pushdown constraints, EXPLAIN (COSTS OFF))
EXPLAIN (COSTS OFF)
SELECT value1, octet_length(str1) FROM numeric_tbl WHERE value2 != 200;
-- select octet_length (stub function, pushdown constraints, result)
SELECT value1, octet_length(str1) FROM numeric_tbl WHERE value2 != 200;

-- select octet_length with non pushdown func and explicit constant (EXPLAIN (COSTS OFF))
EXPLAIN (COSTS OFF)
SELECT octet_length(str1), pi(), 4.1 FROM numeric_tbl;
-- select octet_length with non pushdown func and explicit constant (result)
SELECT octet_length(str1), pi(), 4.1 FROM numeric_tbl;

-- select octet_length with order by (EXPLAIN (COSTS OFF))
EXPLAIN (COSTS OFF)
SELECT value1, octet_length(str1) FROM numeric_tbl ORDER BY octet_length(str1);
-- select octet_length with order by (result)
SELECT value1, octet_length(str1) FROM numeric_tbl ORDER BY octet_length(str1);

-- select octet_length with order by index (result)
SELECT value1, octet_length(str1) FROM numeric_tbl ORDER BY 2,1;
-- select octet_length with order by index (result)
SELECT value1, octet_length(str1) FROM numeric_tbl ORDER BY 1,2;

-- select octet_length with group by (EXPLAIN (COSTS OFF))
EXPLAIN (COSTS OFF)
SELECT count(value1), octet_length(str1) FROM numeric_tbl GROUP BY octet_length(str1);
-- select octet_length with group by (result)
SELECT count(value1), octet_length(str1) FROM numeric_tbl GROUP BY octet_length(str1);

-- select octet_length with group by index (result)
SELECT value1, octet_length(str1) FROM numeric_tbl GROUP BY 2,1;

-- select octet_length with group by having (EXPLAIN (COSTS OFF))
EXPLAIN (COSTS OFF)
SELECT count(value1), octet_length(str1) FROM numeric_tbl GROUP BY octet_length(str1), str1 HAVING octet_length(str1) IS NOT NULL;
-- select octet_length with group by having (result)
SELECT count(value1), octet_length(str1) FROM numeric_tbl GROUP BY octet_length(str1), str1 HAVING octet_length(str1) IS NOT NULL;

-- select octet_length with group by index having (result)
SELECT value1, octet_length(str1) FROM numeric_tbl GROUP BY 1,2 HAVING value1 > 1;

-- ===============================================================================
-- test position()
-- ===============================================================================
-- select position (stub function, EXPLAIN (COSTS OFF))
EXPLAIN (COSTS OFF)
SELECT position('XYZ' IN str1), position('XYZ' IN str2) FROM numeric_tbl;
-- select position (stub function, result)
SELECT position('XYZ' IN str1), position('XYZ' IN str2) FROM numeric_tbl;

-- select position (stub function, not pushdown constraints, EXPLAIN (COSTS OFF))
EXPLAIN (COSTS OFF)
SELECT value1, position('XYZ' IN str1) FROM numeric_tbl WHERE to_hex(value2) = '64';
-- select position (stub function, not pushdown constraints, result)
SELECT value1, position('XYZ' IN str1) FROM numeric_tbl WHERE to_hex(value2) = '64';

-- select position (stub function, pushdown constraints, EXPLAIN (COSTS OFF))
EXPLAIN (COSTS OFF)
SELECT value1, position('XYZ' IN str1) FROM numeric_tbl WHERE value2 != 200;
-- select position (stub function, pushdown constraints, result)
SELECT value1, position('XYZ' IN str1) FROM numeric_tbl WHERE value2 != 200;

-- select position with non pushdown func and explicit constant (EXPLAIN (COSTS OFF))
EXPLAIN (COSTS OFF)
SELECT position('XYZ' IN str1), pi(), 4.1 FROM numeric_tbl;
-- select position with non pushdown func and explicit constant (result)
SELECT position('XYZ' IN str1), pi(), 4.1 FROM numeric_tbl;

-- select position with order by (EXPLAIN (COSTS OFF))
EXPLAIN (COSTS OFF)
SELECT value1, position('XYZ' IN str1) FROM numeric_tbl ORDER BY position('XYZ' IN str1);
-- select position with order by (result)
SELECT value1, position('XYZ' IN str1) FROM numeric_tbl ORDER BY position('XYZ' IN str1);

-- select position with order by index (result)
SELECT value1, position('XYZ' IN str1) FROM numeric_tbl ORDER BY 2,1;
-- select position with order by index (result)
SELECT value1, position('XYZ' IN str1) FROM numeric_tbl ORDER BY 1,2;

-- select position with group by (EXPLAIN (COSTS OFF))
EXPLAIN (COSTS OFF)
SELECT count(value1), position('XYZ' IN str1) FROM numeric_tbl GROUP BY position('XYZ' IN str1);
-- select position with group by (result)
SELECT count(value1), position('XYZ' IN str1) FROM numeric_tbl GROUP BY position('XYZ' IN str1);

-- select position with group by index (result)
SELECT value1, position('XYZ' IN str1) FROM numeric_tbl GROUP BY 2,1;

-- select position with group by having (EXPLAIN (COSTS OFF))
EXPLAIN (COSTS OFF)
SELECT count(value1), position('XYZ' IN str1) FROM numeric_tbl GROUP BY position('XYZ' IN str1), str1 HAVING position('XYZ' IN str1) IS NOT NULL;
-- select position with group by having (result)
SELECT count(value1), position('XYZ' IN str1) FROM numeric_tbl GROUP BY position('XYZ' IN str1), str1 HAVING position('XYZ' IN str1) IS NOT NULL;

-- select position with group by index having (result)
SELECT value1, position('XYZ' IN str1) FROM numeric_tbl GROUP BY 1,2 HAVING value1 > 1;

-- ===============================================================================
-- test pow()
-- ===============================================================================
-- select pow (builtin function, EXPLAIN (COSTS OFF))
EXPLAIN (COSTS OFF)
SELECT pow(value1, 2), pow(value2, 2), pow(value3, 2), pow(value4, 2) FROM numeric_tbl;

-- select pow (builtin function, result)
SELECT pow(value1, 2), pow(value2, 2), pow(value3, 2), pow(value4, 2) FROM numeric_tbl;

-- select pow (builtin function, not pushdown constraints, EXPLAIN (COSTS OFF))
EXPLAIN (COSTS OFF)
SELECT pow(value1, 2), pow(value2, 2), pow(value3, 2), pow(value4, 2) FROM numeric_tbl WHERE to_hex(value2) != '64';

-- select pow (builtin function, not pushdown constraints, result)
SELECT pow(value1, 2), pow(value2, 2), pow(value3, 2), pow(value4, 2) FROM numeric_tbl WHERE to_hex(value2) != '64';

-- select pow (builtin function, pushdown constraints, EXPLAIN (COSTS OFF))
EXPLAIN (COSTS OFF)
SELECT pow(value1, 2), pow(value2, 2), pow(value3, 2), pow(value4, 2) FROM numeric_tbl WHERE value2 != 200;

-- select pow (builtin function, pushdown constraints, result)
SELECT pow(value1, 2), pow(value2, 2), pow(value3, 2), pow(value4, 2) FROM numeric_tbl WHERE value2 != 200;

-- select pow as nest function with agg (pushdown, EXPLAIN (COSTS OFF))
EXPLAIN (COSTS OFF)
SELECT sum(value3),pow(sum(value3), 2) FROM numeric_tbl;

-- select pow as nest function with agg (pushdown, result)
SELECT sum(value3),pow(sum(value3), 2) FROM numeric_tbl;

-- select pow as nest with log2 (pushdown, EXPLAIN (COSTS OFF))
EXPLAIN (COSTS OFF)
SELECT value1, pow(log(2, value2), 2) FROM numeric_tbl;

-- select pow as nest with log2 (pushdown, result)
SELECT value1, pow(log(2, value2), 2) FROM numeric_tbl;

-- select pow with non pushdown func and explicit constant (EXPLAIN (COSTS OFF))
EXPLAIN (COSTS OFF)
SELECT pow(value3, 2), pi(), 4.1 FROM numeric_tbl;

-- select pow with non pushdown func and explicit constant (result)
SELECT pow(value3, 2), pi(), 4.1 FROM numeric_tbl;

-- select pow with order by (EXPLAIN (COSTS OFF))
EXPLAIN (COSTS OFF)
SELECT value3, pow(1-value3, 2) FROM numeric_tbl ORDER BY pow(1-value3, 2);

-- select pow with order by (result)
SELECT value3, pow(1-value3, 2) FROM numeric_tbl ORDER BY pow(1-value3, 2);

-- select pow with order by index (result)
SELECT value3, pow(1-value3, 2) FROM numeric_tbl ORDER BY 2,1;

-- select pow with order by index (result)
SELECT value3, pow(1-value3, 2) FROM numeric_tbl ORDER BY 1,2;

-- select pow and as
SELECT pow(value3, 2) as pow1 FROM numeric_tbl;

-- ===============================================================================
-- test power()
-- ===============================================================================
-- select power (builtin function, EXPLAIN (COSTS OFF))
EXPLAIN (COSTS OFF)
SELECT power(value1, 2), power(value2, 2), power(value3, 2), power(value4, 2), power(5, 2) FROM numeric_tbl;

-- select power (builtin function, result)
SELECT power(value1, 2), power(value2, 2), power(value3, 2), power(value4, 2), power(5, 2) FROM numeric_tbl;

-- select power (builtin function, not pushdown constraints, EXPLAIN (COSTS OFF))
EXPLAIN (COSTS OFF)
SELECT power(value1, 2), power(value2, 2), power(value3, 2), power(value4, 2), power(5, 2) FROM numeric_tbl WHERE to_hex(value2) = '64';

-- select power (builtin function, not pushdown constraints, result)
SELECT power(value1, 2), power(value2, 2), power(value3, 2), power(value4, 2), power(5, 2) FROM numeric_tbl WHERE to_hex(value2) = '64';

-- select power (builtin function, pushdown constraints, EXPLAIN (COSTS OFF))
EXPLAIN (COSTS OFF)
SELECT power(value1, 2), power(value2, 2), power(value3, 2), power(value4, 2), power(5, 2) FROM numeric_tbl WHERE value2 != 200;

-- select power (builtin function, pushdown constraints, result)
SELECT power(value1, 2), power(value2, 2), power(value3, 2), power(value4, 2), power(5, 2) FROM numeric_tbl WHERE value2 != 200;

-- select power (builtin function, power in constraints, EXPLAIN (COSTS OFF))
EXPLAIN (COSTS OFF)
SELECT power(value1, 2), power(value2, 2), power(value3, 2), power(value4, 2), power(5, 2) FROM numeric_tbl WHERE power(value1, 2) != 1;

-- select power (builtin function, power in constraints, result)
SELECT power(value1, 2), power(value2, 2), power(value3, 2), power(value4, 2), power(5, 2) FROM numeric_tbl WHERE power(value1, 2) != 1;

-- select power (builtin function, power in constraints, EXPLAIN (COSTS OFF))
EXPLAIN (COSTS OFF)
SELECT power(value1, 2), power(value2, 2), power(value3, 2), power(value4, 2), power(5, 2) FROM numeric_tbl WHERE power(5, 2) > value1;

-- select power (builtin function, power in constraints, result)
SELECT power(value1, 2), power(value2, 2), power(value3, 2), power(value4, 2), power(5, 2) FROM numeric_tbl WHERE power(5, 2) > value1;

-- select power as nest function with agg (pushdown, EXPLAIN (COSTS OFF))
EXPLAIN (COSTS OFF)
SELECT sum(value3),power(sum(value3), 2) FROM numeric_tbl;

-- select power as nest function with agg (pushdown, result)
SELECT sum(value3),power(sum(value3), 2) FROM numeric_tbl;

-- select power as nest with log2 (pushdown, EXPLAIN (COSTS OFF))
EXPLAIN (COSTS OFF)
SELECT value1, power(log(2, value2), 2) FROM numeric_tbl;

-- select power as nest with log2 (pushdown, result)
SELECT value1, power(log(2, value2), 2) FROM numeric_tbl;

-- select power with non pushdown func and explicit constant (EXPLAIN (COSTS OFF))
EXPLAIN (COSTS OFF)
SELECT power(value3, 2), pi(), 4.1 FROM numeric_tbl;

-- select power with non pushdown func and explicit constant (result)
SELECT power(value3, 2), pi(), 4.1 FROM numeric_tbl;

-- select power with order by (EXPLAIN (COSTS OFF))
EXPLAIN (COSTS OFF)
SELECT value1, power(1-value3, 2) FROM numeric_tbl ORDER BY power(1-value3, 2);

-- select power with order by (result)
SELECT value1, power(1-value3, 2) FROM numeric_tbl ORDER BY power(1-value3, 2);

-- select power with order by index (result)
SELECT value1, power(1-value3, 2) FROM numeric_tbl ORDER BY 2,1;

-- select power with order by index (result)
SELECT value1, power(1-value3, 2) FROM numeric_tbl ORDER BY 1,2;

-- select power with group by (EXPLAIN (COSTS OFF))
EXPLAIN (COSTS OFF)
SELECT count(value1), power(1-value3, 2) FROM numeric_tbl GROUP BY power(1-value3, 2);

-- select power with group by (result)
SELECT count(value1), power(1-value3, 2) FROM numeric_tbl GROUP BY power(1-value3, 2);

-- select power with group by index (result)
SELECT value1, power(1-value3, 2) FROM numeric_tbl GROUP BY 2,1;

-- select power with group by index (result)
SELECT value1, power(1-value3, 2) FROM numeric_tbl GROUP BY 1,2;

-- select power with group by having (EXPLAIN (COSTS OFF))
EXPLAIN (COSTS OFF)
SELECT count(value1), power(1-value3, 2) FROM numeric_tbl GROUP BY power(1-value3, 2) HAVING power(avg(value1), 2) > 0;

-- select power with group by having (result)
SELECT count(value1), power(1-value3, 2) FROM numeric_tbl GROUP BY power(1-value3, 2) HAVING power(avg(value1), 2) > 0;

-- select power with group by index having (result)
SELECT value1, power(1-value3, 2) FROM numeric_tbl GROUP BY 2,1 HAVING power(1-value3, 2) > 0;

-- select power with group by index having (result)
SELECT value1, power(1-value3, 2) FROM numeric_tbl GROUP BY 1,2 HAVING value1 > 1;

-- select power and as
SELECT power(value3, 2) as power1 FROM numeric_tbl;

-- ===============================================================================
-- test replace()
-- ===============================================================================
-- select replace (stub function, EXPLAIN (COSTS OFF))
EXPLAIN (COSTS OFF)
SELECT replace(str1, 'XYZ', 'ABC'), replace(str2, 'XYZ', 'ABC') FROM numeric_tbl;
-- select replace (stub function, result)
SELECT replace(str1, 'XYZ', 'ABC'), replace(str2, 'XYZ', 'ABC') FROM numeric_tbl;

-- select replace (stub function, not pushdown constraints, EXPLAIN (COSTS OFF))
EXPLAIN (COSTS OFF)
SELECT value1, replace(str1, 'XYZ', 'ABC') FROM numeric_tbl WHERE to_hex(value2) = '64';
-- select replace (stub function, not pushdown constraints, result)
SELECT value1, replace(str1, 'XYZ', 'ABC') FROM numeric_tbl WHERE to_hex(value2) = '64';

-- select replace (stub function, pushdown constraints, EXPLAIN (COSTS OFF))
EXPLAIN (COSTS OFF)
SELECT value1, replace(str1, 'XYZ', 'ABC') FROM numeric_tbl WHERE value2 != 200;
-- select replace (stub function, pushdown constraints, result)
SELECT value1, replace(str1, 'XYZ', 'ABC') FROM numeric_tbl WHERE value2 != 200;

-- select replace with non pushdown func and explicit constant (EXPLAIN (COSTS OFF))
EXPLAIN (COSTS OFF)
SELECT replace(str1, 'XYZ', 'ABC'), pi(), 4.1 FROM numeric_tbl;
-- select replace with non pushdown func and explicit constant (result)
SELECT replace(str1, 'XYZ', 'ABC'), pi(), 4.1 FROM numeric_tbl;

-- select replace with order by (EXPLAIN (COSTS OFF))
EXPLAIN (COSTS OFF)
SELECT value1, replace(str1, 'XYZ', 'ABC') FROM numeric_tbl ORDER BY replace(str1, 'XYZ', 'ABC');
-- select replace with order by (result)
SELECT value1, replace(str1, 'XYZ', 'ABC') FROM numeric_tbl ORDER BY replace(str1, 'XYZ', 'ABC');

-- select replace with order by index (result)
SELECT value1, replace(str1, 'XYZ', 'ABC') FROM numeric_tbl ORDER BY 2,1;
-- select replace with order by index (result)
SELECT value1, replace(str1, 'XYZ', 'ABC') FROM numeric_tbl ORDER BY 1,2;

-- select replace with group by (EXPLAIN (COSTS OFF))
EXPLAIN (COSTS OFF)
SELECT count(value1), replace(str1, 'XYZ', 'ABC') FROM numeric_tbl GROUP BY replace(str1, 'XYZ', 'ABC');
-- select replace with group by (result)
SELECT count(value1), replace(str1, 'XYZ', 'ABC') FROM numeric_tbl GROUP BY replace(str1, 'XYZ', 'ABC');

-- select replace with group by index (result)
SELECT value1, replace(str1, 'XYZ', 'ABC') FROM numeric_tbl GROUP BY 2,1;

-- select replace with group by having (EXPLAIN (COSTS OFF))
EXPLAIN (COSTS OFF)
SELECT count(value1), replace(str1, 'XYZ', 'ABC') FROM numeric_tbl GROUP BY replace(str1, 'XYZ', 'ABC'), str1 HAVING replace(str1, 'XYZ', 'ABC') IS NOT NULL;
-- select replace with group by having (result)
SELECT count(value1), replace(str1, 'XYZ', 'ABC') FROM numeric_tbl GROUP BY replace(str1, 'XYZ', 'ABC'), str1 HAVING replace(str1, 'XYZ', 'ABC') IS NOT NULL;

-- select replace with group by index having (result)
SELECT value1, replace(str1, 'XYZ', 'ABC') FROM numeric_tbl GROUP BY 1,2 HAVING value1 > 1;

-- ===============================================================================
-- test round()
-- ===============================================================================
-- select round (builtin function, EXPLAIN (COSTS OFF))
EXPLAIN (COSTS OFF)
SELECT round(value1), round(value2), round(value3), round(value4), round(0.5) FROM numeric_tbl;

-- select round (builtin function, result)
SELECT round(value1), round(value2), round(value3), round(value4), round(0.5) FROM numeric_tbl;

-- select round (builtin function, not pushdown constraints, EXPLAIN (COSTS OFF))
EXPLAIN (COSTS OFF)
SELECT round(value1), round(value2), round(value3), round(value4), round(0.5) FROM numeric_tbl WHERE to_hex(value2) = '64';

-- select round (builtin function, not pushdown constraints, result)
SELECT round(value1), round(value2), round(value3), round(value4), round(0.5) FROM numeric_tbl WHERE to_hex(value2) = '64';

-- select round (builtin function, pushdown constraints, EXPLAIN (COSTS OFF))
EXPLAIN (COSTS OFF)
SELECT round(value1), round(value2), round(value3), round(value4), round(0.5) FROM numeric_tbl WHERE value2 != 200;

-- select round (builtin function, pushdown constraints, result)
SELECT round(value1), round(value2), round(value3), round(value4), round(0.5) FROM numeric_tbl WHERE value2 != 200;

-- select round (builtin function, round in constraints, EXPLAIN (COSTS OFF))
EXPLAIN (COSTS OFF)
SELECT round(value1), round(value2), round(value3), round(value4), round(0.5) FROM numeric_tbl WHERE round(value1) != 1;

-- select round (builtin function, round in constraints, result)
SELECT round(value1), round(value2), round(value3), round(value4), round(0.5) FROM numeric_tbl WHERE round(value1) != 1;

-- select round (builtin function, round in constraints, EXPLAIN (COSTS OFF))
EXPLAIN (COSTS OFF)
SELECT round(value1), round(value2), round(value3), round(value4), round(0.5) FROM numeric_tbl WHERE round(0.5) > value1;

-- select round (builtin function, round in constraints, result)
SELECT round(value1), round(value2), round(value3), round(value4), round(0.5) FROM numeric_tbl WHERE round(0.5) > value1;

-- select round as nest function with agg (pushdown, EXPLAIN (COSTS OFF))
EXPLAIN (COSTS OFF)
SELECT sum(value3),round(sum(value3)) FROM numeric_tbl;

-- select round as nest function with agg (pushdown, result)
SELECT sum(value3),round(sum(value3)) FROM numeric_tbl;

-- select round as nest with log2 (pushdown, EXPLAIN (COSTS OFF))
EXPLAIN (COSTS OFF)
SELECT round(log(2, value2)) FROM numeric_tbl;

-- select round as nest with log2 (pushdown, result)
SELECT round(log(2, value2)) FROM numeric_tbl;

-- select round with non pushdown func and explicit constant (EXPLAIN (COSTS OFF))
EXPLAIN (COSTS OFF)
SELECT round(value3), pi(), 4.1 FROM numeric_tbl;

-- select round with non pushdown func and explicit constant (result)
SELECT round(value3), pi(), 4.1 FROM numeric_tbl;

-- select round with order by (EXPLAIN (COSTS OFF))
EXPLAIN (COSTS OFF)
SELECT value1, round(1-value3) FROM numeric_tbl ORDER BY round(1-value3);

-- select round with order by (result)
SELECT value1, round(1-value3) FROM numeric_tbl ORDER BY round(1-value3);

-- select round with order by index (result)
SELECT value1, round(1-value3) FROM numeric_tbl ORDER BY 2,1;

-- select round with order by index (result)
SELECT value1, round(1-value3) FROM numeric_tbl ORDER BY 1,2;

-- select round with group by (EXPLAIN (COSTS OFF))
EXPLAIN (COSTS OFF)
SELECT count(value1), round(1-value3) FROM numeric_tbl GROUP BY round(1-value3);

-- select round with group by (result)
SELECT count(value1), round(1-value3) FROM numeric_tbl GROUP BY round(1-value3);

-- select round with group by index (result)
SELECT value1, round(1-value3) FROM numeric_tbl GROUP BY 2,1;

-- select round with group by index (result)
SELECT value1, round(1-value3) FROM numeric_tbl GROUP BY 1,2;

-- select round with group by having (EXPLAIN (COSTS OFF))
EXPLAIN (COSTS OFF)
SELECT count(value1), round(1-value3) FROM numeric_tbl GROUP BY round(1-value3) HAVING round(avg(value1)) > 0;

-- select round with group by having (result)
SELECT count(value1), round(1-value3) FROM numeric_tbl GROUP BY round(1-value3) HAVING round(avg(value1)) > 0;

-- select round with group by index having (result)
SELECT value1, round(1-value3) FROM numeric_tbl GROUP BY 2,1 HAVING round(1-value3) > 0;

-- select round with group by index having (result)
SELECT value1, round(1-value3) FROM numeric_tbl GROUP BY 1,2 HAVING value1 > 1;

-- select round and as
SELECT round(value3) as round1 FROM numeric_tbl;

-- ===============================================================================
-- test rpad()
-- ===============================================================================
-- select rpad (stub function, EXPLAIN (COSTS OFF))
EXPLAIN (COSTS OFF)
SELECT rpad(str1, 16, str2), rpad(str1, 4, str2) FROM numeric_tbl;
-- select rpad (stub function, result)
SELECT rpad(str1, 16, str2), rpad(str1, 4, str2) FROM numeric_tbl;

-- select rpad (stub function, not pushdown constraints, EXPLAIN (COSTS OFF))
EXPLAIN (COSTS OFF)
SELECT value1, rpad(str1, 16, str2) FROM numeric_tbl WHERE to_hex(value2) = '64';
-- select rpad (stub function, not pushdown constraints, result)
SELECT value1, rpad(str1, 16, str2) FROM numeric_tbl WHERE to_hex(value2) = '64';

-- select rpad (stub function, pushdown constraints, EXPLAIN (COSTS OFF))
EXPLAIN (COSTS OFF)
SELECT value1, rpad(str1, 16, str2) FROM numeric_tbl WHERE value2 != 200;
-- select rpad (stub function, pushdown constraints, result)
SELECT value1, rpad(str1, 16, str2) FROM numeric_tbl WHERE value2 != 200;

-- select rpad with non pushdown func and explicit constant (EXPLAIN (COSTS OFF))
EXPLAIN (COSTS OFF)
SELECT rpad(str1, 16, str2), pi(), 4.1 FROM numeric_tbl;
-- select rpad with non pushdown func and explicit constant (result)
SELECT rpad(str1, 16, str2), pi(), 4.1 FROM numeric_tbl;

-- select rpad with order by (EXPLAIN (COSTS OFF))
EXPLAIN (COSTS OFF)
SELECT value1, rpad(str1, 16, str2) FROM numeric_tbl ORDER BY rpad(str1, 16, str2);
-- select rpad with order by (result)
SELECT value1, rpad(str1, 16, str2) FROM numeric_tbl ORDER BY rpad(str1, 16, str2);

-- select rpad with order by index (result)
SELECT value1, rpad(str1, 16, str2) FROM numeric_tbl ORDER BY 2,1;
-- select rpad with order by index (result)
SELECT value1, rpad(str1, 16, str2) FROM numeric_tbl ORDER BY 1,2;

-- select rpad with group by (EXPLAIN (COSTS OFF))
EXPLAIN (COSTS OFF)
SELECT count(value1), rpad(str1, 16, str2) FROM numeric_tbl GROUP BY rpad(str1, 16, str2);
-- select rpad with group by (result)
SELECT count(value1), rpad(str1, 16, str2) FROM numeric_tbl GROUP BY rpad(str1, 16, str2);

-- select rpad with group by index (result)
SELECT value1, rpad(str1, 16, str2) FROM numeric_tbl GROUP BY 2,1;

-- select rpad with group by having (EXPLAIN (COSTS OFF))
EXPLAIN (COSTS OFF)
SELECT count(value1), rpad(str1, 16, str2) FROM numeric_tbl GROUP BY rpad(str1, 16, str2) HAVING rpad(str1, 16, str2) IS NOT NULL;
-- select rpad with group by having (result)
SELECT count(value1), rpad(str1, 16, str2) FROM numeric_tbl GROUP BY rpad(str1, 16, str2) HAVING rpad(str1, 16, str2) IS NOT NULL;

-- select rpad with group by index having (result)
SELECT value1, rpad(str1, 16, str2) FROM numeric_tbl GROUP BY 1,2 HAVING value1 > 1;

-- ===============================================================================
-- test rtrim()
-- ===============================================================================
-- select rtrim (stub function, EXPLAIN (COSTS OFF))
EXPLAIN (COSTS OFF)
SELECT rtrim(str1), rtrim(str2, ' ') FROM numeric_tbl;
-- select rtrim (stub function, result)
SELECT rtrim(str1), rtrim(str2, ' ') FROM numeric_tbl;

-- select rtrim (stub function, not pushdown constraints, EXPLAIN (COSTS OFF))
EXPLAIN (COSTS OFF)
SELECT value1, rtrim(str1, '-') FROM numeric_tbl WHERE to_hex(value2) = '64';
-- select rtrim (stub function, not pushdown constraints, result)
SELECT value1, rtrim(str1, '-') FROM numeric_tbl WHERE to_hex(value2) = '64';

-- select rtrim (stub function, pushdown constraints, EXPLAIN (COSTS OFF))
EXPLAIN (COSTS OFF)
SELECT value1, rtrim(str1, '-') FROM numeric_tbl WHERE value2 != 200;
-- select rtrim (stub function, pushdown constraints, result)
SELECT value1, rtrim(str1, '-') FROM numeric_tbl WHERE value2 != 200;

-- select rtrim with non pushdown func and explicit constant (EXPLAIN (COSTS OFF))
EXPLAIN (COSTS OFF)
SELECT rtrim(str1, '-'), pi(), 4.1 FROM numeric_tbl;
-- select rtrim with non pushdown func and explicit constant (result)
SELECT rtrim(str1, '-'), pi(), 4.1 FROM numeric_tbl;

-- select rtrim with order by (EXPLAIN (COSTS OFF))
EXPLAIN (COSTS OFF)
SELECT value1, rtrim(str1, '-') FROM numeric_tbl ORDER BY rtrim(str1, '-');
-- select rtrim with order by (result)
SELECT value1, rtrim(str1, '-') FROM numeric_tbl ORDER BY rtrim(str1, '-');

-- select rtrim with order by index (result)
SELECT value1, rtrim(str1, '-') FROM numeric_tbl ORDER BY 2,1;
-- select rtrim with order by index (result)
SELECT value1, rtrim(str1, '-') FROM numeric_tbl ORDER BY 1,2;

-- select rtrim with group by (EXPLAIN (COSTS OFF))
EXPLAIN (COSTS OFF)
SELECT count(value1), rtrim(str1, '-') FROM numeric_tbl GROUP BY rtrim(str1, '-');
-- select rtrim with group by (result)
SELECT count(value1), rtrim(str1, '-') FROM numeric_tbl GROUP BY rtrim(str1, '-');

-- select rtrim with group by index (result)
SELECT value1, rtrim(str2) FROM numeric_tbl GROUP BY 2,1;

-- select rtrim with group by having (EXPLAIN (COSTS OFF))
EXPLAIN (COSTS OFF)
SELECT count(value1), rtrim(str1, '-') FROM numeric_tbl GROUP BY rtrim(str1, '-') HAVING rtrim(str1, '-') IS NOT NULL;
-- select rtrim with group by having (result)
SELECT count(value1), rtrim(str1, '-') FROM numeric_tbl GROUP BY rtrim(str1, '-') HAVING rtrim(str1, '-') IS NOT NULL;

-- select rtrim with group by index having (result)
SELECT value1, rtrim(str1, '-') FROM numeric_tbl GROUP BY 1,2 HAVING value1 > 1;

-- ===============================================================================
-- test sign()
-- ===============================================================================
-- select sign (builtin function, EXPLAIN (COSTS OFF))
EXPLAIN (COSTS OFF)
SELECT sign(value1), sign(value2), sign(value3), sign(value4), sign(0.5) FROM numeric_tbl;

-- select sign (builtin function, result)
SELECT sign(value1), sign(value2), sign(value3), sign(value4), sign(0.5) FROM numeric_tbl;

-- select sign (builtin function, not pushdown constraints, EXPLAIN (COSTS OFF))
EXPLAIN (COSTS OFF)
SELECT sign(value1), sign(value2), sign(value3), sign(value4), sign(0.5) FROM numeric_tbl WHERE to_hex(value2) = '64';

-- select sign (builtin function, not pushdown constraints, result)
SELECT sign(value1), sign(value2), sign(value3), sign(value4), sign(0.5) FROM numeric_tbl WHERE to_hex(value2) = '64';

-- select sign (builtin function, pushdown constraints, EXPLAIN (COSTS OFF))
EXPLAIN (COSTS OFF)
SELECT sign(value1), sign(value2), sign(value3), sign(value4), sign(0.5) FROM numeric_tbl WHERE value2 != 200;

-- select sign (builtin function, pushdown constraints, result)
SELECT sign(value1), sign(value2), sign(value3), sign(value4), sign(0.5) FROM numeric_tbl WHERE value2 != 200;

-- select sign (builtin function, sign in constraints, EXPLAIN (COSTS OFF))
EXPLAIN (COSTS OFF)
SELECT sign(value1), sign(value2), sign(value3), sign(value4), sign(0.5) FROM numeric_tbl WHERE sign(value1) != -1;

-- select sign (builtin function, sign in constraints, result)
SELECT sign(value1), sign(value2), sign(value3), sign(value4), sign(0.5) FROM numeric_tbl WHERE sign(value1) != -1;

-- select sign (builtin function, sign in constraints, EXPLAIN (COSTS OFF))
EXPLAIN (COSTS OFF)
SELECT sign(value1), sign(value2), sign(value3), sign(value4), sign(0.5) FROM numeric_tbl WHERE sign(0.5) > value1;

-- select sign (builtin function, sign in constraints, result)
SELECT sign(value1), sign(value2), sign(value3), sign(value4), sign(0.5) FROM numeric_tbl WHERE sign(0.5) > value1;

-- select sign as nest function with agg (pushdown, EXPLAIN (COSTS OFF))
EXPLAIN (COSTS OFF)
SELECT sum(value3),sign(sum(value3)) FROM numeric_tbl;

-- select sign as nest function with agg (pushdown, result)
SELECT sum(value3),sign(sum(value3)) FROM numeric_tbl;

-- select sign as nest with log2 (pushdown, EXPLAIN (COSTS OFF))
EXPLAIN (COSTS OFF)
SELECT sign(log(2, value2)) FROM numeric_tbl;

-- select sign as nest with log2 (pushdown, result)
SELECT sign(log(2, value2)) FROM numeric_tbl;

-- select sign with non pushdown func and explicit constant (EXPLAIN (COSTS OFF))
EXPLAIN (COSTS OFF)
SELECT sign(value3), pi(), 4.1 FROM numeric_tbl;

-- select sign with non pushdown func and explicit constant (result)
SELECT sign(value3), pi(), 4.1 FROM numeric_tbl;

-- select sign with order by (EXPLAIN (COSTS OFF))
EXPLAIN (COSTS OFF)
SELECT value1, sign(1-value3) FROM numeric_tbl ORDER BY sign(1-value3);

-- select sign with order by (result)
SELECT value1, sign(1-value3) FROM numeric_tbl ORDER BY sign(1-value3);

-- select sign with order by index (result)
SELECT value1, sign(1-value3) FROM numeric_tbl ORDER BY 2,1;

-- select sign with order by index (result)
SELECT value1, sign(1-value3) FROM numeric_tbl ORDER BY 1,2;

-- select sign with group by (EXPLAIN (COSTS OFF))
EXPLAIN (COSTS OFF)
SELECT count(value1), sign(1-value3) FROM numeric_tbl GROUP BY sign(1-value3);

-- select sign with group by (result)
SELECT count(value1), sign(1-value3) FROM numeric_tbl GROUP BY sign(1-value3);

-- select sign with group by index (result)
SELECT value1, sign(1-value3) FROM numeric_tbl GROUP BY 2,1;

-- select sign with group by index (result)
SELECT value1, sign(1-value3) FROM numeric_tbl GROUP BY 1,2;

-- select sign with group by having (EXPLAIN (COSTS OFF))
EXPLAIN (COSTS OFF)
SELECT count(value1), sign(1-value3) FROM numeric_tbl GROUP BY sign(1-value3) HAVING sign(avg(value1)) > 0;

-- select sign with group by having (result)
SELECT count(value1), sign(1-value3) FROM numeric_tbl GROUP BY sign(1-value3) HAVING sign(avg(value1)) > 0;

-- select sign with group by index having (result)
SELECT value1, sign(1-value3) FROM numeric_tbl GROUP BY 2,1 HAVING sign(1-value3) > 0;

-- select sign with group by index having (result)
SELECT value1, sign(1-value3) FROM numeric_tbl GROUP BY 1,2 HAVING value1 > 1;

-- select sign and as
SELECT sign(value3) as sign1 FROM numeric_tbl;

-- ===============================================================================
-- test sin()
-- ===============================================================================
-- select sin (builtin function, EXPLAIN (COSTS OFF))
EXPLAIN (COSTS OFF)
SELECT value1, sin(value2), sin(value3), sin(value4), sin(0.5) FROM numeric_tbl;

-- select sin (builtin function, result)
SELECT value1, sin(value2), sin(value3), sin(value4), sin(0.5) FROM numeric_tbl;

-- select sin (builtin function, not pushdown constraints, EXPLAIN (COSTS OFF))
EXPLAIN (COSTS OFF)
SELECT sin(value1), sin(value2), sin(value3), sin(value4), sin(0.5) FROM numeric_tbl WHERE to_hex(value2) = '64';

-- select sin (builtin function, not pushdown constraints, result)
SELECT sin(value1), sin(value2), sin(value3), sin(value4), sin(0.5) FROM numeric_tbl WHERE to_hex(value2) = '64';

-- select sin (builtin function, pushdown constraints, EXPLAIN (COSTS OFF))
EXPLAIN (COSTS OFF)
SELECT sin(value1), sin(value2), sin(value3), sin(value4), sin(0.5) FROM numeric_tbl WHERE value2 != 200;

-- select sin (builtin function, pushdown constraints, result)
SELECT sin(value1), sin(value2), sin(value3), sin(value4), sin(0.5) FROM numeric_tbl WHERE value2 != 200;

-- select sin (builtin function, sin in constraints, EXPLAIN (COSTS OFF))
EXPLAIN (COSTS OFF)
SELECT value1, sin(value2), sin(value3), sin(value4), sin(0.5) FROM numeric_tbl WHERE sin(value1) != 1;

-- select sin (builtin function, sin in constraints, result)
SELECT value1, sin(value2), sin(value3), sin(value4), sin(0.5) FROM numeric_tbl WHERE sin(value1) != 1;

-- select sin (builtin function, sin in constraints, EXPLAIN (COSTS OFF))
EXPLAIN (COSTS OFF)
SELECT sin(value1), sin(value2), sin(value3), sin(value4), sin(0.5) FROM numeric_tbl WHERE sin(0.5) > value1;

-- select sin (builtin function, sin in constraints, result)
SELECT sin(value1), sin(value2), sin(value3), sin(value4), sin(0.5) FROM numeric_tbl WHERE sin(0.5) > value1;

-- select sin as nest function with agg (pushdown, EXPLAIN (COSTS OFF))
EXPLAIN (COSTS OFF)
SELECT sum(value3),sin(sum(value3)) FROM numeric_tbl;

-- select sin as nest function with agg (pushdown, result)
SELECT sum(value3),sin(sum(value3)) FROM numeric_tbl;

-- select sin as nest with log2 (pushdown, EXPLAIN (COSTS OFF))
EXPLAIN (COSTS OFF)
SELECT value1, sin(log(2, value2)) FROM numeric_tbl;

-- select sin as nest with log2 (pushdown, result)
SELECT value1, sin(log(2, value2)) FROM numeric_tbl;

-- select sin with non pushdown func and explicit constant (EXPLAIN (COSTS OFF))
EXPLAIN (COSTS OFF)
SELECT value1, sin(value3), pi(), 4.1 FROM numeric_tbl;

-- select sin with non pushdown func and explicit constant (result)
SELECT value1, sin(value3), pi(), 4.1 FROM numeric_tbl;

-- select sin with order by (EXPLAIN (COSTS OFF))
EXPLAIN (COSTS OFF)
SELECT value1, sin(1-value3) FROM numeric_tbl ORDER BY sin(1-value3);

-- select sin with order by (result)
SELECT value1, sin(1-value3) FROM numeric_tbl ORDER BY sin(1-value3);

-- select sin with order by index (result)
SELECT value1, sin(1-value3) FROM numeric_tbl ORDER BY 2,1;

-- select sin with order by index (result)
SELECT value1, sin(1-value3) FROM numeric_tbl ORDER BY 1,2;

-- select sin with group by (EXPLAIN (COSTS OFF))
EXPLAIN (COSTS OFF)
SELECT value1, sin(1-value3) FROM numeric_tbl GROUP BY value1, sin(1-value3);

-- select sin with group by (result)
SELECT value1, sin(1-value3) FROM numeric_tbl GROUP BY value1, sin(1-value3);

-- select sin with group by index (result)
SELECT value1, sin(1-value3) FROM numeric_tbl GROUP BY 2,1;

-- select sin with group by index (result)
SELECT value1, sin(1-value3) FROM numeric_tbl GROUP BY 1,2;

-- select sin with group by having (EXPLAIN (COSTS OFF))
EXPLAIN (COSTS OFF)
SELECT value1, sin(1-value3) FROM numeric_tbl GROUP BY value1, sin(1-value3) HAVING sin(avg(value1)) > 0;

-- select sin with group by having (result)
SELECT value1, sin(1-value3) FROM numeric_tbl GROUP BY value1, sin(1-value3) HAVING sin(avg(value1)) > 0;

-- select sin with group by index having (result)
SELECT value1, sin(1-value3) FROM numeric_tbl GROUP BY 2,1 HAVING sin(1-value3) > 0;

-- select sin with group by index having (result)
SELECT value1, sin(1-value3) FROM numeric_tbl GROUP BY 1,2 HAVING value1 > 1;

-- select sin and as
SELECT value1, sin(value3) as sin1 FROM numeric_tbl;

-- ===============================================================================
-- test sqrt()
-- ===============================================================================
-- select sqrt (builtin function, EXPLAIN (COSTS OFF))
EXPLAIN (COSTS OFF)
SELECT sqrt(value1), sqrt(value2), sqrt(0.5) FROM numeric_tbl;

-- select sqrt (builtin function, result)
SELECT sqrt(value1), sqrt(value2), sqrt(0.5) FROM numeric_tbl;

-- select sqrt (builtin function, not pushdown constraints, EXPLAIN (COSTS OFF))
EXPLAIN (COSTS OFF)
SELECT sqrt(value1), sqrt(value2), sqrt(0.5) FROM numeric_tbl WHERE to_hex(value2) = '64';

-- select sqrt (builtin function, not pushdown constraints, result)
SELECT sqrt(value1), sqrt(value2), sqrt(0.5) FROM numeric_tbl WHERE to_hex(value2) = '64';

-- select sqrt (builtin function, pushdown constraints, EXPLAIN (COSTS OFF))
EXPLAIN (COSTS OFF)
SELECT sqrt(value1), sqrt(value2), sqrt(0.5) FROM numeric_tbl WHERE value2 != 200;

-- select sqrt (builtin function, pushdown constraints, result)
SELECT sqrt(value1), sqrt(value2), sqrt(0.5) FROM numeric_tbl WHERE value2 != 200;

-- select sqrt (builtin function, sqrt in constraints, EXPLAIN (COSTS OFF))
EXPLAIN (COSTS OFF)
SELECT sqrt(value1), sqrt(value2), sqrt(0.5) FROM numeric_tbl WHERE sqrt(value1) != 1;

-- select sqrt (builtin function, sqrt in constraints, result)
SELECT sqrt(value1), sqrt(value2), sqrt(0.5) FROM numeric_tbl WHERE sqrt(value1) != 1;

-- select sqrt (builtin function, sqrt in constraints, EXPLAIN (COSTS OFF))
EXPLAIN (COSTS OFF)
SELECT sqrt(value1), sqrt(value2), sqrt(0.5) FROM numeric_tbl WHERE sqrt(0.5) > value1;

-- select sqrt (builtin function, sqrt in constraints, result)
SELECT sqrt(value1), sqrt(value2), sqrt(0.5) FROM numeric_tbl WHERE sqrt(0.5) > value1;

-- select sqrt as nest function with agg (pushdown, EXPLAIN (COSTS OFF))
EXPLAIN (COSTS OFF)
SELECT sum(value3),sqrt(sum(value1)) FROM numeric_tbl;

-- select sqrt as nest function with agg (pushdown, result)
SELECT sum(value3),sqrt(sum(value1)) FROM numeric_tbl;

-- select sqrt as nest with log2 (pushdown, EXPLAIN (COSTS OFF))
EXPLAIN (COSTS OFF)
SELECT value1, sqrt(log(2, value2)) FROM numeric_tbl;

-- select sqrt as nest with log2 (pushdown, result)
SELECT value1, sqrt(log(2, value2)) FROM numeric_tbl;

-- select sqrt with non pushdown func and explicit constant (EXPLAIN (COSTS OFF))
EXPLAIN (COSTS OFF)
SELECT sqrt(value2), pi(), 4.1 FROM numeric_tbl;

-- select sqrt with non pushdown func and explicit constant (result)
SELECT sqrt(value2), pi(), 4.1 FROM numeric_tbl;

-- select sqrt with order by (EXPLAIN (COSTS OFF))
EXPLAIN (COSTS OFF)
SELECT value1, sqrt(1-value3) FROM numeric_tbl ORDER BY sqrt(1-value3);

-- select sqrt with order by (result)
SELECT value1, sqrt(1-value3) FROM numeric_tbl ORDER BY sqrt(1-value3);

-- select sqrt with order by index (result)
SELECT value1, sqrt(1-value3) FROM numeric_tbl ORDER BY 2,1;

-- select sqrt with order by index (result)
SELECT value1, sqrt(1-value3) FROM numeric_tbl ORDER BY 1,2;

-- select sqrt with group by (EXPLAIN (COSTS OFF))
EXPLAIN (COSTS OFF)
SELECT count(value1), sqrt(1-value3) FROM numeric_tbl GROUP BY sqrt(1-value3);

-- select sqrt with group by (result)
SELECT count(value1), sqrt(1-value3) FROM numeric_tbl GROUP BY sqrt(1-value3);

-- select sqrt with group by index (result)
SELECT value1, sqrt(1-value3) FROM numeric_tbl GROUP BY 2,1;

-- select sqrt with group by index (result)
SELECT value1, sqrt(1-value3) FROM numeric_tbl GROUP BY 1,2;

-- select sqrt with group by having (EXPLAIN (COSTS OFF))
EXPLAIN (COSTS OFF)
SELECT count(value1), sqrt(1-value3) FROM numeric_tbl GROUP BY sqrt(1-value3) HAVING sqrt(avg(value1)) > 0;

-- select sqrt with group by having (result)
SELECT count(value1), sqrt(1-value3) FROM numeric_tbl GROUP BY sqrt(1-value3) HAVING sqrt(avg(value1)) > 0;

-- select sqrt with group by index having (result)
SELECT value1, sqrt(1-value3) FROM numeric_tbl GROUP BY 2,1 HAVING sqrt(1-value3) > 0;

-- select sqrt with group by index having (result)
SELECT value1, sqrt(1-value3) FROM numeric_tbl GROUP BY 1,2 HAVING value1 > 1;

-- select sqrt and as (return null with negative number)
SELECT value1, value3 + 1, sqrt(value1 + 1) as sqrt1 FROM numeric_tbl;

-- ===============================================================================
-- test substr()
-- ===============================================================================
-- select substr (stub function, EXPLAIN (COSTS OFF))
EXPLAIN (COSTS OFF)
SELECT substr(str1, 3), substr(str2, 3, 4) FROM numeric_tbl;
-- select substr (stub function, result)
SELECT substr(str1, 3), substr(str2, 3, 4) FROM numeric_tbl;

-- select substr (stub function, not pushdown constraints, EXPLAIN (COSTS OFF))
EXPLAIN (COSTS OFF)
SELECT value1, substr(str2, 3, 4) FROM numeric_tbl WHERE to_hex(value2) = '64';
-- select substr (stub function, not pushdown constraints, result)
SELECT value1, substr(str2, 3, 4) FROM numeric_tbl WHERE to_hex(value2) = '64';

-- select substr (stub function, pushdown constraints, EXPLAIN (COSTS OFF))
EXPLAIN (COSTS OFF)
SELECT value1, substr(str2, 3, 4) FROM numeric_tbl WHERE value2 != 200;
-- select substr (stub function, pushdown constraints, result)
SELECT value1, substr(str2, 3, 4) FROM numeric_tbl WHERE value2 != 200;

-- select substr with non pushdown func and explicit constant (EXPLAIN (COSTS OFF))
EXPLAIN (COSTS OFF)
SELECT substr(str2, 3, 4), pi(), 4.1 FROM numeric_tbl;
-- select substr with non pushdown func and explicit constant (result)
SELECT substr(str2, 3, 4), pi(), 4.1 FROM numeric_tbl;

-- select substr with order by (EXPLAIN (COSTS OFF))
EXPLAIN (COSTS OFF)
SELECT value1, substr(str2, 3, 4) FROM numeric_tbl ORDER BY substr(str2, 3, 4);
-- select substr with order by (result)
SELECT value1, substr(str2, 3, 4) FROM numeric_tbl ORDER BY substr(str2, 3, 4);

-- select substr with order by index (result)
SELECT value1, substr(str2, 3, 4) FROM numeric_tbl ORDER BY 2,1;
-- select substr with order by index (result)
SELECT value1, substr(str2, 3, 4) FROM numeric_tbl ORDER BY 1,2;

-- select substr with group by (EXPLAIN (COSTS OFF))
EXPLAIN (COSTS OFF)
SELECT count(value1), substr(str2, 3, 4) FROM numeric_tbl GROUP BY substr(str2, 3, 4);
-- select substr with group by (result)
SELECT count(value1), substr(str2, 3, 4) FROM numeric_tbl GROUP BY substr(str2, 3, 4);

-- select substr with group by index (result)
SELECT value1, substr(str2, 3, 4) FROM numeric_tbl GROUP BY 2,1;

-- select substr with group by having (EXPLAIN (COSTS OFF))
EXPLAIN (COSTS OFF)
SELECT count(value1), substr(str2, 3, 4) FROM numeric_tbl GROUP BY substr(str2, 3, 4), str2 HAVING substr(str2, 3, 4) IS NOT NULL;
-- select substr with group by having (result)
SELECT count(value1), substr(str2, 3, 4) FROM numeric_tbl GROUP BY substr(str2, 3, 4), str2 HAVING substr(str2, 3, 4) IS NOT NULL;

-- select substr with group by index having (result)
SELECT value1, substr(str2, 3, 4) FROM numeric_tbl GROUP BY 1,2 HAVING value1 > 1;

-- ===============================================================================
-- test substring()
-- ===============================================================================
-- select substring (stub function, EXPLAIN (COSTS OFF))
EXPLAIN (COSTS OFF)
SELECT substring(str1, 3), substring(str2, 3, 4) FROM numeric_tbl;
-- select substring (stub function, result)
SELECT substring(str1, 3), substring(str2, 3, 4) FROM numeric_tbl;

-- select substring (stub function, EXPLAIN (COSTS OFF))
EXPLAIN (COSTS OFF)
SELECT substring(str1 FROM 3), substring(str2 FROM 3 FOR 4) FROM numeric_tbl;
-- select substring (stub function, result)
SELECT substring(str1 FROM 3), substring(str2 FROM 3 FOR 4) FROM numeric_tbl;

-- select substring (stub function, not pushdown constraints, EXPLAIN (COSTS OFF))
EXPLAIN (COSTS OFF)
SELECT value1, substring(str2, 3, 4) FROM numeric_tbl WHERE to_hex(value2) = '64';
-- select substring (stub function, not pushdown constraints, result)
SELECT value1, substring(str2, 3, 4) FROM numeric_tbl WHERE to_hex(value2) = '64';

-- select substring (stub function, pushdown constraints, EXPLAIN (COSTS OFF))
EXPLAIN (COSTS OFF)
SELECT value1, substring(str2 FROM 3 FOR 4) FROM numeric_tbl WHERE value2 != 200;
-- select substring (stub function, pushdown constraints, result)
SELECT value1, substring(str2 FROM 3 FOR 4) FROM numeric_tbl WHERE value2 != 200;

-- select substring with non pushdown func and explicit constant (EXPLAIN (COSTS OFF))
EXPLAIN (COSTS OFF)
SELECT substring(str2 FROM 3 FOR 4), pi(), 4.1 FROM numeric_tbl;
-- select substring with non pushdown func and explicit constant (result)
SELECT substring(str2 FROM 3 FOR 4), pi(), 4.1 FROM numeric_tbl;

-- select substring with order by (EXPLAIN (COSTS OFF))
EXPLAIN (COSTS OFF)
SELECT value1, substring(str2 FROM 3 FOR 4) FROM numeric_tbl ORDER BY substring(str2 FROM 3 FOR 4);
-- select substring with order by (result)
SELECT value1, substring(str2 FROM 3 FOR 4) FROM numeric_tbl ORDER BY substring(str2 FROM 3 FOR 4);

-- select substring with order by index (result)
SELECT value1, substring(str2 FROM 3 FOR 4) FROM numeric_tbl ORDER BY 2,1;
-- select substring with order by index (result)
SELECT value1, substring(str2 FROM 3 FOR 4) FROM numeric_tbl ORDER BY 1,2;

-- select substring with group by (EXPLAIN (COSTS OFF))
EXPLAIN (COSTS OFF)
SELECT count(value1), substring(str2, 3, 4) FROM numeric_tbl GROUP BY substring(str2, 3, 4);
-- select substring with group by (result)
SELECT count(value1), substring(str2, 3, 4) FROM numeric_tbl GROUP BY substring(str2, 3, 4);

-- select substring with group by index (result)
SELECT value1, substring(str2, 3, 4) FROM numeric_tbl GROUP BY 2,1;

-- select substring with group by having (EXPLAIN (COSTS OFF))
EXPLAIN (COSTS OFF)
SELECT count(value1), substring(str2, 3, 4) FROM numeric_tbl GROUP BY substring(str2, 3, 4), str2 HAVING substring(str2, 3, 4) IS NOT NULL;
-- select substring with group by having (result)
SELECT count(value1), substring(str2, 3, 4) FROM numeric_tbl GROUP BY substring(str2, 3, 4), str2 HAVING substring(str2, 3, 4) IS NOT NULL;

-- select substring with group by index having (result)
SELECT value1, substring(str2, 3, 4) FROM numeric_tbl GROUP BY 1,2 HAVING value1 > 1;

-- ===============================================================================
-- test tan()
-- ===============================================================================
-- select tan (builtin function, EXPLAIN (COSTS OFF))
EXPLAIN (COSTS OFF)
SELECT value1, tan(value2), tan(value3), tan(value4), tan(0.5) FROM numeric_tbl;

-- select tan (builtin function, result)
SELECT value1, tan(value2), tan(value3), tan(value4), tan(0.5) FROM numeric_tbl;

-- select tan (builtin function, not pushdown constraints, EXPLAIN (COSTS OFF))
EXPLAIN (COSTS OFF)
SELECT tan(value1), tan(value2), tan(value3), tan(value4), tan(0.5) FROM numeric_tbl WHERE to_hex(value2) = '64';

-- select tan (builtin function, not pushdown constraints, result)
SELECT tan(value1), tan(value2), tan(value3), tan(value4), tan(0.5) FROM numeric_tbl WHERE to_hex(value2) = '64';

-- select tan (builtin function, pushdown constraints, EXPLAIN (COSTS OFF))
EXPLAIN (COSTS OFF)
SELECT tan(value1), tan(value2), tan(value3), tan(value4), tan(0.5) FROM numeric_tbl WHERE value2 != 200;

-- select tan (builtin function, pushdown constraints, result)
SELECT tan(value1), tan(value2), tan(value3), tan(value4), tan(0.5) FROM numeric_tbl WHERE value2 != 200;

-- select tan (builtin function, tan in constraints, EXPLAIN (COSTS OFF))
EXPLAIN (COSTS OFF)
SELECT value1, tan(value2), tan(value3), tan(value4), tan(0.5) FROM numeric_tbl WHERE tan(value1) != 1;

-- select tan (builtin function, tan in constraints, result)
SELECT value1, tan(value2), tan(value3), tan(value4), tan(0.5) FROM numeric_tbl WHERE tan(value1) != 1;

-- select tan (builtin function, tan in constraints, EXPLAIN (COSTS OFF))
EXPLAIN (COSTS OFF)
SELECT tan(value1), tan(value2), tan(value3), tan(value4), tan(0.5) FROM numeric_tbl WHERE tan(0.5) > value1;

-- select tan (builtin function, tan in constraints, result)
SELECT tan(value1), tan(value2), tan(value3), tan(value4), tan(0.5) FROM numeric_tbl WHERE tan(0.5) > value1;

-- select tan as nest function with agg (pushdown, EXPLAIN (COSTS OFF))
EXPLAIN (COSTS OFF)
SELECT sum(value3),tan(sum(value3)) FROM numeric_tbl;

-- select tan as nest function with agg (pushdown, result)
SELECT sum(value3),tan(sum(value3)) FROM numeric_tbl;

-- select tan as nest with log2 (pushdown, EXPLAIN (COSTS OFF))
EXPLAIN (COSTS OFF)
SELECT value1, tan(log(2, value2)) FROM numeric_tbl;

-- select tan as nest with log2 (pushdown, result)
SELECT value1, tan(log(2, value2)) FROM numeric_tbl;

-- select tan with non pushdown func and explicit constant (EXPLAIN (COSTS OFF))
EXPLAIN (COSTS OFF)
SELECT value1, tan(value3), pi(), 4.1 FROM numeric_tbl;

-- select tan with non pushdown func and explicit constant (result)
SELECT value1, tan(value3), pi(), 4.1 FROM numeric_tbl;

-- select tan with order by (EXPLAIN (COSTS OFF))
EXPLAIN (COSTS OFF)
SELECT value1, tan(1-value3) FROM numeric_tbl ORDER BY tan(1-value3);

-- select tan with order by (result)
SELECT value1, tan(1-value3) FROM numeric_tbl ORDER BY tan(1-value3);

-- select tan with order by index (result)
SELECT value1, tan(1-value3) FROM numeric_tbl ORDER BY 2,1;

-- select tan with order by index (result)
SELECT value1, tan(1-value3) FROM numeric_tbl ORDER BY 1,2;

-- select tan with group by (EXPLAIN (COSTS OFF))
EXPLAIN (COSTS OFF)
SELECT value1, tan(1-value3) FROM numeric_tbl GROUP BY value1, tan(1-value3);

-- select tan with group by (result)
SELECT value1, tan(1-value3) FROM numeric_tbl GROUP BY value1, tan(1-value3);

-- select tan with group by index (result)
SELECT value1, tan(1-value3) FROM numeric_tbl GROUP BY 2,1;

-- select tan with group by index (result)
SELECT value1, tan(1-value3) FROM numeric_tbl GROUP BY 1,2;

-- select tan with group by having (EXPLAIN (COSTS OFF))
EXPLAIN (COSTS OFF)
SELECT value1, tan(1-value3) FROM numeric_tbl GROUP BY value1, tan(1-value3) HAVING tan(avg(value1)) > 0;

-- select tan with group by having (result)
SELECT value1, tan(1-value3) FROM numeric_tbl GROUP BY value1, tan(1-value3) HAVING tan(avg(value1)) > 0;

-- select tan with group by index having (result)
SELECT value1, tan(1-value3) FROM numeric_tbl GROUP BY 2,1 HAVING tan(1-value3) > 0;

-- select tan with group by index having (result)
SELECT value1, tan(1-value3) FROM numeric_tbl GROUP BY 1,2 HAVING value1 > 1;

-- select tan and as
SELECT value1, tan(value3) as tan1 FROM numeric_tbl;

-- ===============================================================================
-- test upper()
-- ===============================================================================
-- select upper (stub function, EXPLAIN (COSTS OFF))
EXPLAIN (COSTS OFF)
SELECT upper(tag1), upper(str1), upper(str2) FROM numeric_tbl;
-- select upper (stub function, result)
SELECT upper(tag1), upper(str1), upper(str2) FROM numeric_tbl;

-- select upper (stub function, not pushdown constraints, EXPLAIN (COSTS OFF))
EXPLAIN (COSTS OFF)
SELECT value1, upper(tag1) FROM numeric_tbl WHERE to_hex(value2) = '64';
-- select upper (stub function, not pushdown constraints, result)
SELECT value1, upper(tag1) FROM numeric_tbl WHERE to_hex(value2) = '64';

-- select upper (stub function, pushdown constraints, EXPLAIN (COSTS OFF))
EXPLAIN (COSTS OFF)
SELECT value1, upper(str1) FROM numeric_tbl WHERE value2 != 200;
-- select upper (stub function, pushdown constraints, result)
SELECT value1, upper(str1) FROM numeric_tbl WHERE value2 != 200;

-- select upper with non pushdown func and explicit constant (EXPLAIN (COSTS OFF))
EXPLAIN (COSTS OFF)
SELECT upper(str1), pi(), 4.1 FROM numeric_tbl;
-- select ucase with non pushdown func and explicit constant (result)
SELECT upper(str1), pi(), 4.1 FROM numeric_tbl;

-- select upper with order by (EXPLAIN (COSTS OFF))
EXPLAIN (COSTS OFF)
SELECT value1, upper(str1) FROM numeric_tbl ORDER BY upper(str1);
-- select upper with order by (result)
SELECT value1, upper(str1) FROM numeric_tbl ORDER BY upper(str1);

-- select upper with order by index (result)
SELECT value1, upper(str1) FROM numeric_tbl ORDER BY 2,1;
-- select upper with order by index (result)
SELECT value1, upper(str1) FROM numeric_tbl ORDER BY 1,2;

-- select upper with group by (EXPLAIN (COSTS OFF))
EXPLAIN (COSTS OFF)
SELECT count(value1), upper(str1) FROM numeric_tbl GROUP BY upper(str1);
-- select upper with group by (result)
SELECT count(value1), upper(str1) FROM numeric_tbl GROUP BY upper(str1);

-- select upper with group by index (result)
SELECT value1, upper(str1) FROM numeric_tbl GROUP BY 2,1;

-- select upper with group by having (EXPLAIN (COSTS OFF))
EXPLAIN (COSTS OFF)
SELECT count(value1), upper(str1) FROM numeric_tbl GROUP BY upper(str1), tag1 HAVING upper(str1) IS NOT NULL;
-- select upper with group by having (result)
SELECT count(value1), upper(str1) FROM numeric_tbl GROUP BY upper(str1), tag1 HAVING upper(str1) IS NOT NULL;

-- select upper with group by index having (result)
SELECT value1, upper(tag1) FROM numeric_tbl GROUP BY 1,2 HAVING value1 > 1;

-- ===============================================================================
-- test log()
-- ===============================================================================
-- select log (builtin function, numeric cast, EXPLAIN (COSTS OFF))
-- log_<base>(v) : postgresql (base, v), mysql (base, v)
EXPLAIN (COSTS OFF)
SELECT value1, log(value1::numeric, value2::numeric) FROM numeric_tbl WHERE value1 != 1;

-- select log (builtin function, numeric cast, result)
SELECT value1, log(value1::numeric, value2::numeric) FROM numeric_tbl WHERE value1 != 1;

-- select log (builtin function,  float8, EXPLAIN (COSTS OFF))
EXPLAIN (COSTS OFF)
SELECT value1, log(value1::numeric, 0.1) FROM numeric_tbl WHERE value1 != 1;

-- select log (builtin function, float8, result)
SELECT value1, log(value1::numeric, 0.1) FROM numeric_tbl WHERE value1 != 1;

-- select log (builtin function, bigint, EXPLAIN (COSTS OFF))
EXPLAIN (COSTS OFF)
SELECT value1, log(value2::numeric, 3) FROM numeric_tbl WHERE value1 != 1;

-- select log (builtin function, bigint, result)
SELECT value1, log(value2::numeric, 3) FROM numeric_tbl WHERE value1 != 1;

-- select log (builtin function, mix type, EXPLAIN (COSTS OFF))
EXPLAIN (COSTS OFF)
SELECT value1, log(value1::numeric, value2::numeric) FROM numeric_tbl WHERE value1 != 1;

-- select log (builtin function,  mix type, result)
SELECT value1, log(value1::numeric, value2::numeric) FROM numeric_tbl WHERE value1 != 1;

-- select log(v) -- built in function
-- log(v): postgreSQL base 10 logarithm
EXPLAIN (COSTS OFF)
SELECT log(10, value2) FROM numeric_tbl WHERE value1 != 1;
SELECT log(10, value2) FROM numeric_tbl WHERE value1 != 1;

-- select log (builtin function, EXPLAIN (COSTS OFF))
EXPLAIN (COSTS OFF)
SELECT log(10, value2), log(10, value2), log(0.5) FROM numeric_tbl;

-- select log (builtin function, result)
SELECT log(10, value2), log(10, value2), log(0.5) FROM numeric_tbl;

-- select log (builtin function, not pushdown constraints, EXPLAIN (COSTS OFF))
EXPLAIN (COSTS OFF)
SELECT log(10, value2), log(10, value2), log(0.5) FROM numeric_tbl WHERE to_hex(value2) = '64';

-- select log (builtin function, not pushdown constraints, result)
SELECT log(10, value2), log(10, value2), log(0.5) FROM numeric_tbl WHERE to_hex(value2) = '64';

-- select log (builtin function, pushdown constraints, EXPLAIN (COSTS OFF))
EXPLAIN (COSTS OFF)
SELECT log(10, value2), log(10, value2), log(0.5) FROM numeric_tbl WHERE value2 != 200;

-- select log (builtin function, pushdown constraints, result)
SELECT log(10, value2), log(10, value2), log(0.5) FROM numeric_tbl WHERE value2 != 200;

-- select log (builtin function, log in constraints, EXPLAIN (COSTS OFF))
EXPLAIN (COSTS OFF)
SELECT log(10, value2), log(10, value2), log(0.5) FROM numeric_tbl WHERE log(10, value2) != 1;

-- select log (builtin function, log in constraints, result)
SELECT log(10, value2), log(10, value2), log(0.5) FROM numeric_tbl WHERE log(10, value2) != 1;

-- select log (builtin function, log in constraints, EXPLAIN (COSTS OFF))
EXPLAIN (COSTS OFF)
SELECT log(10, value2), log(10, value2), log(0.5) FROM numeric_tbl WHERE log(5) > value1;

-- select log (builtin function, log in constraints, result)
SELECT log(10, value2), log(10, value2), log(0.5) FROM numeric_tbl WHERE log(5) > value1;

-- select log as nest function with agg (pushdown, EXPLAIN (COSTS OFF))
EXPLAIN (COSTS OFF)
SELECT sum(value3),log(10, sum(value2)) FROM numeric_tbl;

-- select log as nest function with agg (pushdown, result)
SELECT sum(value3),log(10, sum(value2)) FROM numeric_tbl;

-- select log as nest with log2 (pushdown, EXPLAIN (COSTS OFF))
EXPLAIN (COSTS OFF)
SELECT value1, log(log(2, value2)) FROM numeric_tbl;

-- select log as nest with log2 (pushdown, result)
SELECT value1, log(log(2, value2)) FROM numeric_tbl;

-- select log with non pushdown func and explicit constant (EXPLAIN (COSTS OFF))
EXPLAIN (COSTS OFF)
SELECT log(10, value2), pi(), 4.1 FROM numeric_tbl;

-- select log with non pushdown func and explicit constant (result)
SELECT log(10, value2), pi(), 4.1 FROM numeric_tbl;

-- select log with order by (EXPLAIN (COSTS OFF))
EXPLAIN (COSTS OFF)
SELECT value3, log(10, value2) FROM numeric_tbl ORDER BY log(10, value2);

-- select log with order by (result)
SELECT value3, log(10, value2) FROM numeric_tbl ORDER BY log(10, value2);

-- select log with order by index (result)
SELECT value3, log(10, value2) FROM numeric_tbl ORDER BY 2,1;

-- select log with order by index (result)
SELECT value3, log(10, value2) FROM numeric_tbl ORDER BY 1,2;

-- select log with group by (EXPLAIN (COSTS OFF))
EXPLAIN (COSTS OFF)
SELECT count(value1), log(10, value2) FROM numeric_tbl GROUP BY log(10, value2);

-- select log with group by (result)
SELECT count(value1), log(10, value2) FROM numeric_tbl GROUP BY log(10, value2);

-- select log with group by index (result)
SELECT value1, log(10, value2) FROM numeric_tbl GROUP BY 2,1;

-- select log with group by index (result)
SELECT value1, log(10, value2) FROM numeric_tbl GROUP BY 1,2;

-- select log with group by having (EXPLAIN (COSTS OFF))
EXPLAIN (COSTS OFF)
SELECT count(value1), log(10, value2) FROM numeric_tbl GROUP BY log(10, value2) HAVING log(10, avg(value2)) > 0;

-- select log with group by having (result)
SELECT count(value1), log(10, value2) FROM numeric_tbl GROUP BY log(10, value2) HAVING log(10, avg(value2)) > 0;

-- select log with group by index having (result)
SELECT value3, log(10, value2) FROM numeric_tbl GROUP BY 2,1 HAVING log(10, value2) < 0;

-- select log with group by index having (result)
SELECT value3, log(10, value2) FROM numeric_tbl GROUP BY 1,2 HAVING value3 > 1;

-- select log and as
SELECT log(10, value2) as log1 FROM numeric_tbl;

-- ===============================================================================
-- test ln()
-- ===============================================================================
-- select ln as nest function with agg (pushdown, EXPLAIN (COSTS OFF))
EXPLAIN (COSTS OFF)
SELECT sum(value3),ln(sum(value1)) FROM numeric_tbl;

-- select ln as nest function with agg (pushdown, result)
SELECT sum(value3),ln(sum(value1)) FROM numeric_tbl;

-- select ln as nest with log2 (pushdown, EXPLAIN (COSTS OFF))
EXPLAIN (COSTS OFF)
SELECT value1, ln(log(2, value2)) FROM numeric_tbl;

-- select ln as nest with log2 (pushdown, result)
SELECT value1, ln(log(2, value2)) FROM numeric_tbl;

-- select ln with non pushdown func and explicit constant (EXPLAIN (COSTS OFF))
EXPLAIN (COSTS OFF)
SELECT ln(value2), pi(), 4.1 FROM numeric_tbl;

-- select ln with non pushdown func and explicit constant (result)
SELECT ln(value2), pi(), 4.1 FROM numeric_tbl;

-- select ln with order by (EXPLAIN (COSTS OFF))
EXPLAIN (COSTS OFF)
SELECT value1, ln(1-value3) FROM numeric_tbl ORDER BY ln(1-value3);

-- select ln with order by (result)
SELECT value1, ln(1-value3) FROM numeric_tbl ORDER BY ln(1-value3);

-- select ln with order by index (result)
SELECT value1, ln(1-value3) FROM numeric_tbl ORDER BY 2,1;

-- select ln with order by index (result)
SELECT value1, ln(1-value3) FROM numeric_tbl ORDER BY 1,2;

-- select ln with group by (EXPLAIN (COSTS OFF))
EXPLAIN (COSTS OFF)
SELECT count(value1), ln(1-value3) FROM numeric_tbl GROUP BY ln(1-value3);

-- select ln with group by (result)
SELECT count(value1), ln(1-value3) FROM numeric_tbl GROUP BY ln(1-value3);

-- select ln with group by index (result)
SELECT value1, ln(1-value3) FROM numeric_tbl GROUP BY 2,1;

-- select ln with group by index (result)
SELECT value1, ln(1-value3) FROM numeric_tbl GROUP BY 1,2;

-- select ln with group by having (EXPLAIN (COSTS OFF))
EXPLAIN (COSTS OFF)
SELECT count(value1), ln(1-value3) FROM numeric_tbl GROUP BY ln(1-value3) HAVING ln(avg(value1)) > 0;

-- select ln with group by having (result)
SELECT count(value1), ln(1-value3) FROM numeric_tbl GROUP BY ln(1-value3) HAVING ln(avg(value1)) > 0;

-- select ln with group by index having (result)
SELECT value1, ln(1-value3) FROM numeric_tbl GROUP BY 2,1 HAVING ln(1-value3) < 0;

-- select ln with group by index having (result)
SELECT value1, ln(1-value3) FROM numeric_tbl GROUP BY 1,2 HAVING value1 > 1;

-- select ln and as
SELECT ln(value1) as ln1 FROM numeric_tbl;

-- select ln (builtin function, EXPLAIN (COSTS OFF))
EXPLAIN (COSTS OFF)
SELECT ln(value1), ln(value2), ln(value3 + 10), ln(0.5) FROM numeric_tbl;

-- select ln (builtin function, result)
SELECT ln(value1), ln(value2), ln(value3 + 10), ln(0.5) FROM numeric_tbl;

-- select ln (builtin function, not pushdown constraints, EXPLAIN (COSTS OFF))
EXPLAIN (COSTS OFF)
SELECT ln(value1), ln(value2), ln(value3 + 10), ln(0.5) FROM numeric_tbl WHERE to_hex(value2) = '64';

-- select ln (builtin function, not pushdown constraints, result)
SELECT ln(value1), ln(value2), ln(value3 + 10), ln(0.5) FROM numeric_tbl WHERE to_hex(value2) = '64';

-- select ln (builtin function, pushdown constraints, EXPLAIN (COSTS OFF))
EXPLAIN (COSTS OFF)
SELECT ln(value1), ln(value2), ln(value3 + 10), ln(0.5) FROM numeric_tbl WHERE value2 != 200;

-- select ln (builtin function, pushdown constraints, result)
SELECT ln(value1), ln(value2), ln(value3 + 10), ln(0.5) FROM numeric_tbl WHERE value2 != 200;

-- select ln (builtin function, ln in constraints, EXPLAIN (COSTS OFF))
EXPLAIN (COSTS OFF)
SELECT ln(value1), ln(value2), ln(value3 + 10), ln(0.5) FROM numeric_tbl WHERE ln(value1) != 1;

-- select ln (builtin function, ln in constraints, result)
SELECT ln(value1), ln(value2), ln(value3 + 10), ln(0.5) FROM numeric_tbl WHERE ln(value1) != 1;

-- select ln (builtin function, ln in constraints, EXPLAIN (COSTS OFF))
EXPLAIN (COSTS OFF)
SELECT ln(value1), ln(value2), ln(value3 + 10), ln(0.5) FROM numeric_tbl WHERE ln(0.5) < value1;

-- select ln (builtin function, ln in constraints, result)
SELECT ln(value1), ln(value2), ln(value3 + 10), ln(0.5) FROM numeric_tbl WHERE ln(0.5) < value1;

-- ===============================================================================
-- test floor()
-- ===============================================================================
-- select floor (builtin function, EXPLAIN (COSTS OFF))
EXPLAIN (COSTS OFF)
SELECT floor(value1), floor(value2), floor(value3), floor(value4), floor(1.5) FROM numeric_tbl;

-- select floor (builtin function, result)
SELECT floor(value1), floor(value2), floor(value3), floor(value4), floor(1.5) FROM numeric_tbl;

-- select floor (builtin function, not pushdown constraints, EXPLAIN (COSTS OFF))
EXPLAIN (COSTS OFF)
SELECT floor(value1), floor(value2), floor(value3), floor(value4), floor(1.5) FROM numeric_tbl WHERE to_hex(value2) = '64';

-- select floor (builtin function, not pushdown constraints, result)
SELECT floor(value1), floor(value2), floor(value3), floor(value4), floor(1.5) FROM numeric_tbl WHERE to_hex(value2) = '64';

-- select floor (builtin function, pushdown constraints, EXPLAIN (COSTS OFF))
EXPLAIN (COSTS OFF)
SELECT floor(value1), floor(value2), floor(value3), floor(value4), floor(1.5) FROM numeric_tbl WHERE value2 != 200;

-- select floor (builtin function, pushdown constraints, result)
SELECT floor(value1), floor(value2), floor(value3), floor(value4), floor(1.5) FROM numeric_tbl WHERE value2 != 200;

-- select floor (builtin function, floor in constraints, EXPLAIN (COSTS OFF))
EXPLAIN (COSTS OFF)
SELECT floor(value1), floor(value2), floor(value3), floor(value4), floor(1.5) FROM numeric_tbl WHERE floor(value1) != 1;

-- select floor (builtin function, floor in constraints, result)
SELECT floor(value1), floor(value2), floor(value3), floor(value4), floor(1.5) FROM numeric_tbl WHERE floor(value1) != 1;

-- select floor (builtin function, floor in constraints, EXPLAIN (COSTS OFF))
EXPLAIN (COSTS OFF)
SELECT floor(value1), floor(value2), floor(value3), floor(value4), floor(1.5) FROM numeric_tbl WHERE floor(1.5) > value1;

-- select floor (builtin function, floor in constraints, result)
SELECT floor(value1), floor(value2), floor(value3), floor(value4), floor(1.5) FROM numeric_tbl WHERE floor(1.5) > value1;

-- select floor as nest function with agg (pushdown, EXPLAIN (COSTS OFF))
EXPLAIN (COSTS OFF)
SELECT sum(value3),floor(sum(value3)) FROM numeric_tbl;

-- select floor as nest function with agg (pushdown, result)
SELECT sum(value3),floor(sum(value3)) FROM numeric_tbl;

-- select floor as nest with log2 (pushdown, EXPLAIN (COSTS OFF))
EXPLAIN (COSTS OFF)
SELECT floor(log(2, value2)) FROM numeric_tbl;

-- select floor as nest with log2 (pushdown, result)
SELECT floor(log(2, value2)) FROM numeric_tbl;

-- select floor with non pushdown func and explicit constant (EXPLAIN (COSTS OFF))
EXPLAIN (COSTS OFF)
SELECT floor(value3), pi(), 4.1 FROM numeric_tbl;

-- select floor with non pushdown func and explicit constant (result)
SELECT floor(value3), pi(), 4.1 FROM numeric_tbl;

-- select floor with order by (EXPLAIN (COSTS OFF))
EXPLAIN (COSTS OFF)
SELECT value1, floor(10 - value1) FROM numeric_tbl ORDER BY floor(10 - value1);

-- select floor with order by (result)
SELECT value1, floor(10 - value1) FROM numeric_tbl ORDER BY floor(10 - value1);

-- select floor with order by index (result)
SELECT value1, floor(10 - value1) FROM numeric_tbl ORDER BY 2,1;

-- select floor with order by index (result)
SELECT value1, floor(10 - value1) FROM numeric_tbl ORDER BY 1,2;

-- select floor with group by (EXPLAIN (COSTS OFF))
EXPLAIN (COSTS OFF)
SELECT value1, floor(10 - value1) FROM numeric_tbl GROUP BY value1, floor(10 - value1);

-- select floor with group by (result)
SELECT value1, floor(10 - value1) FROM numeric_tbl GROUP BY value1, floor(10 - value1);

-- select floor with group by index (result)
SELECT value1, floor(10 - value1) FROM numeric_tbl GROUP BY 2,1;

-- select floor with group by index (result)
SELECT value1, floor(10 - value1) FROM numeric_tbl GROUP BY 1,2;

-- select floor with group by having (EXPLAIN (COSTS OFF))
EXPLAIN (COSTS OFF)
SELECT value1, floor(10 - value1) FROM numeric_tbl GROUP BY value1, floor(10 - value1) HAVING floor(avg(value1)) > 0;

-- select floor with group by having (result)
SELECT value1, floor(10 - value1) FROM numeric_tbl GROUP BY value1, floor(10 - value1) HAVING floor(avg(value1)) > 0;

-- select floor with group by index having (result)
SELECT value1, floor(10 - value1) FROM numeric_tbl GROUP BY 2,1 HAVING floor(10 - value1) > 0;

-- select floor with group by index having (result)
SELECT value1, floor(10 - value1) FROM numeric_tbl GROUP BY 1,2 HAVING value1 > 1;

-- select floor and as
SELECT floor(value3) as floor1 FROM numeric_tbl;

-- ===============================================================================
-- test cosh()
-- ===============================================================================
-- select cosh (builtin function, EXPLAIN (COSTS OFF))
EXPLAIN (COSTS OFF)
SELECT value1, cosh(value2), cosh(value3), cosh(value4), cosh(0.5) FROM numeric_tbl;

-- select cosh (builtin function, result)
SELECT value1, cosh(value2), cosh(value3), cosh(value4), cosh(0.5) FROM numeric_tbl;

-- select cosh (builtin function, not pushdown constraints, EXPLAIN (COSTS OFF))
EXPLAIN (COSTS OFF)
SELECT cosh(value1), cosh(value2), cosh(value3), cosh(value4), cosh(0.5) FROM numeric_tbl WHERE to_hex(value2) = '64';

-- select cosh (builtin function, not pushdown constraints, result)
SELECT cosh(value1), cosh(value2), cosh(value3), cosh(value4), cosh(0.5) FROM numeric_tbl WHERE to_hex(value2) = '64';

-- select cosh (builtin function, pushdown constraints, EXPLAIN (COSTS OFF))
EXPLAIN (COSTS OFF)
SELECT cosh(value1), cosh(value2), cosh(value3), cosh(value4), cosh(0.5) FROM numeric_tbl WHERE value2 != 200;

-- select cosh (builtin function, pushdown constraints, result)
SELECT cosh(value1), cosh(value2), cosh(value3), cosh(value4), cosh(0.5) FROM numeric_tbl WHERE value2 != 200;

-- select cosh (builtin function, cosh in constraints, EXPLAIN (COSTS OFF))
EXPLAIN (COSTS OFF)
SELECT value1, cosh(value2), cosh(value3), cosh(value4), cosh(0.5) FROM numeric_tbl WHERE cosh(value1) != 1;

-- select cosh (builtin function, cosh in constraints, result)
SELECT value1, cosh(value2), cosh(value3), cosh(value4), cosh(0.5) FROM numeric_tbl WHERE cosh(value1) != 1;

-- select cosh (builtin function, cosh in constraints, EXPLAIN (COSTS OFF))
EXPLAIN (COSTS OFF)
SELECT cosh(value1), cosh(value2), cosh(value3), cosh(value4), cosh(0.5) FROM numeric_tbl WHERE cosh(0.5) > value1;

-- select cosh (builtin function, cosh in constraints, result)
SELECT cosh(value1), cosh(value2), cosh(value3), cosh(value4), cosh(0.5) FROM numeric_tbl WHERE cosh(0.5) > value1;

-- select cosh as nest function with agg (pushdown, EXPLAIN (COSTS OFF))
EXPLAIN (COSTS OFF)
SELECT sum(value3),cosh(sum(value3)) FROM numeric_tbl;

-- select cosh as nest function with agg (pushdown, result)
SELECT sum(value3),cosh(sum(value3)) FROM numeric_tbl;

-- select cosh as nest with log2 (pushdown, EXPLAIN (COSTS OFF))
EXPLAIN (COSTS OFF)
SELECT value1, cosh(log(2, value2)) FROM numeric_tbl;

-- select cosh as nest with log2 (pushdown, result)
SELECT value1, cosh(log(2, value2)) FROM numeric_tbl;

-- select cosh with non pushdown func and explicit constant (EXPLAIN (COSTS OFF))
EXPLAIN (COSTS OFF)
SELECT value1, cosh(value3), pi(), 4.1 FROM numeric_tbl;

-- select cosh with non pushdown func and explicit constant (result)
SELECT value1, cosh(value3), pi(), 4.1 FROM numeric_tbl;

-- select cosh with order by (EXPLAIN (COSTS OFF))
EXPLAIN (COSTS OFF)
SELECT value1, cosh(1-value3) FROM numeric_tbl ORDER BY cosh(1-value3);

-- select cosh with order by (result)
SELECT value1, cosh(1-value3) FROM numeric_tbl ORDER BY cosh(1-value3);

-- select cosh with order by index (result)
SELECT value1, cosh(1-value3) FROM numeric_tbl ORDER BY 2,1;

-- select cosh with order by index (result)
SELECT value1, cosh(1-value3) FROM numeric_tbl ORDER BY 1,2;

-- select cosh with group by (EXPLAIN (COSTS OFF))
EXPLAIN (COSTS OFF)
SELECT value1, cosh(1-value3) FROM numeric_tbl GROUP BY value1, cosh(1-value3);

-- select cosh with group by (result)
SELECT value1, cosh(1-value3) FROM numeric_tbl GROUP BY value1, cosh(1-value3);

-- select cosh with group by index (result)
SELECT value1, cosh(1-value3) FROM numeric_tbl GROUP BY 2,1;

-- select cosh with group by index (result)
SELECT value1, cosh(1-value3) FROM numeric_tbl GROUP BY 1,2;

-- select cosh with group by having (EXPLAIN (COSTS OFF))
EXPLAIN (COSTS OFF)
SELECT value1, cosh(1-value3) FROM numeric_tbl GROUP BY value1, cosh(1-value3) HAVING cosh(avg(value1)) > 0;

-- select cosh with group by having (result)
SELECT value1, cosh(1-value3) FROM numeric_tbl GROUP BY value1, cosh(1-value3) HAVING cosh(avg(value1)) > 0;

-- select cosh with group by index having (result)
SELECT value1, cosh(1-value3) FROM numeric_tbl GROUP BY 2,1 HAVING cosh(1-value3) > 0;

-- select cosh with group by index having (result)
SELECT value1, cosh(1-value3) FROM numeric_tbl GROUP BY 1,2 HAVING value1 > 1;

-- select cosh and as
SELECT value1, cosh(value3) as sin1 FROM numeric_tbl;

-- ===============================================================================
-- test sinh()
-- ===============================================================================
-- select sinh (builtin function, EXPLAIN (COSTS OFF))
EXPLAIN (COSTS OFF)
SELECT value1, sinh(value2), sinh(value3), sinh(value4), sinh(0.5) FROM numeric_tbl;

-- select sinh (builtin function, result)
SELECT value1, sinh(value2), sinh(value3), sinh(value4), sinh(0.5) FROM numeric_tbl;

-- select sinh (builtin function, not pushdown constraints, EXPLAIN (COSTS OFF))
EXPLAIN (COSTS OFF)
SELECT sinh(value1), sinh(value2), sinh(value3), sinh(value4), sinh(0.5) FROM numeric_tbl WHERE to_hex(value2) = '64';

-- select sinh (builtin function, not pushdown constraints, result)
SELECT sinh(value1), sinh(value2), sinh(value3), sinh(value4), sinh(0.5) FROM numeric_tbl WHERE to_hex(value2) = '64';

-- select sinh (builtin function, pushdown constraints, EXPLAIN (COSTS OFF))
EXPLAIN (COSTS OFF)
SELECT sinh(value1), sinh(value2), sinh(value3), sinh(value4), sinh(0.5) FROM numeric_tbl WHERE value2 != 200;

-- select sinh (builtin function, pushdown constraints, result)
SELECT sinh(value1), sinh(value2), sinh(value3), sinh(value4), sinh(0.5) FROM numeric_tbl WHERE value2 != 200;

-- select sinh (builtin function, sinh in constraints, EXPLAIN (COSTS OFF))
EXPLAIN (COSTS OFF)
SELECT value1, sinh(value2), sinh(value3), sinh(value4), sinh(0.5) FROM numeric_tbl WHERE sinh(value1) != 1;

-- select sinh (builtin function, sinh in constraints, result)
SELECT value1, sinh(value2), sinh(value3), sinh(value4), sinh(0.5) FROM numeric_tbl WHERE sinh(value1) != 1;

-- select sinh (builtin function, sinh in constraints, EXPLAIN (COSTS OFF))
EXPLAIN (COSTS OFF)
SELECT sinh(value1), sinh(value2), sinh(value3), sinh(value4), sinh(0.5) FROM numeric_tbl WHERE sinh(0.5) > value1;

-- select sinh (builtin function, sinh in constraints, result)
SELECT sinh(value1), sinh(value2), sinh(value3), sinh(value4), sinh(0.5) FROM numeric_tbl WHERE sinh(0.5) > value1;

-- select sinh as nest function with agg (pushdown, EXPLAIN (COSTS OFF))
EXPLAIN (COSTS OFF)
SELECT sum(value3),sinh(sum(value3)) FROM numeric_tbl;

-- select sinh as nest function with agg (pushdown, result)
SELECT sum(value3),sinh(sum(value3)) FROM numeric_tbl;

-- select sinh as nest with log2 (pushdown, EXPLAIN (COSTS OFF))
EXPLAIN (COSTS OFF)
SELECT value1, sinh(log(2, value2)) FROM numeric_tbl;

-- select sinh as nest with log2 (pushdown, result)
SELECT value1, sinh(log(2, value2)) FROM numeric_tbl;

-- select sinh with non pushdown func and explicit constant (EXPLAIN (COSTS OFF))
EXPLAIN (COSTS OFF)
SELECT value1, sinh(value3), pi(), 4.1 FROM numeric_tbl;

-- select sinh with non pushdown func and explicit constant (result)
SELECT value1, sinh(value3), pi(), 4.1 FROM numeric_tbl;

-- select sinh with order by (EXPLAIN (COSTS OFF))
EXPLAIN (COSTS OFF)
SELECT value1, sinh(1-value3) FROM numeric_tbl ORDER BY sinh(1-value3);

-- select sinh with order by (result)
SELECT value1, sinh(1-value3) FROM numeric_tbl ORDER BY sinh(1-value3);

-- select sinh with order by index (result)
SELECT value1, sinh(1-value3) FROM numeric_tbl ORDER BY 2,1;

-- select sinh with order by index (result)
SELECT value1, sinh(1-value3) FROM numeric_tbl ORDER BY 1,2;

-- select sinh with group by (EXPLAIN (COSTS OFF))
EXPLAIN (COSTS OFF)
SELECT value1, sinh(1-value3) FROM numeric_tbl GROUP BY value1, sinh(1-value3);

-- select sinh with group by (result)
SELECT value1, sinh(1-value3) FROM numeric_tbl GROUP BY value1, sinh(1-value3);

-- select sinh with group by index (result)
SELECT value1, sinh(1-value3) FROM numeric_tbl GROUP BY 2,1;

-- select sinh with group by index (result)
SELECT value1, sinh(1-value3) FROM numeric_tbl GROUP BY 1,2;

-- select sinh with group by having (EXPLAIN (COSTS OFF))
EXPLAIN (COSTS OFF)
SELECT value1, sinh(1-value3) FROM numeric_tbl GROUP BY value1, sinh(1-value3) HAVING sinh(avg(value1)) > 0;

-- select sinh with group by having (result)
SELECT value1, sinh(1-value3) FROM numeric_tbl GROUP BY value1, sinh(1-value3) HAVING sinh(avg(value1)) > 0;

-- select sinh with group by index having (result)
SELECT value1, sinh(1-value3) FROM numeric_tbl GROUP BY 2,1 HAVING sinh(1-value3) > 0;

-- select sinh with group by index having (result)
SELECT value1, sinh(1-value3) FROM numeric_tbl GROUP BY 1,2 HAVING value1 > 1;

-- select sinh and as
SELECT value1, sinh(value3) as sin1 FROM numeric_tbl;

-- ===============================================================================
-- test tanh()
-- ===============================================================================
-- select tanh (builtin function, EXPLAIN (COSTS OFF))
EXPLAIN (COSTS OFF)
SELECT value1, tanh(value2), tanh(value3), tanh(value4), tanh(0.5) FROM numeric_tbl;

-- select tanh (builtin function, result)
SELECT value1, tanh(value2), tanh(value3), tanh(value4), tanh(0.5) FROM numeric_tbl;

-- select tanh (builtin function, not pushdown constraints, EXPLAIN (COSTS OFF))
EXPLAIN (COSTS OFF)
SELECT tanh(value1), tanh(value2), tanh(value3), tanh(value4), tanh(0.5) FROM numeric_tbl WHERE to_hex(value2) = '64';

-- select tanh (builtin function, not pushdown constraints, result)
SELECT tanh(value1), tanh(value2), tanh(value3), tanh(value4), tanh(0.5) FROM numeric_tbl WHERE to_hex(value2) = '64';

-- select tanh (builtin function, pushdown constraints, EXPLAIN (COSTS OFF))
EXPLAIN (COSTS OFF)
SELECT tanh(value1), tanh(value2), tanh(value3), tanh(value4), tanh(0.5) FROM numeric_tbl WHERE value2 != 200;

-- select tanh (builtin function, pushdown constraints, result)
SELECT tanh(value1), tanh(value2), tanh(value3), tanh(value4), tanh(0.5) FROM numeric_tbl WHERE value2 != 200;

-- select tanh (builtin function, tanh in constraints, EXPLAIN (COSTS OFF))
EXPLAIN (COSTS OFF)
SELECT value1, tanh(value2), tanh(value3), tanh(value4), tanh(0.5) FROM numeric_tbl WHERE tanh(value1) != 1;

-- select tanh (builtin function, tanh in constraints, result)
SELECT value1, tanh(value2), tanh(value3), tanh(value4), tanh(0.5) FROM numeric_tbl WHERE tanh(value1) != 1;

-- select tanh (builtin function, tanh in constraints, EXPLAIN (COSTS OFF))
EXPLAIN (COSTS OFF)
SELECT tanh(value1), tanh(value2), tanh(value3), tanh(value4), tanh(0.5) FROM numeric_tbl WHERE tanh(0.5) > value1;

-- select tanh (builtin function, tanh in constraints, result)
SELECT tanh(value1), tanh(value2), tanh(value3), tanh(value4), tanh(0.5) FROM numeric_tbl WHERE tanh(0.5) > value1;

-- select tanh as nest function with agg (pushdown, EXPLAIN (COSTS OFF))
EXPLAIN (COSTS OFF)
SELECT sum(value3),tanh(sum(value3)) FROM numeric_tbl;

-- select tanh as nest function with agg (pushdown, result)
SELECT sum(value3),tanh(sum(value3)) FROM numeric_tbl;

-- select tanh as nest with log2 (pushdown, EXPLAIN (COSTS OFF))
EXPLAIN (COSTS OFF)
SELECT value1, tanh(log(2, value2)) FROM numeric_tbl;

-- select tanh as nest with log2 (pushdown, result)
SELECT value1, tanh(log(2, value2)) FROM numeric_tbl;

-- select tanh with non pushdown func and explicit constant (EXPLAIN (COSTS OFF))
EXPLAIN (COSTS OFF)
SELECT value1, tanh(value3), pi(), 4.1 FROM numeric_tbl;

-- select tanh with non pushdown func and explicit constant (result)
SELECT value1, tanh(value3), pi(), 4.1 FROM numeric_tbl;

-- select tanh with order by (EXPLAIN (COSTS OFF))
EXPLAIN (COSTS OFF)
SELECT value1, tanh(1-value3) FROM numeric_tbl ORDER BY tanh(1-value3);

-- select tanh with order by (result)
SELECT value1, tanh(1-value3) FROM numeric_tbl ORDER BY tanh(1-value3);

-- select tanh with order by index (result)
SELECT value1, tanh(1-value3) FROM numeric_tbl ORDER BY 2,1;

-- select tanh with order by index (result)
SELECT value1, tanh(1-value3) FROM numeric_tbl ORDER BY 1,2;

-- select tanh with group by (EXPLAIN (COSTS OFF))
EXPLAIN (COSTS OFF)
SELECT value1, tanh(1-value3) FROM numeric_tbl GROUP BY value1, tanh(1-value3);

-- select tanh with group by (result)
SELECT value1, tanh(1-value3) FROM numeric_tbl GROUP BY value1, tanh(1-value3);

-- select tanh with group by index (result)
SELECT value1, tanh(1-value3) FROM numeric_tbl GROUP BY 2,1;

-- select tanh with group by index (result)
SELECT value1, tanh(1-value3) FROM numeric_tbl GROUP BY 1,2;

-- select tanh with group by having (EXPLAIN (COSTS OFF))
EXPLAIN (COSTS OFF)
SELECT value1, tanh(1-value3) FROM numeric_tbl GROUP BY value1, tanh(1-value3) HAVING tanh(avg(value1)) > 0;

-- select tanh with group by having (result)
SELECT value1, tanh(1-value3) FROM numeric_tbl GROUP BY value1, tanh(1-value3) HAVING tanh(avg(value1)) > 0;

-- select tanh with group by index having (result)
SELECT value1, tanh(1-value3) FROM numeric_tbl GROUP BY 2,1 HAVING tanh(1-value3) > 0;

-- select tanh with group by index having (result)
SELECT value1, tanh(1-value3) FROM numeric_tbl GROUP BY 1,2 HAVING value1 > 1;

-- select tanh and as
SELECT value1, tanh(value3) as sin1 FROM numeric_tbl;

-- ===============================================================================
-- test width_bucket
-- ===============================================================================
-- select width_bucket (builtin function, EXPLAIN (COSTS OFF))
EXPLAIN (COSTS OFF)
SELECT width_bucket(value5, value6, value7, value8), width_bucket(value5, value6, value7, value8), width_bucket(value5, value6, value7, value8), width_bucket(value5, value6, value7, 2), width_bucket(value5, value6, value7, value8) FROM numeric_tbl;

-- select width_bucket (builtin function, result)
SELECT width_bucket(value5, value6, value7, value8), width_bucket(value5, value6, value7, value8), width_bucket(value5, value6, value7, value8), width_bucket(value5, value6, value7, 2), width_bucket(value5, value6, value7, value8) FROM numeric_tbl;

-- select width_bucket (builtin function, not pushdown constraints, EXPLAIN (COSTS OFF))
EXPLAIN (COSTS OFF)
SELECT width_bucket(value5, value6, value7, value8), width_bucket(value5, value6, value7, value8), width_bucket(value5, value6, value7, value8), width_bucket(value5, value6, value7, 2), width_bucket(value5, value6, value7, value8) FROM numeric_tbl WHERE to_hex(value2) = '64';

-- select width_bucket (builtin function, not pushdown constraints, result)
SELECT width_bucket(value5, value6, value7, value8), width_bucket(value5, value6, value7, value8), width_bucket(value5, value6, value7, value8), width_bucket(value5, value6, value7, 2), width_bucket(value5, value6, value7, value8) FROM numeric_tbl WHERE to_hex(value2) = '64';

-- select width_bucket (builtin function, pushdown constraints, EXPLAIN (COSTS OFF))
EXPLAIN (COSTS OFF)
SELECT width_bucket(value5, value6, value7, value8), width_bucket(value5, value6, value7, value8), width_bucket(value5, value6, value7, value8), width_bucket(value5, value6, value7, 2), width_bucket(value5, value6, value7, value8) FROM numeric_tbl WHERE value2 != 200;

-- select width_bucket (builtin function, pushdown constraints, result)
SELECT width_bucket(value5, value6, value7, value8), width_bucket(value5, value6, value7, value8), width_bucket(value5, value6, value7, value8), width_bucket(value5, value6, value7, 2), width_bucket(value5, value6, value7, value8) FROM numeric_tbl WHERE value2 != 200;

-- select width_bucket (builtin function, width_bucket in constraints, EXPLAIN (COSTS OFF))
EXPLAIN (COSTS OFF)
SELECT width_bucket(value5, value6, value7, value8), width_bucket(value5, value6, value7, value8), width_bucket(value5, value6, value7, value8), width_bucket(value5, value6, value7, 2), width_bucket(value5, value6, value7, value8) FROM numeric_tbl WHERE width_bucket(value5, value6, value7, value8) != 1;

-- select width_bucket (builtin function, width_bucket in constraints, result)
SELECT width_bucket(value5, value6, value7, value8), width_bucket(value5, value6, value7, value8), width_bucket(value5, value6, value7, value8), width_bucket(value5, value6, value7, 2), width_bucket(value5, value6, value7, value8) FROM numeric_tbl WHERE width_bucket(value5, value6, value7, value8) != 1;

-- select width_bucket (builtin function, width_bucket in constraints, EXPLAIN (COSTS OFF))
EXPLAIN (COSTS OFF)
SELECT width_bucket(value5, value6, value7, value8), width_bucket(value5, value6, value7, value8), width_bucket(value5, value6, value7, value8), width_bucket(value5, value6, value7, 2), width_bucket(value5, value6, value7, value8) FROM numeric_tbl WHERE width_bucket(value5, value6, value7, value8) > value1;

-- select width_bucket (builtin function, width_bucket in constraints, result)
SELECT width_bucket(value5, value6, value7, value8), width_bucket(value5, value6, value7, value8), width_bucket(value5, value6, value7, value8), width_bucket(value5, value6, value7, 2), width_bucket(value5, value6, value7, value8) FROM numeric_tbl WHERE width_bucket(value5, value6, value7, value8) > value1;

-- select width_bucket as nest function with agg (pushdown, EXPLAIN (COSTS OFF))
EXPLAIN (COSTS OFF)
SELECT sum(value3),width_bucket(value5, value6, value7, value8) FROM numeric_tbl GROUP BY value5, value6, value7, value8;

-- select width_bucket as nest function with agg (pushdown, result)
SELECT sum(value3),width_bucket(value5, value6, value7, value8) FROM numeric_tbl GROUP BY value5, value6, value7, value8;

-- select width_bucket as nest with log2 (pushdown, EXPLAIN (COSTS OFF))
EXPLAIN (COSTS OFF)
SELECT width_bucket(value5, value6, value7, value8) FROM numeric_tbl;

-- select width_bucket as nest with log2 (pushdown, result)
SELECT width_bucket(value5, value6, value7, value8) FROM numeric_tbl;

-- select width_bucket with non pushdown func and explicit constant (EXPLAIN (COSTS OFF))
EXPLAIN (COSTS OFF)
SELECT width_bucket(value5, value6, value7, value8), pi(), 4.1 FROM numeric_tbl;

-- select width_bucket with non pushdown func and explicit constant (result)
SELECT width_bucket(value5, value6, value7, value8), pi(), 4.1 FROM numeric_tbl;

-- select width_bucket with order by (EXPLAIN (COSTS OFF))
EXPLAIN (COSTS OFF)
SELECT value1, width_bucket(value5, value6, value7, value8) FROM numeric_tbl ORDER BY width_bucket(value5, value6, value7, value8);

-- select width_bucket with order by (result)
SELECT value1, width_bucket(value5, value6, value7, value8) FROM numeric_tbl ORDER BY width_bucket(value5, value6, value7, value8);

-- select width_bucket with order by index (result)
SELECT value1, width_bucket(value5, value6, value7, value8) FROM numeric_tbl ORDER BY 2,1;

-- select width_bucket with order by index (result)
SELECT value1, width_bucket(value5, value6, value7, value8) FROM numeric_tbl ORDER BY 1,2;

-- select width_bucket with group by (EXPLAIN (COSTS OFF))
EXPLAIN (COSTS OFF)
SELECT value1, width_bucket(value5, value6, value7, value8) FROM numeric_tbl GROUP BY value1, width_bucket(value5, value6, value7, value8);

-- select width_bucket with group by (result)
SELECT value1, width_bucket(value5, value6, value7, value8) FROM numeric_tbl GROUP BY value1, width_bucket(value5, value6, value7, value8);

-- select width_bucket with group by index (result)
SELECT value1, width_bucket(value5, value6, value7, value8) FROM numeric_tbl GROUP BY 2,1;

-- select width_bucket with group by index (result)
SELECT value1, width_bucket(value5, value6, value7, value8) FROM numeric_tbl GROUP BY 1,2;

-- select width_bucket with group by having (EXPLAIN (COSTS OFF))
EXPLAIN (COSTS OFF)
SELECT value1, width_bucket(value5, value6, value7, value8) FROM numeric_tbl GROUP BY value1, width_bucket(value5, value6, value7, value8) HAVING width_bucket(value5, value6, value7, value8) > 0;

-- select width_bucket with group by having (result)
SELECT value1, width_bucket(value5, value6, value7, value8) FROM numeric_tbl GROUP BY value1, width_bucket(value5, value6, value7, value8) HAVING width_bucket(value5, value6, value7, value8) > 0;

-- select width_bucket with group by index having (result)
SELECT value1, width_bucket(value5, value6, value7, value8) FROM numeric_tbl GROUP BY 2,1 HAVING width_bucket(value5, value6, value7, value8) > 0;

-- select width_bucket with group by index having (result)
SELECT value1, width_bucket(value5, value6, value7, value8) FROM numeric_tbl GROUP BY 1,2 HAVING value1 > 1;

-- select width_bucket and as
SELECT width_bucket(value5, value6, value7, value8) as floor1 FROM numeric_tbl;

-- ===============================================================================
-- test initcap
-- ===============================================================================
-- select initcap (stub function, EXPLAIN (COSTS OFF))
EXPLAIN (COSTS OFF)
SELECT initcap(str3), initcap(str2) FROM numeric_tbl;
-- select initcap (stub function, result)
SELECT initcap(str3), initcap(str2) FROM numeric_tbl;

-- select initcap (stub function, not pushdown constraints, EXPLAIN (COSTS OFF))
EXPLAIN (COSTS OFF)
SELECT value1, initcap(str3) FROM numeric_tbl WHERE to_hex(value2) = '64';
-- select initcap (stub function, not pushdown constraints, result)
SELECT value1, initcap(str3) FROM numeric_tbl WHERE to_hex(value2) = '64';

-- select initcap (stub function, pushdown constraints, EXPLAIN (COSTS OFF))
EXPLAIN (COSTS OFF)
SELECT value1, initcap(str3) FROM numeric_tbl WHERE value2 != 200;
-- select initcap (stub function, pushdown constraints, result)
SELECT value1, initcap(str3) FROM numeric_tbl WHERE value2 != 200;

-- select initcap with non pushdown func and explicit constant (EXPLAIN (COSTS OFF))
EXPLAIN (COSTS OFF)
SELECT initcap(str3), pi(), 4.1 FROM numeric_tbl;
-- select initcap with non pushdown func and explicit constant (result)
SELECT initcap(str3), pi(), 4.1 FROM numeric_tbl;

-- select initcap with order by (EXPLAIN (COSTS OFF))
EXPLAIN (COSTS OFF)
SELECT value1, initcap(str3) FROM numeric_tbl ORDER BY initcap(str3);
-- select initcap with order by (result)
SELECT value1, initcap(str3) FROM numeric_tbl ORDER BY initcap(str3);

-- select initcap with order by index (result)
SELECT value1, initcap(str3) FROM numeric_tbl ORDER BY 2,1;
-- select initcap with order by index (result)
SELECT value1, initcap(str3) FROM numeric_tbl ORDER BY 1,2;

-- select initcap with group by (EXPLAIN (COSTS OFF))
EXPLAIN (COSTS OFF)
SELECT count(value1), initcap(str3) FROM numeric_tbl GROUP BY initcap(str3);
-- select initcap with group by (result)
SELECT count(value1), initcap(str3) FROM numeric_tbl GROUP BY initcap(str3);

-- select initcap with group by index (result)
SELECT value1, initcap(str3) FROM numeric_tbl GROUP BY 2,1;

-- select initcap with group by having (EXPLAIN (COSTS OFF))
EXPLAIN (COSTS OFF)
SELECT count(value1), initcap(str3) FROM numeric_tbl GROUP BY initcap(str3), str1 HAVING initcap(str3) IS NOT NULL;
-- select initcap with group by having (result)
SELECT count(value1), initcap(str3) FROM numeric_tbl GROUP BY initcap(str3), str1 HAVING initcap(str3) IS NOT NULL;

-- select initcap with group by index having (result)
SELECT value1, initcap(str3) FROM numeric_tbl GROUP BY 1,2 HAVING value1 > 1;

-- ===============================================================================
-- test to_timestamp()
-- ===============================================================================
-- select to_timestamp (stub function, EXPLAIN (COSTS OFF))
EXPLAIN (COSTS OFF)
SELECT to_timestamp(str4, 'YYYY-MM-DD HH:MI:SS'), to_timestamp(str4, 'YYYY-MM-DD HH:MI:SS'), to_timestamp(str4, 'YYYY-MM-DD HH:MI:SS') FROM numeric_tbl;
-- select to_timestamp (stub function, result)
SELECT to_timestamp(str4, 'YYYY-MM-DD HH:MI:SS'), to_timestamp(str4, 'YYYY-MM-DD HH:MI:SS'), to_timestamp(str4, 'YYYY-MM-DD HH:MI:SS') FROM numeric_tbl;

-- select to_timestamp (stub function, not pushdown constraints, EXPLAIN (COSTS OFF))
EXPLAIN (COSTS OFF)
SELECT id, to_timestamp(str4, 'YYYY-MM-DD HH:MI:SS') FROM numeric_tbl WHERE to_hex(value2) = '64';
-- select to_timestamp (stub function, not pushdown constraints, EXPLAIN (COSTS OFF))
SELECT id, to_timestamp(str4, 'YYYY-MM-DD HH:MI:SS') FROM numeric_tbl WHERE to_hex(value2) = '64';

-- select to_timestamp (stub function, to_timestamp in constraints, EXPLAIN (COSTS OFF))
EXPLAIN (COSTS OFF)
SELECT id, to_timestamp(str4, 'YYYY-MM-DD HH:MI:SS') FROM numeric_tbl WHERE to_timestamp(str4, 'YYYY-MM-DD HH:MI:SS') > to_timestamp('2016-03-31 9:30:20', 'YYYY-MM-DD HH:MI:SS');
-- select to_timestamp (stub function, to_timestamp in constraints, result)
SELECT id, to_timestamp(str4, 'YYYY-MM-DD HH:MI:SS') FROM numeric_tbl WHERE to_timestamp(str4, 'YYYY-MM-DD HH:MI:SS') > to_timestamp('2016-03-31 9:30:20', 'YYYY-MM-DD HH:MI:SS');

-- select to_timestamp with non pushdown func and explicit constant (EXPLAIN (COSTS OFF))
EXPLAIN (COSTS OFF)
SELECT to_timestamp(str4, 'YYYY-MM-DD HH:MI:SS'), pi(), 4.1 FROM numeric_tbl;
-- select to_timestamp with non pushdown func and explicit constant (result)
SELECT to_timestamp(str4, 'YYYY-MM-DD HH:MI:SS'), pi(), 4.1 FROM numeric_tbl;

-- select to_timestamp with order by (EXPLAIN (COSTS OFF))
EXPLAIN (COSTS OFF)
SELECT value1, to_timestamp(str4, 'YYYY-MM-DD HH:MI:SS') FROM numeric_tbl ORDER BY to_timestamp(str4, 'YYYY-MM-DD HH:MI:SS'), 1 DESC;
-- select to_timestamp with order by (result)
SELECT value1, to_timestamp(str4, 'YYYY-MM-DD HH:MI:SS') FROM numeric_tbl ORDER BY to_timestamp(str4, 'YYYY-MM-DD HH:MI:SS'), 1 DESC;

-- select to_timestamp with group by (EXPLAIN (COSTS OFF))
EXPLAIN (COSTS OFF)
SELECT count(value1), to_timestamp(str4, 'YYYY-MM-DD HH:MI:SS') FROM numeric_tbl GROUP BY to_timestamp(str4, 'YYYY-MM-DD HH:MI:SS');
-- select to_timestamp with group by (result)
SELECT count(value1), to_timestamp(str4, 'YYYY-MM-DD HH:MI:SS') FROM numeric_tbl GROUP BY to_timestamp(str4, 'YYYY-MM-DD HH:MI:SS');

-- select to_timestamp with group by index (result)
SELECT value1, to_timestamp(str4, 'YYYY-MM-DD HH:MI:SS') FROM numeric_tbl GROUP BY 2,1;

-- select to_timestamp with group by having (EXPLAIN (COSTS OFF))
EXPLAIN (COSTS OFF)
SELECT count(value1), to_timestamp(str4, 'YYYY-MM-DD HH:MI:SS') FROM numeric_tbl GROUP BY to_timestamp(str4, 'YYYY-MM-DD HH:MI:SS'), str1 HAVING to_timestamp(str4, 'YYYY-MM-DD HH:MI:SS') > to_timestamp('2016-03-31 9:30:20', 'YYYY-MM-DD HH:MI:SS');
-- select to_timestamp with group by having (result)
SELECT count(value1), to_timestamp(str4, 'YYYY-MM-DD HH:MI:SS') FROM numeric_tbl GROUP BY to_timestamp(str4, 'YYYY-MM-DD HH:MI:SS'), str1 HAVING to_timestamp(str4, 'YYYY-MM-DD HH:MI:SS') > to_timestamp('2016-03-31 9:30:20', 'YYYY-MM-DD HH:MI:SS');

-- select to_timestamp with group by index having (result)
SELECT value1, to_timestamp(str4, 'YYYY-MM-DD HH:MI:SS') FROM numeric_tbl GROUP BY 1,2 HAVING value1 > 1;


-- ===============================================================================
-- test trunc()
-- ===============================================================================
-- select trunc (builtin function, EXPLAIN (COSTS OFF))
EXPLAIN (COSTS OFF)
SELECT trunc(value6), trunc(value7), trunc(value5), trunc(value1) FROM numeric_tbl;
-- select trunc (buitin function, result)
SELECT trunc(value6), trunc(value7), trunc(value5), trunc(value1) FROM numeric_tbl;
-- select trunc (builtin function, not pushdown constraints, EXPLAIN (COSTS OFF))
EXPLAIN (COSTS OFF)
SELECT trunc(value6), trunc(value7), trunc(value5), trunc(value1) FROM numeric_tbl WHERE to_hex(value2) != '64';
-- select trunc (builtin function, not pushdown constraints, result)
SELECT trunc(value6), trunc(value7), trunc(value5), trunc(value1) FROM numeric_tbl WHERE to_hex(value2) != '64';
-- select trunc (builtin function, pushdown constraints, EXPLAIN (COSTS OFF))
EXPLAIN (COSTS OFF)
SELECT trunc(value6), trunc(value7), trunc(value5), trunc(value1) FROM numeric_tbl WHERE value2 != 200;
-- select trunc (builtin function, pushdown constraints, result)
SELECT trunc(value6), trunc(value7), trunc(value5), trunc(value1) FROM numeric_tbl WHERE value2 != 200;

-- ===============================================================================
-- test translate()
-- ===============================================================================
-- select translate (stub function, EXPLAIN (COSTS OFF))
EXPLAIN (COSTS OFF)
SELECT translate(str3, 'abc', '123'),translate(str1, 'abc', '123') FROM numeric_tbl;
-- select translate (stub function, result)
SELECT translate(str3, 'abc', '123'),translate(str1, 'abc', '123') FROM numeric_tbl;

-- select translate (stub function, not pushdown constraints, EXPLAIN (COSTS OFF))
EXPLAIN (COSTS OFF)
SELECT value1, translate(str3, 'abc', '123') FROM numeric_tbl WHERE to_hex(value2) = '64';
-- select translate (stub function, not pushdown constraints, result)
SELECT value1, translate(str3, 'abc', '123') FROM numeric_tbl WHERE to_hex(value2) = '64';

-- select translate (stub function, pushdown constraints, EXPLAIN (COSTS OFF))
EXPLAIN (COSTS OFF)
SELECT value1, translate(str3, 'abc', '123') FROM numeric_tbl WHERE value2 != 200;
-- select translate (stub function, pushdown constraints, result)
SELECT value1, translate(str3, 'abc', '123') FROM numeric_tbl WHERE value2 != 200;

-- select translate with non pushdown func and explicit constant (EXPLAIN (COSTS OFF))
EXPLAIN (COSTS OFF)
SELECT translate(str3, 'abc', '123'), pi(), 4.1 FROM numeric_tbl;
-- select translate with non pushdown func and explicit constant (result)
SELECT translate(str3, 'abc', '123'), pi(), 4.1 FROM numeric_tbl;

-- select translate with order by (EXPLAIN (COSTS OFF))
EXPLAIN (COSTS OFF)
SELECT value1, translate(str3, 'abc', '123') FROM numeric_tbl ORDER BY translate(str3, 'abc', '123');
-- select translate with order by (result)
SELECT value1, translate(str3, 'abc', '123') FROM numeric_tbl ORDER BY translate(str3, 'abc', '123');

-- select translate with order by index (result)
SELECT value1, translate(str3, 'abc', '123') FROM numeric_tbl ORDER BY 2,1;
-- select translate with order by index (result)
SELECT value1, translate(str3, 'abc', '123') FROM numeric_tbl ORDER BY 1,2;

-- select translate with group by (EXPLAIN (COSTS OFF))
EXPLAIN (COSTS OFF)
SELECT count(value1), translate(str3, 'abc', '123') FROM numeric_tbl GROUP BY translate(str3, 'abc', '123');
-- select translate with group by (result)
SELECT count(value1), translate(str3, 'abc', '123') FROM numeric_tbl GROUP BY translate(str3, 'abc', '123');

-- select translate with group by index (result)
SELECT value1, translate(str3, 'abc', '123') FROM numeric_tbl GROUP BY 2,1;

-- select translate with group by having (EXPLAIN (COSTS OFF))
EXPLAIN (COSTS OFF)
SELECT count(value1), translate(str3, 'abc', '123') FROM numeric_tbl GROUP BY translate(str3, 'abc', '123'), str2 HAVING translate(str3, 'abc', '123') IS NOT NULL;
-- select translate with group by having (result)
SELECT count(value1), translate(str3, 'abc', '123') FROM numeric_tbl GROUP BY translate(str3, 'abc', '123'), str2 HAVING translate(str3, 'abc', '123') IS NOT NULL;

-- select translate with group by index having (result)
SELECT value1, translate(str3, 'abc', '123') FROM numeric_tbl GROUP BY 1,2 HAVING value1 > 1;



-- ===============================================================================
-- test to_char()
-- ===============================================================================
-- select to_char (stub function, EXPLAIN (COSTS OFF))
EXPLAIN (COSTS OFF)
SELECT to_char(value2, '999'), to_char(value2, '999'), to_char(value2, '999') FROM numeric_tbl;
-- select to_char (stub function, result)
SELECT to_char(value2, '999'), to_char(value2, '999'), to_char(value2, '999') FROM numeric_tbl;

-- select to_char (stub function, not pushdown constraints, EXPLAIN (COSTS OFF))
EXPLAIN (COSTS OFF)
SELECT id, to_char(value2, '999') FROM numeric_tbl WHERE to_hex(value2) = '64';
-- select to_char (stub function, not pushdown constraints, EXPLAIN (COSTS OFF))
SELECT id, to_char(value2, '999') FROM numeric_tbl WHERE to_hex(value2) = '64';

-- select to_char (stub function, to_char in constraints, EXPLAIN (COSTS OFF))
EXPLAIN (COSTS OFF)
SELECT id, to_char(value2, '999') FROM numeric_tbl WHERE to_char(value2, '999') > to_char(value4, '999');
-- select to_char (stub function, to_char in constraints, result)
SELECT id, to_char(value2, '999') FROM numeric_tbl WHERE to_char(value2, '999') > to_char(value4, '999');

-- select to_char with non pushdown func and explicit constant (EXPLAIN (COSTS OFF))
EXPLAIN (COSTS OFF)
SELECT to_char(value2, '999'), pi(), 4.1 FROM numeric_tbl;
-- select to_char with non pushdown func and explicit constant (result)
SELECT to_char(value2, '999'), pi(), 4.1 FROM numeric_tbl;

-- select to_char with order by (EXPLAIN (COSTS OFF))
EXPLAIN (COSTS OFF)
SELECT value1, to_char(value2, '999') FROM numeric_tbl ORDER BY to_char(value2, '999'), 1 DESC;
-- select to_char with order by (result)
SELECT value1, to_char(value2, '999') FROM numeric_tbl ORDER BY to_char(value2, '999'), 1 DESC;

-- select to_char with group by (EXPLAIN (COSTS OFF))
EXPLAIN (COSTS OFF)
SELECT count(value1), to_char(value2, '999') FROM numeric_tbl GROUP BY to_char(value2, '999');
-- select to_char with group by (result)
SELECT count(value1), to_char(value2, '999') FROM numeric_tbl GROUP BY to_char(value2, '999');

-- select to_char with group by index (result)
SELECT value1, to_char(value2, '999') FROM numeric_tbl GROUP BY 2,1;

-- select to_char with group by having (EXPLAIN (COSTS OFF))
EXPLAIN (COSTS OFF)
SELECT count(value1), to_char(value2, '999') FROM numeric_tbl GROUP BY value4, to_char(value2, '999'), str1 HAVING to_char(value2, '999') > to_char(value4, '999');
-- select to_char with group by having (result)
SELECT count(value1), to_char(value2, '999') FROM numeric_tbl GROUP BY value4, to_char(value2, '999'), str1 HAVING to_char(value2, '999') > to_char(value4, '999');

-- select to_char with group by index having (result)
SELECT value1, to_char(value2, '999') FROM numeric_tbl GROUP BY 1,2 HAVING value1 > 1;


-- ===============================================================================
-- test to_date()
-- ===============================================================================
-- select to_date (stub function, EXPLAIN (COSTS OFF))
EXPLAIN (COSTS OFF)
SELECT to_date(str4, 'YYYY-MM-DD HH:MI:SS'), to_date(str4, 'YYYY-MM-DD HH:MI:SS'), to_date(str4, 'YYYY-MM-DD HH:MI:SS') FROM numeric_tbl;
-- select to_date (stub function, result)
SELECT to_date(str4, 'YYYY-MM-DD HH:MI:SS'), to_date(str4, 'YYYY-MM-DD HH:MI:SS'), to_date(str4, 'YYYY-MM-DD HH:MI:SS') FROM numeric_tbl;

-- select to_date (stub function, not pushdown constraints, EXPLAIN (COSTS OFF))
EXPLAIN (COSTS OFF)
SELECT id, to_date(str4, 'YYYY-MM-DD HH:MI:SS') FROM numeric_tbl WHERE to_hex(value2) = '64';
-- select to_date (stub function, not pushdown constraints, EXPLAIN (COSTS OFF))
SELECT id, to_date(str4, 'YYYY-MM-DD HH:MI:SS') FROM numeric_tbl WHERE to_hex(value2) = '64';

-- select to_date (stub function, to_date in constraints, EXPLAIN (COSTS OFF))
EXPLAIN (COSTS OFF)
SELECT id, to_date(str4, 'YYYY-MM-DD HH:MI:SS') FROM numeric_tbl WHERE to_date(str4, 'YYYY-MM-DD HH:MI:SS') > to_date('2016-03-31 9:30:20', 'YYYY-MM-DD HH:MI:SS');
-- select to_date (stub function, to_date in constraints, result)
SELECT id, to_date(str4, 'YYYY-MM-DD HH:MI:SS') FROM numeric_tbl WHERE to_date(str4, 'YYYY-MM-DD HH:MI:SS') > to_date('2016-03-31 9:30:20', 'YYYY-MM-DD HH:MI:SS');

-- select to_date with non pushdown func and explicit constant (EXPLAIN (COSTS OFF))
EXPLAIN (COSTS OFF)
SELECT to_date(str4, 'YYYY-MM-DD HH:MI:SS'), pi(), 4.1 FROM numeric_tbl;
-- select to_date with non pushdown func and explicit constant (result)
SELECT to_date(str4, 'YYYY-MM-DD HH:MI:SS'), pi(), 4.1 FROM numeric_tbl;

-- select to_date with order by (EXPLAIN (COSTS OFF))
EXPLAIN (COSTS OFF)
SELECT value1, to_date(str4, 'YYYY-MM-DD HH:MI:SS') FROM numeric_tbl ORDER BY to_date(str4, 'YYYY-MM-DD HH:MI:SS'), 1 DESC;
-- select to_date with order by (result)
SELECT value1, to_date(str4, 'YYYY-MM-DD HH:MI:SS') FROM numeric_tbl ORDER BY to_date(str4, 'YYYY-MM-DD HH:MI:SS'), 1 DESC;

-- select to_date with group by (EXPLAIN (COSTS OFF))
EXPLAIN (COSTS OFF)
SELECT count(value1), to_date(str4, 'YYYY-MM-DD HH:MI:SS') FROM numeric_tbl GROUP BY to_date(str4, 'YYYY-MM-DD HH:MI:SS');
-- select to_date with group by (result)
SELECT count(value1), to_date(str4, 'YYYY-MM-DD HH:MI:SS') FROM numeric_tbl GROUP BY to_date(str4, 'YYYY-MM-DD HH:MI:SS');

-- select to_date with group by index (result)
SELECT value1, to_date(str4, 'YYYY-MM-DD HH:MI:SS') FROM numeric_tbl GROUP BY 2,1;

-- select to_date with group by having (EXPLAIN (COSTS OFF))
EXPLAIN (COSTS OFF)
SELECT count(value1), to_date(str4, 'YYYY-MM-DD HH:MI:SS') FROM numeric_tbl GROUP BY to_date(str4, 'YYYY-MM-DD HH:MI:SS'), str1 HAVING to_date(str4, 'YYYY-MM-DD HH:MI:SS') > to_date('2016-03-31 9:30:20', 'YYYY-MM-DD HH:MI:SS');
-- select to_date with group by having (result)
SELECT count(value1), to_date(str4, 'YYYY-MM-DD HH:MI:SS') FROM numeric_tbl GROUP BY to_date(str4, 'YYYY-MM-DD HH:MI:SS'), str1 HAVING to_date(str4, 'YYYY-MM-DD HH:MI:SS') > to_date('2016-03-31 9:30:20', 'YYYY-MM-DD HH:MI:SS');

-- select to_date with group by index having (result)
SELECT value1, to_date(str4, 'YYYY-MM-DD HH:MI:SS') FROM numeric_tbl GROUP BY 1,2 HAVING value1 > 1;

-- ===============================================================================
-- test to_number()
-- ===============================================================================
-- select to_number (stub function, EXPLAIN (COSTS OFF))
EXPLAIN (COSTS OFF)
SELECT to_number(str5, '99G999D9S'), to_number(str5, '99G999D9S'), to_number(str5, '99G999D9S') FROM numeric_tbl;
-- select to_number (stub function, result)
SELECT to_number(str5, '99G999D9S'), to_number(str5, '99G999D9S'), to_number(str5, '99G999D9S') FROM numeric_tbl;

-- select to_number (stub function, not pushdown constraints, EXPLAIN (COSTS OFF))
EXPLAIN (COSTS OFF)
SELECT id, to_number(str5, '99G999D9S') FROM numeric_tbl WHERE to_hex(value2) = '64';
-- select to_number (stub function, not pushdown constraints, EXPLAIN (COSTS OFF))
SELECT id, to_number(str5, '99G999D9S') FROM numeric_tbl WHERE to_hex(value2) = '64';

-- select to_number (stub function, to_number in constraints, EXPLAIN (COSTS OFF))
EXPLAIN (COSTS OFF)
SELECT id, to_number(str5, '99G999D9S') FROM numeric_tbl WHERE to_number(str5, '99G999D9S') < 0;
-- select to_number (stub function, to_number in constraints, result)
SELECT id, to_number(str5, '99G999D9S') FROM numeric_tbl WHERE to_number(str5, '99G999D9S') < 0;

-- select to_number with non pushdown func and explicit constant (EXPLAIN (COSTS OFF))
EXPLAIN (COSTS OFF)
SELECT to_number(str5, '99G999D9S'), pi(), 4.1 FROM numeric_tbl;
-- select to_number with non pushdown func and explicit constant (result)
SELECT to_number(str5, '99G999D9S'), pi(), 4.1 FROM numeric_tbl;

-- select to_number with order by (EXPLAIN (COSTS OFF))
EXPLAIN (COSTS OFF)
SELECT value1, to_number(str5, '99G999D9S') FROM numeric_tbl ORDER BY to_number(str5, '99G999D9S'), 1 DESC;
-- select to_number with order by (result)
SELECT value1, to_number(str5, '99G999D9S') FROM numeric_tbl ORDER BY to_number(str5, '99G999D9S'), 1 DESC;

-- select to_number with group by (EXPLAIN (COSTS OFF))
EXPLAIN (COSTS OFF)
SELECT count(value1), to_number(str5, '99G999D9S') FROM numeric_tbl GROUP BY to_number(str5, '99G999D9S');
-- select to_number with group by (result)
SELECT count(value1), to_number(str5, '99G999D9S') FROM numeric_tbl GROUP BY to_number(str5, '99G999D9S');

-- select to_number with group by index (result)
SELECT value1, to_number(str5, '99G999D9S') FROM numeric_tbl GROUP BY 2,1;

-- select to_number with group by having (EXPLAIN (COSTS OFF))
EXPLAIN (COSTS OFF)
SELECT count(value1), to_number(str5, '99G999D9S') FROM numeric_tbl GROUP BY to_number(str5, '99G999D9S'), str1 HAVING to_number(str5, '99G999D9S') < 0;
-- select to_number with group by having (result)
SELECT count(value1), to_number(str5, '99G999D9S') FROM numeric_tbl GROUP BY to_number(str5, '99G999D9S'), str1 HAVING to_number(str5, '99G999D9S') < 0;

-- select to_number with group by index having (explain)
EXPLAIN (COSTS OFF)
SELECT value1, to_number(str5, '99G999D9S') FROM numeric_tbl GROUP BY 1,2 HAVING value1 > 1;;
-- select to_number with group by index having (result)
SELECT value1, to_number(str5, '99G999D9S') FROM numeric_tbl GROUP BY 1,2 HAVING value1 > 1;;

-- End test for Numeric


--
-- test for date/time function
--

-- ADD_MONTHS()
-- select add_months (stub function, explain)
EXPLAIN (COSTS OFF)
SELECT add_months(c2, 10), add_months(c2, '10') FROM time_tbl;

-- select add_months (stub function, result)
SELECT add_months(c2, 10), add_months(c2, '10') FROM time_tbl;

-- select add_months (stub function, explain)
EXPLAIN (COSTS OFF)
SELECT add_months(c2, -10), add_months('01-Aug-03', '10') FROM time_tbl;

-- select add_months (stub function, result)
SELECT add_months(c2, -10), add_months('01-Aug-03', '10') FROM time_tbl;

-- CURRENT_DATE()
-- select oracle_current_date (stub function, explain)
EXPLAIN (COSTS OFF)
SELECT oracle_current_date() FROM time_tbl;

-- select oracle_current_date with months_between to make stable result (stub function, result)
SELECT months_between(oracle_current_date(), oracle_current_date()) FROM time_tbl GROUP BY 1;

-- select oracle_current_date (stub function, not pushdown constraints, explain)
EXPLAIN (COSTS OFF)
SELECT oracle_current_date() FROM time_tbl WHERE to_hex(id) > '0';

-- select oracle_current_date with months_between to make stable result (stub function, result)
SELECT months_between(oracle_current_date(), oracle_current_date()) FROM time_tbl WHERE to_hex(id) > '0' GROUP BY 1;

-- select oracle_current_date (stub function, pushdown constraints, explain)
EXPLAIN (COSTS OFF)
SELECT oracle_current_date() FROM time_tbl WHERE id = 1;

-- select oracle_current_date with months_between to make stable result (stub function, result)
SELECT months_between(oracle_current_date(), oracle_current_date()) FROM time_tbl WHERE id = 1 GROUP BY 1;

-- select oracle_current_date (stub function, oracle_current_date in constraints, explain)
EXPLAIN (COSTS OFF)
SELECT oracle_current_date() FROM time_tbl WHERE oracle_current_date() > '2000-01-01';

-- select oracle_current_date with months_between to make stable result (stub function, result)
SELECT months_between(oracle_current_date(), oracle_current_date()) FROM time_tbl WHERE oracle_current_date() > '2000-01-01' GROUP BY 1;

-- oracle_current_date in constrains (stub function, explain)
EXPLAIN (COSTS OFF)
SELECT c1 FROM time_tbl WHERE oracle_current_date() > '2000-01-01';

-- oracle_current_date in constrains (stub function, result)
SELECT c1 FROM time_tbl WHERE oracle_current_date() > '2000-01-01';

-- oracle_current_date as parameter of add_moths(stub function, explain)
EXPLAIN (COSTS OFF)
SELECT c1 FROM time_tbl WHERE add_months(oracle_current_date(), 31) > '2000-01-01';

-- oracle_current_date as parameter of add_months(stub function, result)
SELECT c1 FROM time_tbl WHERE add_months(oracle_current_date(), 31) > '2000-01-01';

-- select oracle_current_date and agg (pushdown, explain)
EXPLAIN (COSTS OFF)
SELECT oracle_current_date(), sum(id) FROM time_tbl;

-- select oracle_current_date with months_between to make stable result (stub function, result)
SELECT months_between(oracle_current_date(), oracle_current_date()), sum(id) FROM time_tbl GROUP BY 1;

-- select oracle_current_date with order by (explain)
EXPLAIN (COSTS OFF)
SELECT oracle_current_date(), c1 FROM time_tbl ORDER BY c1;

-- select oracle_current_date with months_between to make stable result (stub function, result)
SELECT months_between(oracle_current_date(), oracle_current_date()), c1 FROM time_tbl ORDER BY c1;

-- select oracle_current_date with order by index (explain)
EXPLAIN (COSTS OFF)
SELECT oracle_current_date(), c1 FROM time_tbl ORDER BY 2;

-- select oracle_current_date with months_between to make stable result (stub function, result)
SELECT months_between(oracle_current_date(), oracle_current_date()), c1 FROM time_tbl ORDER BY 2;

-- oracle_current_date constraints with order by (explain)
EXPLAIN (COSTS OFF)
SELECT c1 FROM time_tbl WHERE add_months(oracle_current_date(), 31) > '2000-01-01' ORDER BY c1;

-- oracle_current_date constraints with order by (result)
SELECT c1 FROM time_tbl WHERE add_months(oracle_current_date(), 31) > '2000-01-01' ORDER BY c1;

-- select oracle_current_date with group by (explain)
EXPLAIN (COSTS OFF)
SELECT oracle_current_date(), c1 FROM time_tbl GROUP BY c1;

-- select oracle_current_date with months_between to make stable result (stub function, result)
SELECT months_between(oracle_current_date(), oracle_current_date()), c1 FROM time_tbl GROUP BY c1;

-- select oracle_current_date with group by index (explain)
EXPLAIN (COSTS OFF)
SELECT oracle_current_date(), c1 FROM time_tbl GROUP BY 2;

-- select oracle_current_date with months_between to make stable result (stub function, result)
SELECT months_between(oracle_current_date(), oracle_current_date()), c1 FROM time_tbl GROUP BY 2;

-- select oracle_current_date with group by having (explain)
EXPLAIN (COSTS OFF)
SELECT oracle_current_date(), c1 FROM time_tbl GROUP BY oracle_current_date(), c1 HAVING oracle_current_date() > '2000-01-01';

-- select oracle_current_date with months_between to make stable result (stub function, result)
SELECT months_between(oracle_current_date(), oracle_current_date()), c1 FROM time_tbl GROUP BY oracle_current_date(), c1 HAVING oracle_current_date() > '2000-01-01';

-- select oracle_current_date with group by index having (explain)
EXPLAIN (COSTS OFF)
SELECT oracle_current_date(), c1 FROM time_tbl GROUP BY 2,1 HAVING oracle_current_date() > '2000-01-01';

-- select oracle_current_date with months_between to make stable result (stub function, result)
SELECT months_between(oracle_current_date(), oracle_current_date()), c1 FROM time_tbl GROUP BY 2,1 HAVING oracle_current_date() > '2000-01-01';

-- oracle_current_date constraints with group by (explain)
EXPLAIN (COSTS OFF)
SELECT c1 FROM time_tbl WHERE add_months(oracle_current_date(), 31) > '2000-01-01' GROUP BY c1;

-- oracle_current_date constraints with group by (result)
SELECT c1 FROM time_tbl WHERE add_months(oracle_current_date(), 31) > '2000-01-01' GROUP BY c1;

-- select oracle_current_date with alias (explain)
EXPLAIN (COSTS OFF)
SELECT oracle_current_date() as oracle_current_date1 FROM time_tbl;

-- CURRENT_TIMESTAMP
-- oracle_current_timestamp constraints (explain)
EXPLAIN (COSTS OFF)
SELECT oracle_current_timestamp() FROM time_tbl;

-- oracle_current_timestamp constraints (result)
SELECT oracle_current_timestamp() - oracle_current_timestamp() FROM time_tbl;

-- select oracle_current_timestamp (stub function, not pushdown constraints, explain)
EXPLAIN (COSTS OFF)
SELECT oracle_current_timestamp() - oracle_current_timestamp() FROM time_tbl WHERE to_hex(id) > '0';

-- select oracle_current_timestamp (stub function, not pushdown constraints, result)
SELECT oracle_current_timestamp() - oracle_current_timestamp() FROM time_tbl WHERE to_hex(id) > '0';

-- select oracle_current_timestamp (stub function, pushdown constraints, explain)
EXPLAIN (COSTS OFF)
SELECT oracle_current_timestamp() FROM time_tbl WHERE id = 1;

-- select oracle_current_timestamp (stub function, pushdown constraints, result)
SELECT oracle_current_timestamp() - oracle_current_timestamp() FROM time_tbl WHERE id = 1;

-- select oracle_current_timestamp (stub function, oracle_current_timestamp in constraints, explain)
EXPLAIN (COSTS OFF)
SELECT oracle_current_timestamp() FROM time_tbl WHERE oracle_current_timestamp() > '2000-01-01 00:00:00'::timestamp;

-- select oracle_current_timestamp (stub function, oracle_current_timestamp in constraints, result)
SELECT oracle_current_timestamp() - oracle_current_timestamp() FROM time_tbl WHERE oracle_current_timestamp() > '2000-01-01 00:00:00'::timestamp;

-- oracle_current_timestamp in constrains (stub function, explain)
EXPLAIN (COSTS OFF)
SELECT c1 FROM time_tbl WHERE oracle_current_timestamp() > '2000-01-01 00:00:00'::timestamp;

-- oracle_current_timestamp in constrains (stub function, result)
SELECT c1 FROM time_tbl WHERE oracle_current_timestamp() > '2000-01-01 00:00:00'::timestamp;

-- select oracle_current_timestamp and agg (pushdown, explain)
EXPLAIN (COSTS OFF)
SELECT oracle_current_timestamp(), sum(id) FROM time_tbl;

-- select oracle_current_timestamp and agg (pushdown, result)
SELECT oracle_current_timestamp() - oracle_current_timestamp(), sum(id) FROM time_tbl;

-- select oracle_current_timestamp with order by (explain)
EXPLAIN (COSTS OFF)
SELECT oracle_current_timestamp(), c1 FROM time_tbl ORDER BY oracle_current_timestamp();

-- select oracle_current_timestamp with order by (result)
SELECT oracle_current_timestamp() - oracle_current_timestamp(), c1 FROM time_tbl ORDER BY oracle_current_timestamp();

-- select oracle_current_timestamp with order by index (explain)
EXPLAIN (COSTS OFF)
SELECT oracle_current_timestamp(), c1 FROM time_tbl ORDER BY 1;

-- select oracle_current_timestamp with order by index (result)
SELECT oracle_current_timestamp() - oracle_current_timestamp(), c1 FROM time_tbl ORDER BY 1;

-- select oracle_current_timestamp with group by (explain)
EXPLAIN (COSTS OFF)
SELECT oracle_current_timestamp(), c1 FROM time_tbl GROUP BY c1;

-- select oracle_current_timestamp with group by (result)
SELECT oracle_current_timestamp() - oracle_current_timestamp(), c1 FROM time_tbl GROUP BY c1;

-- select oracle_current_timestamp with group by index (explain)
EXPLAIN (COSTS OFF)
SELECT oracle_current_timestamp(), c1 FROM time_tbl GROUP BY 2;

-- select oracle_current_timestamp with group by index (result)
SELECT oracle_current_timestamp() - oracle_current_timestamp(), c1 FROM time_tbl GROUP BY 2;

-- select oracle_current_timestamp with group by having (explain)
EXPLAIN (COSTS OFF)
SELECT oracle_current_timestamp(), c1 FROM time_tbl GROUP BY oracle_current_timestamp(),c1 HAVING oracle_current_timestamp() > '2000-01-01 00:00:00'::timestamp;

-- select oracle_current_timestamp with group by having (result)
SELECT oracle_current_timestamp() - oracle_current_timestamp(), c1 FROM time_tbl GROUP BY oracle_current_timestamp(),c1 HAVING oracle_current_timestamp() > '2000-01-01 00:00:00'::timestamp;

-- select oracle_current_timestamp with group by index having (explain)
EXPLAIN (COSTS OFF)
SELECT oracle_current_timestamp(), c1 FROM time_tbl GROUP BY 2,1 HAVING oracle_current_timestamp() > '2000-01-01 00:00:00'::timestamp;

-- select oracle_current_timestamp with group by index having (result)
SELECT oracle_current_timestamp() - oracle_current_timestamp(), c1 FROM time_tbl GROUP BY 2,1 HAVING oracle_current_timestamp() > '2000-01-01 00:00:00'::timestamp;

-- select oracle_current_timestamp with alias (explain)
EXPLAIN (COSTS OFF)
SELECT oracle_current_timestamp() as oracle_current_timestamp1 FROM time_tbl;

-- select oracle_current_timestamp with alias (result)
SELECT (oracle_current_timestamp() - oracle_current_timestamp()) as oracle_current_timestamp_diff FROM time_tbl;

-- LOCALTIMESTAMP, LOCALTIMESTAMP()
-- select oracle_localtimestamp (stub function, explain)
EXPLAIN (COSTS OFF)
SELECT oracle_localtimestamp() FROM time_tbl;

-- select oracle_localtimestamp (stub function, result)
-- result is different from expected one
SELECT oracle_localtimestamp() - oracle_localtimestamp() FROM time_tbl;

-- select oracle_localtimestamp (stub function, not pushdown constraints, explain)
EXPLAIN (COSTS OFF)
SELECT oracle_localtimestamp() FROM time_tbl WHERE to_hex(id) > '0';

-- select oracle_localtimestamp (stub function, not pushdown constraints, result)
SELECT oracle_localtimestamp() - oracle_localtimestamp() FROM time_tbl WHERE to_hex(id) > '0';

-- select oracle_localtimestamp (stub function, pushdown constraints, explain)
EXPLAIN (COSTS OFF)
SELECT oracle_localtimestamp() FROM time_tbl WHERE id = 1;

-- select oracle_localtimestamp (stub function, pushdown constraints, result)
SELECT oracle_localtimestamp() - oracle_localtimestamp() FROM time_tbl WHERE id = 1;

-- select oracle_localtimestamp (stub function, oracle_localtimestamp in constraints, explain)
EXPLAIN (COSTS OFF)
SELECT oracle_localtimestamp() FROM time_tbl WHERE oracle_localtimestamp() > '2000-01-01 00:00:00'::timestamp;

-- select oracle_localtimestamp (stub function, oracle_localtimestamp in constraints, result)
SELECT oracle_localtimestamp() - oracle_localtimestamp() FROM time_tbl WHERE oracle_localtimestamp() > '2000-01-01 00:00:00'::timestamp;

-- oracle_localtimestamp in constrains (stub function, explain)
EXPLAIN (COSTS OFF)
SELECT c1 FROM time_tbl WHERE oracle_localtimestamp() > '2000-01-01 00:00:00'::timestamp;

-- oracle_localtimestamp in constrains (stub function, result)
SELECT c1 FROM time_tbl WHERE oracle_localtimestamp() > '2000-01-01 00:00:00'::timestamp;

-- select oracle_localtimestamp and agg (pushdown, explain)
EXPLAIN (COSTS OFF)
SELECT oracle_localtimestamp(), sum(id) FROM time_tbl;

-- select oracle_localtimestamp and agg (pushdown, result)
-- result is different from expected one
SELECT oracle_localtimestamp() - oracle_localtimestamp(), sum(id) FROM time_tbl;

-- select oracle_localtimestamp with order by (explain)
EXPLAIN (COSTS OFF)
SELECT oracle_localtimestamp(), c1 FROM time_tbl ORDER BY oracle_localtimestamp();

-- select oracle_localtimestamp with order by (result)
SELECT oracle_localtimestamp() - oracle_localtimestamp(), c1 FROM time_tbl ORDER BY oracle_localtimestamp();

-- select oracle_localtimestamp with order by index (explain)
EXPLAIN (COSTS OFF)
SELECT oracle_localtimestamp(), c1 FROM time_tbl ORDER BY 1;

-- select oracle_localtimestamp with order by index (result)
SELECT oracle_localtimestamp() - oracle_localtimestamp(), c1 FROM time_tbl ORDER BY 1;

-- select oracle_localtimestamp with group by (explain)
EXPLAIN (COSTS OFF)
SELECT oracle_localtimestamp(), c1 FROM time_tbl GROUP BY c1;

-- select oracle_localtimestamp with group by (result)
SELECT oracle_localtimestamp() - oracle_localtimestamp(), c1 FROM time_tbl GROUP BY c1;

-- select oracle_localtimestamp with group by index (explain)
EXPLAIN (COSTS OFF)
SELECT oracle_localtimestamp(), c1 FROM time_tbl GROUP BY 2;

-- select oracle_localtimestamp with group by index (result)
SELECT oracle_localtimestamp() - oracle_localtimestamp(), c1 FROM time_tbl GROUP BY 2;

-- select oracle_localtimestamp with group by having (explain)
EXPLAIN (COSTS OFF)
SELECT oracle_localtimestamp(), c1 FROM time_tbl GROUP BY oracle_localtimestamp(),c1 HAVING oracle_localtimestamp() > '2000-01-01 00:00:00'::timestamp;

-- select oracle_localtimestamp with group by having (result)
SELECT oracle_localtimestamp() - oracle_localtimestamp(), c1 FROM time_tbl GROUP BY oracle_localtimestamp(),c1 HAVING oracle_localtimestamp() > '2000-01-01 00:00:00'::timestamp;

-- select oracle_localtimestamp with group by index having (explain)
EXPLAIN (COSTS OFF)
SELECT oracle_localtimestamp(), c1 FROM time_tbl GROUP BY 2,1 HAVING oracle_localtimestamp() > '2000-01-01 00:00:00'::timestamp;

-- select oracle_localtimestamp with group by index having (result)
SELECT oracle_localtimestamp() - oracle_localtimestamp(), c1 FROM time_tbl GROUP BY 2,1 HAVING oracle_localtimestamp() > '2000-01-01 00:00:00'::timestamp;

-- select oracle_localtimestamp with alias (explain)
EXPLAIN (COSTS OFF)
SELECT oracle_localtimestamp() as oracle_localtimestamp1 FROM time_tbl;

-- select oracle_localtimestamp with alias (result)
SELECT (oracle_localtimestamp() - oracle_localtimestamp()) as oracle_localtimestamp_diff FROM time_tbl;

-- LAST_DAY()
-- select last_day (stub function, explain)
EXPLAIN (COSTS OFF)
SELECT last_day(c2) FROM time_tbl;

-- select last_day (stub function, result)
SELECT last_day(c2) FROM time_tbl;

-- select last_day (stub function, not pushdown constraints, explain)
EXPLAIN (COSTS OFF)
SELECT last_day(c2) FROM time_tbl WHERE to_hex(id) = '1';

-- select last_day (stub function, not pushdown constraints, result)
SELECT last_day(c2) FROM time_tbl WHERE to_hex(id) = '1';

-- select last_day (stub function, pushdown constraints, explain)
EXPLAIN (COSTS OFF)
SELECT last_day(c2) FROM time_tbl WHERE id != 200;

-- select last_day (stub function, pushdown constraints, result)
SELECT last_day(c2) FROM time_tbl WHERE id != 200;

-- select last_day with agg (pushdown, explain)
EXPLAIN (COSTS OFF)
SELECT max(c2), last_day(max(c2)) FROM time_tbl;

-- select last_day as nest function with agg (pushdown, result)
SELECT max(c2), last_day(max(c2)) FROM time_tbl;

-- select last_day with order by (explain)
EXPLAIN (COSTS OFF)
SELECT id, last_day(c2) FROM time_tbl order by last_day(c2);

-- select last_day with order by (result)
SELECT id, last_day(c2) FROM time_tbl order by last_day(c2);

-- select last_day with order by index (explain)
EXPLAIN (COSTS OFF)
SELECT id, last_day(c2) FROM time_tbl order by 2,1;

-- select last_day with order by index (result)
SELECT id, last_day(c2) FROM time_tbl order by 2,1;

-- select last_day with order by index (explain)
EXPLAIN (COSTS OFF)
SELECT id, last_day(c2) FROM time_tbl order by 1,2;

-- select last_day with order by index (result)
SELECT id, last_day(c2) FROM time_tbl order by 1,2;

-- select last_day with group by (explain)
EXPLAIN (COSTS OFF)
SELECT max(c2), last_day(c2) FROM time_tbl group by 2;

-- select last_day with group by (result)
SELECT max(c2), last_day(c2) FROM time_tbl group by 2;

-- select last_day with group by index (explain)
EXPLAIN (COSTS OFF)
SELECT id, last_day(c2) FROM time_tbl group by 2, 1;

-- select last_day with group by index (result)
SELECT id, last_day(c2) FROM time_tbl group by 2, 1;

-- select last_day with group by index (explain)
EXPLAIN (COSTS OFF)
SELECT id, last_day(c2) FROM time_tbl group by 1, 2;

-- select last_day with group by index (result)
SELECT id, last_day(c2) FROM time_tbl group by 1, 2;

-- select last_day with group by having (explain)
EXPLAIN (COSTS OFF)
SELECT max(c2), last_day(c2) FROM time_tbl group by last_day(c2), c2 HAVING last_day(c2) > '2001-01-31'::date;

-- select last_day with group by having (result)
SELECT max(c2), last_day(c2) FROM time_tbl group by last_day(c2), c2 HAVING last_day(c2) > '2001-01-31'::date;

-- select last_day with group by index having (explain)
EXPLAIN (COSTS OFF)
SELECT id, last_day(c2), c2 FROM time_tbl group by id, last_day(c2), c2 HAVING last_day(c2) > '2001-01-31'::date;

-- select last_day with group by index having (result)
SELECT id, last_day(c2), c2 FROM time_tbl group by id, last_day(c2), c2 HAVING last_day(c2) > '2001-01-31'::date;


-- EXTRACT()
-- select oracle_extract (stub function, explain)
EXPLAIN (COSTS OFF)
SELECT oracle_extract('year', c1), oracle_extract('year', c2), oracle_extract('year', c3) FROM time_tbl;

-- select oracle_extract (stub function, result)
SELECT oracle_extract('year', c1), oracle_extract('year', c2), oracle_extract('year', c3) FROM time_tbl;

-- select oracle_extract (stub function, not pushdown constraints, explain)
EXPLAIN (COSTS OFF)
SELECT oracle_extract('year', c1), oracle_extract('year', c2), oracle_extract('year', c3) FROM time_tbl WHERE to_hex(id) = '1';

-- select oracle_extract (stub function, not pushdown constraints, result)
SELECT oracle_extract('year', c1), oracle_extract('year', c2), oracle_extract('year', c3) FROM time_tbl WHERE to_hex(id) = '1';

-- select oracle_extract (stub function, pushdown constraints, explain)
EXPLAIN (COSTS OFF)
SELECT oracle_extract('year', c1), oracle_extract('year', c2), oracle_extract('year', c3) FROM time_tbl WHERE id != 200;

-- select oracle_extract (stub function, pushdown constraints, result)
SELECT oracle_extract('year', c1), oracle_extract('year', c2), oracle_extract('year', c3) FROM time_tbl WHERE id != 200;

-- select oracle_extract (stub function, oracle_extract in constraints, explain)
EXPLAIN (COSTS OFF)
SELECT oracle_extract('year', c1), oracle_extract('year', c2), oracle_extract('year', c3) FROM time_tbl WHERE oracle_extract('year', c3 ) != oracle_extract('year', '2000-01-01'::timestamp);

-- select oracle_extract (stub function, oracle_extract in constraints, result)
SELECT oracle_extract('year', c1), oracle_extract('year', c2), oracle_extract('year', c3) FROM time_tbl WHERE oracle_extract('year', c3 ) != oracle_extract('year', '2000-01-01'::timestamp);

-- select oracle_extract (stub function, oracle_extract in constraints, explain)
EXPLAIN (COSTS OFF)
SELECT oracle_extract('year', c2), oracle_extract('second', '2021-01-03 12:10:30.123456'::timestamp), oracle_extract('day', c3) FROM time_tbl WHERE oracle_extract('year', c3 ) > '1';

-- select oracle_extract (stub function, oracle_extract in constraints, result)
SELECT oracle_extract('year', c2), oracle_extract('second', '2021-01-03 12:10:30.123456'::timestamp), oracle_extract('day', c3) FROM time_tbl WHERE oracle_extract('year', c3 ) > '1';

-- select oracle_extract with agg (pushdown, explain)
EXPLAIN (COSTS OFF)
SELECT max(c3), oracle_extract('year', max(c3)) FROM time_tbl;

-- select oracle_extract as nest function with agg (pushdown, result)
SELECT max(c3), oracle_extract('year', max(c3)) FROM time_tbl;

-- select oracle_extract with order by (explain)
EXPLAIN (COSTS OFF)
SELECT id, oracle_extract('year', c2), oracle_extract('second', '2021-01-03 12:10:30.123456'::timestamp), oracle_extract('day', c3) FROM time_tbl order by oracle_extract('year', c2), oracle_extract('second', '2021-01-03 12:10:30.123456'::timestamp), oracle_extract('day', c3);

-- select oracle_extract with order by (result)
SELECT id, oracle_extract('year', c2), oracle_extract('second', '2021-01-03 12:10:30.123456'::timestamp), oracle_extract('day', c3) FROM time_tbl order by oracle_extract('year', c2), oracle_extract('second', '2021-01-03 12:10:30.123456'::timestamp), oracle_extract('day', c3);

-- select oracle_extract with order by index (explain)
EXPLAIN (COSTS OFF)
SELECT id, oracle_extract('year', c2), oracle_extract('second', '2021-01-03 12:10:30.123456'::timestamp), oracle_extract('minute', c3) FROM time_tbl order by 4,3,2,1;

-- select oracle_extract with order by index (result)
SELECT id, oracle_extract('year', c2), oracle_extract('second', '2021-01-03 12:10:30.123456'::timestamp), oracle_extract('minute', c3) FROM time_tbl order by 4,3,2,1;

-- select oracle_extract with group by (explain)
EXPLAIN (COSTS OFF)
SELECT max(c3), oracle_extract('year', c2), oracle_extract('second', '2021-01-03 12:10:30.123456'::timestamp), oracle_extract('minute', c3) FROM time_tbl group by oracle_extract('minute', c3),c2;

-- select oracle_extract with group by (result)
SELECT max(c3), oracle_extract('year', c2), oracle_extract('second', '2021-01-03 12:10:30.123456'::timestamp), oracle_extract('minute', c3) FROM time_tbl group by oracle_extract('minute', c3),c2;

-- select oracle_extract with group by index (explain)
EXPLAIN (COSTS OFF)
SELECT id, oracle_extract('year', c2), oracle_extract('second', '2021-01-03 12:10:30.123456'::timestamp), oracle_extract('minute', c3) FROM time_tbl group by 4,3,2,1;

-- select oracle_extract with group by index (result)
SELECT id, oracle_extract('year', c2), oracle_extract('second', '2021-01-03 12:10:30.123456'::timestamp), oracle_extract('minute', c3) FROM time_tbl group by 4,3,2,1;

-- select oracle_extract with group by index (explain)
EXPLAIN (COSTS OFF)
SELECT id, oracle_extract('year', c2), oracle_extract('second', '2021-01-03 12:10:30.123456'::timestamp), oracle_extract('minute', c3) FROM time_tbl group by 1,2,3,4;

-- select oracle_extract with group by index (result)
SELECT id, oracle_extract('year', c2), oracle_extract('second', '2021-01-03 12:10:30.123456'::timestamp), oracle_extract('minute', c3) FROM time_tbl group by 1,2,3,4;

-- select oracle_extract with group by index having (explain)
EXPLAIN (COSTS OFF)
SELECT id, oracle_extract('year', c2), oracle_extract('second', '2021-01-03 12:10:30.123456'::timestamp), oracle_extract('minute', c3), c2 FROM time_tbl group by 5, 4, 3, 2, 1 HAVING oracle_extract('year', c2) > 2000;

-- select oracle_extract with group by index having (result)
SELECT id, oracle_extract('year', c2), oracle_extract('second', '2021-01-03 12:10:30.123456'::timestamp), oracle_extract('minute', c3), c2 FROM time_tbl group by 5, 4, 3, 2, 1 HAVING oracle_extract('year', c2) > 2000;

-- select oracle_extract with group by index having (explain)
EXPLAIN (COSTS OFF)
SELECT id, oracle_extract('year', c2), oracle_extract('second', '2021-01-03 12:10:30.123456'::timestamp), oracle_extract('minute', c3), c2 FROM time_tbl group by 1, 2, 3, 4, 5 HAVING oracle_extract('year', c2) > 2000;

-- select oracle_extract with group by index having (result)
SELECT id, oracle_extract('year', c2), oracle_extract('second', '2021-01-03 12:10:30.123456'::timestamp), oracle_extract('minute', c3), c2 FROM time_tbl group by 1, 2, 3, 4, 5 HAVING oracle_extract('year', c2) > 2000;

-- select oracle_extract and as
SELECT oracle_extract('year', c2) as oracle_extract1, oracle_extract('second', '2021-01-03 12:10:30.123456'::timestamp) as oracle_extract2, oracle_extract('minute', c3) as oracle_extract3 FROM time_tbl;

-- select oracle_extract with date type (explain)
EXPLAIN (COSTS OFF)
SELECT oracle_extract('year', c2), oracle_extract('month', c2), oracle_extract('day', c2) FROM time_tbl; 

-- select oracle_extract with date type (result)
SELECT oracle_extract('year', c2), oracle_extract('month', c2), oracle_extract('day', c2) FROM time_tbl; 

-- select oracle_extract with timestamp with time zone type (explain)
EXPLAIN (COSTS OFF)
SELECT oracle_extract('year', c1), oracle_extract('month', c1), oracle_extract('day', c1), oracle_extract('hour', c1), oracle_extract('minute', c1), oracle_extract('second', c1),  oracle_extract('timezone_hour', c1),  oracle_extract('timezone_minute', c1) FROM time_tbl;

-- select oracle_extract with timestamp with time zone type (result)
SELECT oracle_extract('year', c1), oracle_extract('month', c1), oracle_extract('day', c1), oracle_extract('hour', c1), oracle_extract('minute', c1), oracle_extract('second', c1),  oracle_extract('timezone_hour', c1),  oracle_extract('timezone_minute', c1) FROM time_tbl;

-- select oracle_extract with timestamp without time zone type (explain)
EXPLAIN (COSTS OFF)
SELECT oracle_extract('year', c1), oracle_extract('month', c1), oracle_extract('day', c1), oracle_extract('hour', c1), oracle_extract('minute', c1), oracle_extract('second', c1) FROM time_tbl;

-- select oracle_extract with timestamp without time zone type (result)
SELECT oracle_extract('year', c1), oracle_extract('month', c1), oracle_extract('day', c1), oracle_extract('hour', c1), oracle_extract('minute', c1), oracle_extract('second', c1) FROM time_tbl;

-- select oracle_extract with interval day to second type (explain)
EXPLAIN (COSTS OFF)
SELECT oracle_extract('day', interval '5 04:30:20.11' day to second), oracle_extract('hour', interval '5 04:30:20.11' day to second), oracle_extract('minute', interval '5 04:30:20.11' day to second), oracle_extract('second', interval '5 04:30:20.11' day to second) FROM time_tbl;

-- select oracle_extract with interval day to second type (result)
SELECT oracle_extract('day', interval '5 04:30:20.11' day to second), oracle_extract('hour', interval '5 04:30:20.11' day to second), oracle_extract('minute', interval '5 04:30:20.11' day to second), oracle_extract('second', interval '5 04:30:20.11' day to second) FROM time_tbl;

-- select oracle_extract with interval day to second type (explain)
EXPLAIN (COSTS OFF)
SELECT oracle_extract('day', interval '40 days 2 hours 1 minute 1 second'), oracle_extract('hour', interval '40 days 2 hours 1 minute 1 second'), oracle_extract('minute', interval '40 days 2 hours 1 minute 1 second'), oracle_extract('second', interval '40 days 2 hours 1 minute 1 second') FROM time_tbl;

-- select oracle_extract with interval day to second type (result)
SELECT oracle_extract('day', interval '40 days 2 hours 1 minute 1 second'), oracle_extract('hour', interval '40 days 2 hours 1 minute 1 second'), oracle_extract('minute', interval '40 days 2 hours 1 minute 1 second'), oracle_extract('second', interval '40 days 2 hours 1 minute 1 second') FROM time_tbl;


-- DBTIMEZONE()
-- select dbtimezone (explain)
EXPLAIN (COSTS OFF)
SELECT dbtimezone() FROM time_tbl LIMIT 1;

-- select dbtimezone (result)
SELECT dbtimezone() FROM time_tbl LIMIT 1;

-- FROM_TZ(timestamp, timezone)
-- select from_tz (explain)
EXPLAIN (COSTS OFF)
SELECT c3, from_tz(c3, '3:00') FROM time_tbl;

-- select from_tz (result)
SELECT c3, from_tz(c3, '3:00') FROM time_tbl;


-- MONTHS_BETWEEN(date, date)
-- select months_between, negative result (explain)
EXPLAIN (COSTS OFF)
SELECT c2, months_between(c2, '2025-01-01') FROM time_tbl;

-- select months_between, negative result (result)
SELECT c2, months_between(c2, '2025-01-01') FROM time_tbl;

-- select months_between, positive result (explain)
EXPLAIN (COSTS OFF)
SELECT c2, months_between('2025-01-01', c2) FROM time_tbl;

-- select months_between, positive result (result)
SELECT c2, months_between('2025-01-01', c2) FROM time_tbl;


-- NEW_TIME(date, timezone1, timezone2)
-- set 24 hour format
SELECT oracle_execute('oracle_srv', 'ALTER SESSION SET NLS_DATE_FORMAT = ''DD-MON-YYYY HH24:MI:SS''');

-- select new_time, ast, bst (explain)
EXPLAIN (COSTS OFF)
SELECT c2, new_time(c2, 'AST', 'BST') FROM time_tbl;

-- select new_time, ast, bst (result)
SELECT c2, new_time(c2, 'AST', 'BST') FROM time_tbl;

-- select new_time, ast, cst (explain)
EXPLAIN (COSTS OFF)
SELECT c2, new_time(c2, 'AST', 'CST') FROM time_tbl;

-- select new_time, ast, cst (result)
SELECT c2, new_time(c2, 'AST', 'CST') FROM time_tbl;

-- select new_time, ast, est (explain)
EXPLAIN (COSTS OFF)
SELECT c2, new_time(c2, 'AST', 'EST') FROM time_tbl;

-- select new_time, ast, est (result)
SELECT c2, new_time(c2, 'AST', 'EST') FROM time_tbl;

-- select new_time, ast, gmt (explain)
EXPLAIN (COSTS OFF)
SELECT c2, new_time(c2, 'AST', 'GMT') FROM time_tbl;

-- select new_time, ast, gmt (result)
SELECT c2, new_time(c2, 'AST', 'GMT') FROM time_tbl;

-- select new_time, ast, hst (explain)
EXPLAIN (COSTS OFF)
SELECT c2, new_time(c2, 'AST', 'HST') FROM time_tbl;

-- select new_time, ast, hst (result)
SELECT c2, new_time(c2, 'AST', 'HST') FROM time_tbl;

-- select new_time, ast, mst (explain)
EXPLAIN (COSTS OFF)
SELECT c2, new_time(c2, 'AST', 'MST') FROM time_tbl;

-- select new_time, ast, mst (result)
SELECT c2, new_time(c2, 'AST', 'MST') FROM time_tbl;

-- select new_time, ast, nst (explain)
EXPLAIN (COSTS OFF)
SELECT c2, new_time(c2, 'AST', 'NST') FROM time_tbl;

-- select new_time, ast, nst (result)
SELECT c2, new_time(c2, 'AST', 'NST') FROM time_tbl;

-- select new_time, ast, pst (explain)
EXPLAIN (COSTS OFF)
SELECT c2, new_time(c2, 'AST', 'PST') FROM time_tbl;

-- select new_time, ast, pst (result)
SELECT c2, new_time(c2, 'AST', 'PST') FROM time_tbl;

-- select new_time, ast, yst (explain)
EXPLAIN (COSTS OFF)
SELECT c2, new_time(c2, 'AST', 'YST') FROM time_tbl;

-- select new_time, ast, yst (result)
SELECT c2, new_time(c2, 'AST', 'YST') FROM time_tbl;

-- NEXT_DAY(date, day_of_week)
-- select next_day (explain)
EXPLAIN (COSTS OFF)
SELECT c2, next_day(c2, 'MON') FROM time_tbl;

-- select next_day (result)
SELECT c2, next_day(c2, 'MON') FROM time_tbl;

-- select next_day (explain)
EXPLAIN (COSTS OFF)
SELECT c2, next_day(c2, 'TUE') FROM time_tbl;

-- select next_day (result)
SELECT c2, next_day(c2, 'TUE') FROM time_tbl;

-- select next_day (explain)
EXPLAIN (COSTS OFF)
SELECT c2, next_day(c2, 'WED') FROM time_tbl;

-- select next_day (result)
SELECT c2, next_day(c2, 'WED') FROM time_tbl;

-- select next_day (explain)
EXPLAIN (COSTS OFF)
SELECT c2, next_day(c2, 'THU') FROM time_tbl;

-- select next_day (result)
SELECT c2, next_day(c2, 'THU') FROM time_tbl;

-- select next_day (explain)
EXPLAIN (COSTS OFF)
SELECT c2, next_day(c2, 'FRI') FROM time_tbl;

-- select next_day (result)
SELECT c2, next_day(c2, 'FRI') FROM time_tbl;

-- select next_day (explain)
EXPLAIN (COSTS OFF)
SELECT c2, next_day(c2, 'SAT') FROM time_tbl;

-- select next_day (result)
SELECT c2, next_day(c2, 'SAT') FROM time_tbl;

-- select next_day (explain)
EXPLAIN (COSTS OFF)
SELECT c2, next_day(c2, 'MONDAY') FROM time_tbl;

-- select next_day (result)
SELECT c2, next_day(c2, 'MONDAY') FROM time_tbl;

-- select next_day (explain)
EXPLAIN (COSTS OFF)
SELECT c2, next_day(c2, 'TUESDAY') FROM time_tbl;

-- select next_day (result)
SELECT c2, next_day(c2, 'TUESDAY') FROM time_tbl;

-- select next_day (explain)
EXPLAIN (COSTS OFF)
SELECT c2, next_day(c2, 'WEDNESDAY') FROM time_tbl;

-- select next_day (result)
SELECT c2, next_day(c2, 'WEDNESDAY') FROM time_tbl;

-- select next_day (explain)
EXPLAIN (COSTS OFF)
SELECT c2, next_day(c2, 'THURSDAY') FROM time_tbl;

-- select next_day (result)
SELECT c2, next_day(c2, 'THURSDAY') FROM time_tbl;

-- select next_day (explain)
EXPLAIN (COSTS OFF)
SELECT c2, next_day(c2, 'FRIDAY') FROM time_tbl;

-- select next_day (result)
SELECT c2, next_day(c2, 'FRIDAY') FROM time_tbl;

-- select next_day (explain)
EXPLAIN (COSTS OFF)
SELECT c2, next_day(c2, 'SATURDAY') FROM time_tbl;

-- select next_day (result)
SELECT c2, next_day(c2, 'SATURDAY') FROM time_tbl;

-- NUMTODSINTERVAL(number, unit)
-- select numtodsinterval with day (explain)
EXPLAIN (COSTS OFF)
SELECT numtodsinterval(100, 'DAY') as DAY_TO_SECOND FROM time_tbl LIMIT 1;

-- select numtodsinterval with day (result)
SELECT numtodsinterval(100, 'DAY') as DAY_TO_SECOND FROM time_tbl LIMIT 1;

-- select numtodsinterval with hour (explain)
EXPLAIN (COSTS OFF)
SELECT numtodsinterval(100, 'HOUR') as DAY_TO_SECOND FROM time_tbl LIMIT 1;

-- select numtodsinterval with hour (result)
SELECT numtodsinterval(100, 'HOUR') as DAY_TO_SECOND FROM time_tbl LIMIT 1;

-- select numtodsinterval with minute (explain)
EXPLAIN (COSTS OFF)
SELECT numtodsinterval(100, 'MINUTE') as DAY_TO_SECOND FROM time_tbl LIMIT 1;

-- select numtodsinterval with minute (result)
SELECT numtodsinterval(100, 'MINUTE') as DAY_TO_SECOND FROM time_tbl LIMIT 1;

-- select numtodsinterval with second (result)
EXPLAIN (COSTS OFF)
SELECT numtodsinterval(100, 'SECOND') as DAY_TO_SECOND FROM time_tbl LIMIT 1;

-- select numtodsinterval with second (result)
SELECT numtodsinterval(100, 'SECOND') as DAY_TO_SECOND FROM time_tbl LIMIT 1;

-- NUMTOYMINTERVAL(number, unit)
-- select numtoyminterval with year (explain)
EXPLAIN (COSTS OFF)
SELECT numtoyminterval(100, 'YEAR') as YEAR_TO_MONTH FROM time_tbl LIMIT 1;

-- select numtoyminterval with year (result)
SELECT numtoyminterval(100, 'YEAR') as YEAR_TO_MONTH FROM time_tbl LIMIT 1;

-- select numtoyminterval with month (explain)
EXPLAIN (COSTS OFF)
SELECT numtoyminterval(100, 'MONTH') as YEAR_TO_MONTH FROM time_tbl LIMIT 1;

-- select numtoyminterval with month (result)
SELECT numtoyminterval(100, 'MONTH') as YEAR_TO_MONTH FROM time_tbl LIMIT 1;


-- ORACLE_ROUND(date/timestamp)
-- select round with date (explain)
EXPLAIN (COSTS OFF)
select c2, oracle_round(c2) from time_tbl;

-- select round with date (result)
select c2, oracle_round(c2) from time_tbl;

-- select round with date and format (explain)
-- One greater than the first two digits of a four-digit year
EXPLAIN (COSTS OFF)
select c2, oracle_round(c2, 'CC') from time_tbl;

-- select round with date and format (result)
-- One greater than the first two digits of a four-digit year
select c2, oracle_round(c2, 'CC') from time_tbl;

-- select round with date and format (explain)
-- One greater than the first two digits of a four-digit year
EXPLAIN (COSTS OFF)
select c2, oracle_round(c2, 'SCC') from time_tbl;

-- select round with date and format (result)
-- One greater than the first two digits of a four-digit year
select c2, oracle_round(c2, 'SCC') from time_tbl;

-- select round with date and format (explain)
-- Year (rounds up on July 1)
EXPLAIN (COSTS OFF)
select c2, oracle_round(c2, 'SYYYY') from time_tbl;

-- select round with date and format (result)
-- Year (rounds up on July 1)
select c2, oracle_round(c2, 'SYYYY') from time_tbl;

-- select round with date and format (explain)
-- Year (rounds up on July 1)
EXPLAIN (COSTS OFF)
select c2, oracle_round(c2, 'YYYY') from time_tbl;

-- select round with date and format (result)
-- Year (rounds up on July 1)
select c2, oracle_round(c2, 'YYYY') from time_tbl;

-- select round with date and format (explain)
-- Year (rounds up on July 1)
EXPLAIN (COSTS OFF)
select c2, oracle_round(c2, 'YEAR') from time_tbl;

-- select round with date and format (result)
-- Year (rounds up on July 1)
select c2, oracle_round(c2, 'YEAR') from time_tbl;

-- select round with date and format (explain)
-- Year (rounds up on July 1)
EXPLAIN (COSTS OFF)
select c2, oracle_round(c2, 'SYEAR') from time_tbl;

-- select round with date and format (result)
-- Year (rounds up on July 1)
select c2, oracle_round(c2, 'SYEAR') from time_tbl;

-- select round with date and format (explain)
-- Year (rounds up on July 1)
EXPLAIN (COSTS OFF)
select c2, oracle_round(c2, 'YYY') from time_tbl;

-- select round with date and format (result)
-- Year (rounds up on July 1)
select c2, oracle_round(c2, 'YYY') from time_tbl;

-- select round with date and format (explain)
-- Year (rounds up on July 1)
EXPLAIN (COSTS OFF)
select c2, oracle_round(c2, 'YY') from time_tbl;

-- select round with date and format (result)
-- Year (rounds up on July 1)
select c2, oracle_round(c2, 'YY') from time_tbl;

-- select round with date and format (explain)
-- Year (rounds up on July 1)
EXPLAIN (COSTS OFF)
select c2, oracle_round(c2, 'Y') from time_tbl;

-- select round with date and format (result)
-- Year (rounds up on July 1)
select c2, oracle_round(c2, 'Y') from time_tbl;

-- select round with date and format (explain)
-- ISO Year
EXPLAIN (COSTS OFF)
select c2, oracle_round(c2, 'IYYY') from time_tbl;

-- select round with date and format (result)
-- ISO Year
select c2, oracle_round(c2, 'IYYY') from time_tbl;

-- select round with date and format (explain)
-- ISO Year
EXPLAIN (COSTS OFF)
select c2, oracle_round(c2, 'IYY') from time_tbl;

-- select round with date and format (result)
-- ISO Year
select c2, oracle_round(c2, 'IYY') from time_tbl;

-- select round with date and format (explain)
-- ISO Year
EXPLAIN (COSTS OFF)
select c2, oracle_round(c2, 'IY') from time_tbl;

-- select round with date and format (result)
-- ISO Year
select c2, oracle_round(c2, 'IY') from time_tbl;

-- select round with date and format (explain)
-- ISO Year
EXPLAIN (COSTS OFF)
select c2, oracle_round(c2, 'I') from time_tbl;

-- select round with date and format (result)
-- ISO Year
select c2, oracle_round(c2, 'I') from time_tbl;

-- select round with date and format (explain)
-- Quarter (rounds up on the sixteenth day of the second month of the quarter)
EXPLAIN (COSTS OFF)
select c2, oracle_round(c2, 'Q') from time_tbl;

-- select round with date and format (result)
-- Quarter (rounds up on the sixteenth day of the second month of the quarter)
select c2, oracle_round(c2, 'Q') from time_tbl;

-- select round with date and format (explain)
-- Month (rounds up on the sixteenth day)
EXPLAIN (COSTS OFF)
select c2, oracle_round(c2, 'MONTH') from time_tbl;

-- select round with date and format (result)
-- Month (rounds up on the sixteenth day)
select c2, oracle_round(c2, 'MONTH') from time_tbl;

-- select round with date and format (explain)
-- Month (rounds up on the sixteenth day)
EXPLAIN (COSTS OFF)
select c2, oracle_round(c2, 'MON') from time_tbl;

-- select round with date and format (result)
-- Month (rounds up on the sixteenth day)
select c2, oracle_round(c2, 'MON') from time_tbl;

-- select round with date and format (explain)
-- Month (rounds up on the sixteenth day)
EXPLAIN (COSTS OFF)
select c2, oracle_round(c2, 'MM') from time_tbl;

-- select round with date and format (result)
-- Month (rounds up on the sixteenth day)
select c2, oracle_round(c2, 'MM') from time_tbl;

-- select round with date and format (explain)
-- Month (rounds up on the sixteenth day)
EXPLAIN (COSTS OFF)
select c2, oracle_round(c2, 'RM') from time_tbl;

-- select round with date and format (result)
-- Month (rounds up on the sixteenth day)
select c2, oracle_round(c2, 'RM') from time_tbl;

-- select round with date and format (explain)
-- Same day of the week as the first day of the year
EXPLAIN (COSTS OFF)
select c2, oracle_round(c2, 'WW') from time_tbl;

-- select round with date and format (result)
-- Same day of the week as the first day of the year
select c2, oracle_round(c2, 'WW') from time_tbl;

-- select round with date and format (explain)
-- Same day of the week as the first day of the ISO year
EXPLAIN (COSTS OFF)
select c2, oracle_round(c2, 'IW') from time_tbl;

-- select round with date and format (result)
-- Same day of the week as the first day of the ISO year
select c2, oracle_round(c2, 'IW') from time_tbl;

-- select round with date and format (explain)
-- Same day of the week as the first day of the month
EXPLAIN (COSTS OFF)
select c2, oracle_round(c2, 'W') from time_tbl;

-- select round with date and format (result)
-- Same day of the week as the first day of the month
select c2, oracle_round(c2, 'W') from time_tbl;

-- select round with date and format (explain)
-- Day
EXPLAIN (COSTS OFF)
select c2, oracle_round(c2, 'DDD') from time_tbl;

-- select round with date and format (result)
-- Day
select c2, oracle_round(c2, 'DDD') from time_tbl;

-- select round with date and format (result)
-- Day
EXPLAIN (COSTS OFF)
select c2, oracle_round(c2, 'DD') from time_tbl;

-- select round with date and format (result)
-- Day
select c2, oracle_round(c2, 'DD') from time_tbl;

-- select round with date and format (result)
-- Day
EXPLAIN (COSTS OFF)
select c2, oracle_round(c2, 'J') from time_tbl;

-- select round with date and format (result)
-- Day
select c2, oracle_round(c2, 'J') from time_tbl;

-- select round with date and format (explain)
-- Starting day of the week
EXPLAIN (COSTS OFF)
select c2, oracle_round(c2, 'DAY') from time_tbl;

-- select round with date and format (result)
-- Starting day of the week
select c2, oracle_round(c2, 'DAY') from time_tbl;

-- select round with date and format (explain)
-- Starting day of the week
EXPLAIN (COSTS OFF)
select c2, oracle_round(c2, 'DY') from time_tbl;

-- select round with date and format (result)
-- Starting day of the week
select c2, oracle_round(c2, 'DY') from time_tbl;

-- select round with date and format (explain)
-- Starting day of the week
EXPLAIN (COSTS OFF)
select c2, oracle_round(c2, 'D') from time_tbl;

-- select round with date and format (result)
-- Starting day of the week
select c2, oracle_round(c2, 'D') from time_tbl;

-- select round with date and format (explain)
-- Hour
EXPLAIN (COSTS OFF)
select c2, oracle_round(c2, 'HH') from time_tbl;

-- select round with date and format (result)
-- Hour
select c2, oracle_round(c2, 'HH') from time_tbl;

-- select round with date and format (explain)
-- Hour
EXPLAIN (COSTS OFF)
select c2, oracle_round(c2, 'HH12') from time_tbl;

-- select round with date and format (result)
-- Hour
select c2, oracle_round(c2, 'HH12') from time_tbl;

-- select round with date and format (explain)
-- Hour
EXPLAIN (COSTS OFF)
select c2, oracle_round(c2, 'HH24') from time_tbl;

-- select round with date and format (result)
-- Hour
select c2, oracle_round(c2, 'HH24') from time_tbl;

-- select round with date and format (explain)
-- Minute
EXPLAIN (COSTS OFF)
select c2, oracle_round(c2, 'MI') from time_tbl;

-- select round with date and format (result)
-- Minute
select c2, oracle_round(c2, 'MI') from time_tbl;

-- select round with timestamp (explain)
EXPLAIN (COSTS OFF)
select c3, oracle_round(c3) from time_tbl;

-- select round with timestamp (result)
select c3, oracle_round(c3) from time_tbl;

-- select round with timestamp and format (explain)
-- One greater than the first two digits of a four-digit year
EXPLAIN (COSTS OFF)
select c3, oracle_round(c3, 'CC') from time_tbl;

-- select round with timestamp and format (result)
-- One greater than the first two digits of a four-digit year
select c3, oracle_round(c3, 'CC') from time_tbl;

-- select round with timestamp and format (explain)
-- One greater than the first two digits of a four-digit year
EXPLAIN (COSTS OFF)
select c3, oracle_round(c3, 'SCC') from time_tbl;

-- select round with timestamp and format (result)
-- One greater than the first two digits of a four-digit year
select c3, oracle_round(c3, 'SCC') from time_tbl;

-- select round with timestamp and format (explain)
-- Year (rounds up on July 1)
EXPLAIN (COSTS OFF)
select c3, oracle_round(c3, 'SYYYY') from time_tbl;

-- select round with timestamp and format (result)
-- Year (rounds up on July 1)
select c3, oracle_round(c3, 'SYYYY') from time_tbl;

-- select round with timestamp and format (explain)
-- Year (rounds up on July 1)
EXPLAIN (COSTS OFF)
select c3, oracle_round(c3, 'YYYY') from time_tbl;

-- select round with timestamp and format (result)
-- Year (rounds up on July 1)
select c3, oracle_round(c3, 'YYYY') from time_tbl;

-- select round with timestamp and format (explain)
-- Year (rounds up on July 1)
EXPLAIN (COSTS OFF)
select c3, oracle_round(c3, 'YEAR') from time_tbl;

-- select round with timestamp and format (result)
-- Year (rounds up on July 1)
select c3, oracle_round(c3, 'YEAR') from time_tbl;

-- select round with timestamp and format (explain)
-- Year (rounds up on July 1)
EXPLAIN (COSTS OFF)
select c3, oracle_round(c3, 'SYEAR') from time_tbl;

-- select round with timestamp and format (result)
-- Year (rounds up on July 1)
select c3, oracle_round(c3, 'SYEAR') from time_tbl;

-- select round with timestamp and format (explain)
-- Year (rounds up on July 1)
EXPLAIN (COSTS OFF)
select c3, oracle_round(c3, 'YYY') from time_tbl;

-- select round with timestamp and format (result)
-- Year (rounds up on July 1)
select c3, oracle_round(c3, 'YYY') from time_tbl;

-- select round with timestamp and format (explain)
-- Year (rounds up on July 1)
EXPLAIN (COSTS OFF)
select c3, oracle_round(c3, 'YY') from time_tbl;

-- select round with timestamp and format (result)
-- Year (rounds up on July 1)
select c3, oracle_round(c3, 'YY') from time_tbl;

-- select round with timestamp and format (explain)
-- Year (rounds up on July 1)
EXPLAIN (COSTS OFF)
select c3, oracle_round(c3, 'Y') from time_tbl;

-- select round with timestamp and format (result)
-- Year (rounds up on July 1)
select c3, oracle_round(c3, 'Y') from time_tbl;

-- select round with timestamp and format (explain)
-- ISO Year
EXPLAIN (COSTS OFF)
select c3, oracle_round(c3, 'IYYY') from time_tbl;

-- select round with timestamp and format (result)
-- ISO Year
select c3, oracle_round(c3, 'IYYY') from time_tbl;

-- select round with timestamp and format (explain)
-- ISO Year
EXPLAIN (COSTS OFF)
select c3, oracle_round(c3, 'IYY') from time_tbl;

-- select round with timestamp and format (result)
-- ISO Year
select c3, oracle_round(c3, 'IYY') from time_tbl;

-- select round with timestamp and format (explain)
-- ISO Year
EXPLAIN (COSTS OFF)
select c3, oracle_round(c3, 'IY') from time_tbl;

-- select round with timestamp and format (result)
-- ISO Year
select c3, oracle_round(c3, 'IY') from time_tbl;

-- select round with timestamp and format (explain)
-- ISO Year
EXPLAIN (COSTS OFF)
select c3, oracle_round(c3, 'I') from time_tbl;

-- select round with timestamp and format (result)
-- ISO Year
select c3, oracle_round(c3, 'I') from time_tbl;

-- select round with timestamp and format (explain)
-- Quarter (rounds up on the sixteenth day of the second month of the quarter)
EXPLAIN (COSTS OFF)
select c3, oracle_round(c3, 'Q') from time_tbl;

-- select round with timestamp and format (result)
-- Quarter (rounds up on the sixteenth day of the second month of the quarter)
select c3, oracle_round(c3, 'Q') from time_tbl;

-- select round with timestamp and format (explain)
-- Month (rounds up on the sixteenth day)
EXPLAIN (COSTS OFF)
select c3, oracle_round(c3, 'MONTH') from time_tbl;

-- select round with timestamp and format (result)
-- Month (rounds up on the sixteenth day)
select c3, oracle_round(c3, 'MONTH') from time_tbl;

-- select round with timestamp and format (explain)
-- Month (rounds up on the sixteenth day)
EXPLAIN (COSTS OFF)
select c3, oracle_round(c3, 'MON') from time_tbl;

-- select round with timestamp and format (result)
-- Month (rounds up on the sixteenth day)
select c3, oracle_round(c3, 'MON') from time_tbl;

-- select round with timestamp and format (explain)
-- Month (rounds up on the sixteenth day)
EXPLAIN (COSTS OFF)
select c3, oracle_round(c3, 'MM') from time_tbl;

-- select round with timestamp and format (result)
-- Month (rounds up on the sixteenth day)
select c3, oracle_round(c3, 'MM') from time_tbl;

-- select round with timestamp and format (explain)
-- Month (rounds up on the sixteenth day)
EXPLAIN (COSTS OFF)
select c3, oracle_round(c3, 'RM') from time_tbl;

-- select round with timestamp and format (result)
-- Month (rounds up on the sixteenth day)
select c3, oracle_round(c3, 'RM') from time_tbl;

-- select round with timestamp and format (explain)
-- Same day of the week as the first day of the year
EXPLAIN (COSTS OFF)
select c3, oracle_round(c3, 'WW') from time_tbl;

-- select round with timestamp and format (result)
-- Same day of the week as the first day of the year
select c3, oracle_round(c3, 'WW') from time_tbl;

-- select round with timestamp and format (explain)
-- Same day of the week as the first day of the ISO year
EXPLAIN (COSTS OFF)
select c3, oracle_round(c3, 'IW') from time_tbl;

-- select round with timestamp and format (result)
-- Same day of the week as the first day of the ISO year
select c3, oracle_round(c3, 'IW') from time_tbl;

-- select round with timestamp and format (explain)
-- Same day of the week as the first day of the month
EXPLAIN (COSTS OFF)
select c3, oracle_round(c3, 'W') from time_tbl;

-- select round with timestamp and format (result)
-- Same day of the week as the first day of the month
select c3, oracle_round(c3, 'W') from time_tbl;

-- select round with timestamp and format (explain)
-- Day
EXPLAIN (COSTS OFF)
select c3, oracle_round(c3, 'DDD') from time_tbl;

-- select round with timestamp and format (result)
-- Day
select c3, oracle_round(c3, 'DDD') from time_tbl;

-- select round with timestamp and format (result)
-- Day
EXPLAIN (COSTS OFF)
select c3, oracle_round(c3, 'DD') from time_tbl;

-- select round with timestamp and format (result)
-- Day
select c3, oracle_round(c3, 'DD') from time_tbl;

-- select round with timestamp and format (explain)
-- Day
EXPLAIN (COSTS OFF)
select c3, oracle_round(c3, 'J') from time_tbl;

-- select round with timestamp and format (result)
-- Day
select c3, oracle_round(c3, 'J') from time_tbl;

-- select round with timestamp and format (explain)
-- Starting day of the week
EXPLAIN (COSTS OFF)
select c3, oracle_round(c3, 'DAY') from time_tbl;

-- select round with timestamp and format (result)
-- Starting day of the week
select c3, oracle_round(c3, 'DAY') from time_tbl;

-- select round with timestamp and format (explain)
-- Starting day of the week
EXPLAIN (COSTS OFF)
select c3, oracle_round(c3, 'DY') from time_tbl;

-- select round with timestamp and format (result)
-- Starting day of the week
select c3, oracle_round(c3, 'DY') from time_tbl;

-- select round with timestamp and format (explain)
-- Starting day of the week
EXPLAIN (COSTS OFF)
select c3, oracle_round(c3, 'D') from time_tbl;

-- select round with timestamp and format (result)
-- Starting day of the week
select c3, oracle_round(c3, 'D') from time_tbl;

-- select round with timestamp and format (explain)
-- Hour
EXPLAIN (COSTS OFF)
select c3, oracle_round(c3, 'HH') from time_tbl;

-- select round with timestamp and format (result)
-- Hour
select c3, oracle_round(c3, 'HH') from time_tbl;

-- select round with timestamp and format (explain)
-- Hour
EXPLAIN (COSTS OFF)
select c3, oracle_round(c3, 'HH12') from time_tbl;

-- select round with timestamp and format (result)
-- Hour
select c3, oracle_round(c3, 'HH12') from time_tbl;

-- select round with timestamp and format (explain)
-- Hour
EXPLAIN (COSTS OFF)
select c3, oracle_round(c3, 'HH24') from time_tbl;

-- select round with timestamp and format (result)
-- Hour
select c3, oracle_round(c3, 'HH24') from time_tbl;

-- select round with timestamp and format (explain)
-- Minute
EXPLAIN (COSTS OFF)
select c3, oracle_round(c3, 'MI') from time_tbl;

-- select round with timestamp and format (result)
-- Minute
select c3, oracle_round(c3, 'MI') from time_tbl;

--
-- End test for date/time function
--

--
-- Test for character function
--

-- CHR function
EXPLAIN (COSTS OFF)
SELECT id, CHR(id) FROM character_tbl;
SELECT id, CHR(id) FROM character_tbl;

EXPLAIN (COSTS OFF)
SELECT n, CHR(n) FROM character_tbl;
SELECT n, CHR(n) FROM character_tbl;

-- CHR fail if the input is not int
EXPLAIN (COSTS OFF)
SELECT fl, CHR(fl) FROM character_tbl;


-- REGEXP_REPLACE function
EXPLAIN (COSTS OFF)
SELECT vc, regexp_replace(vc, 'a') FROM character_tbl;
SELECT vc, regexp_replace(vc, 'a') FROM character_tbl;

EXPLAIN (COSTS OFF)
SELECT c, regexp_replace(c, 'e', 'Y') FROM character_tbl;
SELECT c, regexp_replace(c, 'e', 'Y') FROM character_tbl;

-- Oracle replaces all, however postgres only replace the first character.
-- To replace all on postgres, use 'g' argument.
EXPLAIN (COSTS OFF)
SELECT nc, REGEXP_REPLACE(nc, '(.)', '\1 ') FROM character_tbl;
SELECT nc, REGEXP_REPLACE(nc, '(.)', '\1 ') FROM character_tbl;

EXPLAIN (COSTS OFF)
SELECT lc, REGEXP_REPLACE (lc, '^(\S*)', 'FirstWord') FROM character_tbl;
SELECT lc, REGEXP_REPLACE (lc, '^(\S*)', 'FirstWord') FROM character_tbl;

EXPLAIN (COSTS OFF)
SELECT nvc, REGEXP_REPLACE (nvc, 'a|b|o|u', 'G') FROM character_tbl;
SELECT nvc, REGEXP_REPLACE (nvc, 'a|b|o|u', 'G') FROM character_tbl;

EXPLAIN (COSTS OFF)
SELECT nvc, REGEXP_REPLACE (nvc, 'a|b|u', 'G'), REGEXP_REPLACE (nvc, 'a|b|u', 'G', 1, 0, 'i') FROM character_tbl;
SELECT nvc, REGEXP_REPLACE (nvc, 'a|b|u', 'G'), REGEXP_REPLACE (nvc, 'a|b|u', 'G', 1, 0, 'i') FROM character_tbl;

EXPLAIN (COSTS OFF)
SELECT lc, REGEXP_REPLACE (lc, 'a|e', 'O', 4), REGEXP_REPLACE (lc, 'a|e', 'O', 8) FROM character_tbl;
SELECT lc, REGEXP_REPLACE (lc, 'a|e', 'O', 4), REGEXP_REPLACE (lc, 'a|e', 'O', 8) FROM character_tbl;

EXPLAIN (COSTS OFF)
SELECT lc, REGEXP_REPLACE (lc, 'a|e', 'O', 4, 1), REGEXP_REPLACE (lc, 'a|e', 'O', 8, 0) FROM character_tbl;
SELECT lc, REGEXP_REPLACE (lc, 'a|e', 'O', 4, 1), REGEXP_REPLACE (lc, 'a|e', 'O', 8, 0) FROM character_tbl;

-- Oracle does not support this argument, however postgresql does.
EXPLAIN (COSTS OFF)
SELECT vc, regexp_replace(vc, 'r(..)', 'X\1Y', 'g') FROM character_tbl;
SELECT vc, regexp_replace(vc, 'r(..)', 'X\1Y', 'g') FROM character_tbl;


-- TRIM function
EXPLAIN (COSTS OFF)
SELECT vc, TRIM(vc) FROM character_tbl;
SELECT vc, TRIM(vc) FROM character_tbl;

EXPLAIN (COSTS OFF)
SELECT c, TRIM(LEADING 'sf' FROM c), TRIM(TRAILING 'r' FROM c), TRIM(BOTH 'r' FROM c) FROM character_tbl;
SELECT c, TRIM(LEADING 'sf' FROM c), TRIM(TRAILING 'r' FROM c), TRIM(BOTH 'r' FROM c) FROM character_tbl;

EXPLAIN (COSTS OFF)
SELECT lc, TRIM('t' FROM lc) FROM character_tbl;
SELECT lc, TRIM('t' FROM lc) FROM character_tbl;

EXPLAIN (COSTS OFF)
SELECT lc, TRIM('' FROM lc) FROM character_tbl;
SELECT lc, TRIM('' FROM lc) FROM character_tbl;


-- ASCII function
EXPLAIN (COSTS OFF)
SELECT lc, ASCII(lc), ASCII(SUBSTR(lc, 1, 1)), ASCII(SUBSTR(lc, 3, 1)) FROM character_tbl;
SELECT lc, ASCII(lc), ASCII(SUBSTR(lc, 1, 1)), ASCII(SUBSTR(lc, 3, 1)) FROM character_tbl;

EXPLAIN (COSTS OFF)
SELECT nvc, ASCII(nvc) FROM character_tbl WHERE ASCII(c) > 100;
SELECT nvc, ASCII(nvc) FROM character_tbl WHERE ASCII(c) > 100;


-- GREATEST function
EXPLAIN (COSTS OFF)
SELECT GREATEST(id, 5, 500), GREATEST(id, 5, 85, NULL) FROM character_tbl WHERE GREATEST(id, 5, 85) != 0;
SELECT GREATEST(id, 5, 500), GREATEST(id, 5, 85, NULL) FROM character_tbl WHERE GREATEST(id, 5, 85) != 0;

EXPLAIN (COSTS OFF)
SELECT GREATEST(c, 'electronic', 'niko') FROM character_tbl;
SELECT GREATEST(c, 'electronic', 'niko') FROM character_tbl;


-- LEAST function
EXPLAIN (COSTS OFF)
SELECT LEAST(n, 5, 500, NULL), LEAST(n, 111, 185) FROM character_tbl WHERE LEAST(n, 5, 85) != 0;
SELECT LEAST(n, 5, 500, NULL), LEAST(n, 111, 185) FROM character_tbl WHERE LEAST(n, 5, 85) != 0;

EXPLAIN (COSTS OFF)
SELECT LEAST(nvc, 'Liquid', 'Johnny') FROM character_tbl;
SELECT LEAST(nvc, 'Liquid', 'Johnny') FROM character_tbl;


-- COALESCE function
EXPLAIN (COSTS OFF)
SELECT COALESCE(n, 5, 500, NULL), COALESCE(111, n) FROM character_tbl WHERE COALESCE(n, 5, 85) != 0;
SELECT COALESCE(n, 5, 500, NULL), COALESCE(111, n) FROM character_tbl WHERE COALESCE(n, 5, 85) != 0;

EXPLAIN (COSTS OFF)
SELECT COALESCE(1.2*id, n, 19) FROM character_tbl;
SELECT COALESCE(1.2*id, n, 19) FROM character_tbl;


-- NULLIF function
EXPLAIN (COSTS OFF)
SELECT NULLIF(n, 5), NULLIF(n, 111) FROM character_tbl WHERE NULLIF(n, 85) != 0;
SELECT NULLIF(n, 5), NULLIF(n, 111) FROM character_tbl WHERE NULLIF(n, 85) != 0;

EXPLAIN (COSTS OFF)
SELECT NULLIF(c, 'Fansipan') FROM character_tbl;
SELECT NULLIF(c, 'Fansipan') FROM character_tbl;


-- TO_CHAR (character) function
EXPLAIN (COSTS OFF)
SELECT TO_CHAR(nc), TO_CHAR(lc), TO_CHAR('113') FROM character_tbl;
SELECT TO_CHAR(nc), TO_CHAR(lc), TO_CHAR('113') FROM character_tbl;

EXPLAIN (COSTS OFF)
SELECT TO_CHAR('01110') FROM character_tbl;
SELECT TO_CHAR('01110') FROM character_tbl;

-- TO_CHAR (datetime) function
EXPLAIN (COSTS OFF)
SELECT TO_CHAR(itv, 'DD-MON-YYYY') FROM character_tbl;
SELECT TO_CHAR(itv, 'DD-MON-YYYY') FROM character_tbl;

EXPLAIN (COSTS OFF)
SELECT TO_CHAR(timetz, 'DD-MON-YYYY HH24:MI:SSxFF TZH:TZM') FROM character_tbl;
SELECT TO_CHAR(timetz, 'DD-MON-YYYY HH24:MI:SSxFF TZH:TZM') FROM character_tbl;

EXPLAIN (COSTS OFF)
SELECT TO_CHAR(dt, 'DD-MON-YYYY HH24:MI:SS') FROM character_tbl;
SELECT TO_CHAR(dt, 'DD-MON-YYYY HH24:MI:SS') FROM character_tbl;

EXPLAIN (COSTS OFF)
SELECT TO_CHAR(dt) FROM character_tbl;
SELECT TO_CHAR(dt) FROM character_tbl;

EXPLAIN (COSTS OFF)
SELECT TO_CHAR(TIMESTAMP'1999-12-01 10:00:00') FROM character_tbl;
SELECT TO_CHAR(TIMESTAMP'1999-12-01 10:00:00') FROM character_tbl;

EXPLAIN (COSTS OFF)
SELECT TO_CHAR(INTERVAL '10 days 21 hours') FROM character_tbl;
SELECT TO_CHAR(INTERVAL '10 days 21 hours') FROM character_tbl;

-- TO_TIMESTAMP function
EXPLAIN (COSTS OFF)
SELECT TO_TIMESTAMP(dt_text, 'DD-Mon-RR HH24:MI:SS.FF') FROM character_tbl;
SELECT TO_TIMESTAMP(dt_text, 'DD-Mon-RR HH24:MI:SS.FF') FROM character_tbl;

EXPLAIN (COSTS OFF)
SELECT TO_TIMESTAMP('05 Dec 2000', 'DD Mon YYYY') FROM character_tbl;
SELECT TO_TIMESTAMP('05 Dec 2000', 'DD Mon YYYY') FROM character_tbl;

--
-- End test for character function
--

DROP FOREIGN TABLE numeric_tbl;
DROP FOREIGN TABLE time_tbl;
DROP FOREIGN TABLE character_tbl;
DROP USER MAPPING FOR CURRENT_USER SERVER oracle_srv;
DROP SERVER oracle_srv;
DROP EXTENSION oracle_fdw CASCADE;
