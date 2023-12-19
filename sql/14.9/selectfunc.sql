--Testcase 1:
SET client_min_messages = WARNING;
--Testcase 2:
CREATE EXTENSION oracle_fdw;

--Testcase 3:
CREATE SERVER oracle_srv FOREIGN DATA WRAPPER oracle_fdw OPTIONS (dbserver '', isolation_level 'read_committed', nchar 'true');

--Testcase 4:
CREATE USER MAPPING FOR CURRENT_USER SERVER oracle_srv OPTIONS (user 'test', password 'test');

-- Init data for numeric function
--Testcase 5:
SELECT oracle_execute('oracle_srv', 'DROP TABLE test.numeric_tbl PURGE');

--Testcase 6:
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

--Testcase 7:
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

--Testcase 8:
INSERT INTO numeric_tbl VALUES (0, 'a', 0.1, 100, -0.1, -100, 0.1, 1.2, 5.0, 1 ,  '---XYZ---', '   abc   ', 'This is',           '2017-03-31 9:30:20', '12,345.6-');
--Testcase 9:
INSERT INTO numeric_tbl VALUES (1, 'a', 0.2, 100, -0.2, -100, 0.2, 2.3, 6.0, 2 ,  '---XYZ---', '   abc   ', 'the test string',   '2017-03-31 9:30:20', '12,345.6-');
--Testcase 10:
INSERT INTO numeric_tbl VALUES (2, 'a', 0.3, 100, -0.3, -100, 0.3, 3.4, 7.5, 3 ,  '---XYZ---', '   abc   ', 'containing space',  '2017-03-31 9:30:20', '12,345.6-');
--Testcase 11:
INSERT INTO numeric_tbl VALUES (3, 'b', 1.1, 200, -1.1, -200, 0.4, 4.5, 8.0, 1 ,  '---XYZ---', '   abc   ', 'between the words', '2017-03-31 9:30:20', '12,345.6-');
--Testcase 12:
INSERT INTO numeric_tbl VALUES (4, 'b', 2.2, 200, -2.2, -200, 0.5, 5.6, 9.0, 2 ,  '---XYZ---', '   abc   ', 'reserved string',   '2017-03-31 9:30:20', '12,345.6-');
--Testcase 13:
INSERT INTO numeric_tbl VALUES (5, 'b', 3.3, 200, -3.3, -200, 0.6, 6.7, 10.5, 3 , '---XYZ---', '   abc   ', 'reserved string2',  '2017-03-31 9:30:20', '12,345.6-');

--Testcase 14:
SELECT * FROM numeric_tbl;

--
-- Init data for date/Time function
--
--Testcase 15:
SELECT oracle_execute('oracle_srv', 'DROP TABLE test.time_tbl PURGE');

--Testcase 16:
SELECT oracle_execute(
          'oracle_srv',
          E'CREATE TABLE test.time_tbl (\n'
          '   id   NUMBER(5) PRIMARY KEY,\n'
          '   c1   TIMESTAMP WITH TIME ZONE,\n'
          '   c2   DATE,\n'
          '   c3   TIMESTAMP\n'
          ') SEGMENT CREATION IMMEDIATE'
       );

--Testcase 17:
CREATE FOREIGN TABLE time_tbl(id int OPTIONS (key 'yes'), 
                              c1 timestamp with time zone, 
                              c2 date, 
                              c3 timestamp)
  SERVER oracle_srv OPTIONS(table 'TIME_TBL');

--Testcase 18:
SELECT oracle_execute(
          'oracle_srv',
          E'INSERT INTO test.time_tbl VALUES (0, \n'
          ' TIMESTAMP ''2021-01-02 12:10:30.123456 +02:00'', \n'
          ' DATE ''2021-01-02'', \n'
          ' TIMESTAMP ''2021-01-03 12:10:30.123456'')'
        );
--Testcase 19:
SELECT oracle_execute(
          'oracle_srv',
          E'INSERT INTO time_tbl VALUES (1, \n'
          ' TIMESTAMP ''2021-01-01 23:12:12.654321 -03:00'', \n'
          ' DATE ''2021-01-01'', \n'
          ' TIMESTAMP ''2021-01-04 23:12:12.654321'')'
        );
--Testcase 20:
SELECT oracle_execute(
          'oracle_srv',
          E'INSERT INTO time_tbl VALUES (2, \n'
          ' TIMESTAMP ''2021-01-10 11:12:12.112233 +04:00'', \n'
          ' DATE ''2021-01-10'', \n'
          ' TIMESTAMP ''2021-01-05 11:12:12.112233'')'
        );
--Testcase 21:
SELECT oracle_execute(
          'oracle_srv',
          E'INSERT INTO time_tbl VALUES (3, \n'
          ' TIMESTAMP ''2021-01-15 05:59:59.654321 -05:00'', \n'
          ' DATE ''2021-01-15'', \n'
          ' TIMESTAMP ''2021-01-06 15:59:59.654321'')'
        );
--Testcase 22:
SELECT oracle_execute(
          'oracle_srv',
          E'INSERT INTO time_tbl VALUES (4, \n'
          ' TIMESTAMP ''2021-01-29 00:59:59.000102 +06:00'', \n'
          ' DATE ''2021-01-29'', \n'
          ' TIMESTAMP ''2021-01-07 00:59:59.000102'')'
        );

--Testcase 23:
SELECT * FROM time_tbl;

--
-- End init data for date/time function
--

--
-- Init data for character function
--

-- drop the Oracle tables if they exist
--Testcase 24:
SELECT oracle_execute('oracle_srv', 'DROP TABLE test.character_tbl PURGE');

--Testcase 25:
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

--Testcase 26:
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

--Testcase 27:
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

--Testcase 28:
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

--Testcase 29:
SELECT * FROM character_tbl;

--
-- End init data for character function
--

-- Test for Numeric

-- ===============================================================================
-- test abs()
-- ===============================================================================
-- select abs (builtin function, EXPLAIN (COSTS OFF))
--Testcase 30:
EXPLAIN (COSTS OFF)
SELECT abs(value1), abs(value2), abs(value3), abs(value4) FROM numeric_tbl;

-- select abs (buitin function, result)
--Testcase 31:
SELECT abs(value1), abs(value2), abs(value3), abs(value4) FROM numeric_tbl;

-- select abs (builtin function, not pushdown constraints, EXPLAIN (COSTS OFF))
--Testcase 32:
EXPLAIN (COSTS OFF)
SELECT abs(value1), abs(value2), abs(value3), abs(value4) FROM numeric_tbl WHERE to_hex(value2) != '64';

-- select abs (builtin function, not pushdown constraints, result)
--Testcase 33:
SELECT abs(value1), abs(value2), abs(value3), abs(value4) FROM numeric_tbl WHERE to_hex(value2) != '64';

-- select abs (builtin function, pushdown constraints, EXPLAIN (COSTS OFF))
--Testcase 34:
EXPLAIN (COSTS OFF)
SELECT abs(value1), abs(value2), abs(value3), abs(value4) FROM numeric_tbl WHERE value2 != 200;

-- select abs (builtin function, pushdown constraints, result)
--Testcase 35:
SELECT abs(value1), abs(value2), abs(value3), abs(value4) FROM numeric_tbl WHERE value2 != 200;

-- ===============================================================================
-- test acos()
-- ===============================================================================
-- select acos (builtin function, EXPLAIN (COSTS OFF))
--Testcase 36:
EXPLAIN (COSTS OFF)
SELECT value1, acos(value5), acos(0.5) FROM numeric_tbl;

-- select acos (builtin function, result)
--Testcase 37:
SELECT value1, acos(value5), acos(0.5) FROM numeric_tbl;

-- select acos (builtin function, not pushdown constraints, EXPLAIN (COSTS OFF))
--Testcase 38:
EXPLAIN (COSTS OFF)
SELECT value1, acos(value5), acos(0.5) FROM numeric_tbl WHERE to_hex(value2) = '64';

-- select acos (builtin function, not pushdown constraints, result)
--Testcase 39:
SELECT value1, acos(value5), acos(0.5) FROM numeric_tbl WHERE to_hex(value2) = '64';

-- select acos (builtin function, pushdown constraints, EXPLAIN (COSTS OFF))
--Testcase 40:
EXPLAIN (COSTS OFF)
SELECT value1, acos(value5), acos(0.5) FROM numeric_tbl WHERE value2 != 200;

-- select acos (builtin function, pushdown constraints, result)
--Testcase 41:
SELECT value1, acos(value5), acos(0.5) FROM numeric_tbl WHERE value2 != 200;

-- select acos (builtin function, acos in constraints, EXPLAIN (COSTS OFF))
--Testcase 42:
EXPLAIN (COSTS OFF)
SELECT value1, acos(value5), acos(0.5) FROM numeric_tbl WHERE acos(value5) != 1;

-- select acos (builtin function, acos in constraints, result)
--Testcase 43:
SELECT value1, acos(value5), acos(0.5) FROM numeric_tbl WHERE acos(value5) != 1;

-- select acos (builtin function, acos in constraints, EXPLAIN (COSTS OFF))
--Testcase 44:
EXPLAIN (COSTS OFF)
SELECT value1, acos(value5), acos(0.5) FROM numeric_tbl WHERE acos(0.5) > value1;

-- select acos (builtin function, acos in constraints, result)
--Testcase 45:
SELECT value1, acos(value5), acos(0.5) FROM numeric_tbl WHERE acos(0.5) > value1;

-- select acos as nest function with agg (pushdown, EXPLAIN (COSTS OFF))
--Testcase 46:
EXPLAIN (COSTS OFF)
SELECT sum(value3),acos(sum(value1)) FROM numeric_tbl WHERE value2 != 200;

-- select acos as nest function with agg (pushdown, result)
--Testcase 47:
SELECT sum(value3),acos(sum(value1)) FROM numeric_tbl WHERE value2 != 200;

-- select acos as nest with log2 (pushdown, EXPLAIN (COSTS OFF))
-- should return failure because input is out of range
--Testcase 48:
EXPLAIN (COSTS OFF)
SELECT value1, acos(log(2, value2)) FROM numeric_tbl;

-- select acos as nest with log2 (pushdown, result)
--Testcase 49:
SELECT value1, acos(log(2, value2)) FROM numeric_tbl;

-- select acos with non pushdown func and explicit constant (EXPLAIN (COSTS OFF))
--Testcase 50:
EXPLAIN (COSTS OFF)
SELECT acos(value5), pi(), 4.1 FROM numeric_tbl;

-- select acos with non pushdown func and explicit constant (result)
--Testcase 51:
SELECT acos(value5), pi(), 4.1 FROM numeric_tbl;

-- select acos with order by (EXPLAIN (COSTS OFF))
--Testcase 52:
EXPLAIN (COSTS OFF)
SELECT value1, acos(1-value5) FROM numeric_tbl ORDER BY acos(1-value5);

-- select acos with order by (result)
--Testcase 53:
SELECT value1, acos(1-value5) FROM numeric_tbl ORDER BY acos(1-value5);

-- select acos with order by index (result)
--Testcase 54:
SELECT value1, acos(1-value5) FROM numeric_tbl ORDER BY 2,1;

-- select acos with order by index (result)
--Testcase 55:
SELECT value1, acos(1-value5) FROM numeric_tbl ORDER BY 1,2;

-- select acos with group by (EXPLAIN (COSTS OFF))
--Testcase 56:
EXPLAIN (COSTS OFF)
SELECT count(value1), acos(1-value5) FROM numeric_tbl GROUP BY acos(1-value5);

-- select acos with group by (result)
--Testcase 57:
SELECT count(value1), acos(1-value5) FROM numeric_tbl GROUP BY acos(1-value5);

-- select acos with group by index (result)
--Testcase 58:
SELECT value1, acos(1-value5) FROM numeric_tbl GROUP BY 2,1;

-- select acos with group by index (result)
--Testcase 59:
SELECT value1, acos(1-value5) FROM numeric_tbl GROUP BY 1,2;

-- select acos with group by having (EXPLAIN (COSTS OFF))
--Testcase 60:
EXPLAIN (COSTS OFF)
SELECT count(value1), acos(1-value5) FROM numeric_tbl GROUP BY acos(1-value5) HAVING avg(value1) > 0;

-- select acos with group by having (result)
--Testcase 61:
SELECT count(value1), acos(1-value5) FROM numeric_tbl GROUP BY acos(1-value5) HAVING avg(value1) > 0;

-- select acos with group by index having (result)
--Testcase 62:
SELECT value1, acos(1-value5) FROM numeric_tbl GROUP BY 2,1 HAVING acos(1-value5) > 0;

-- select acos with group by index having (result)
--Testcase 63:
SELECT value1, acos(1-value5) FROM numeric_tbl GROUP BY 1,2 HAVING value1 > 1;

-- select acos and as
--Testcase 64:
SELECT acos(value5) as acos1 FROM numeric_tbl;

-- ===============================================================================
-- test asin()
-- ===============================================================================
-- select asin (builtin function, EXPLAIN (COSTS OFF))
--Testcase 65:
EXPLAIN (COSTS OFF)
SELECT value1, asin(value5), asin(0.5) FROM numeric_tbl;

-- select asin (builtin function, result)
--Testcase 66:
SELECT value1, asin(value5), asin(0.5) FROM numeric_tbl;

-- select asin (builtin function, not pushdown constraints, EXPLAIN (COSTS OFF))
--Testcase 67:
EXPLAIN (COSTS OFF)
SELECT value1, asin(value5), asin(0.5) FROM numeric_tbl WHERE to_hex(value2) = '64';

-- select asin (builtin function, not pushdown constraints, result)
--Testcase 68:
SELECT value1, asin(value5), asin(0.5) FROM numeric_tbl WHERE to_hex(value2) = '64';

-- select asin (builtin function, pushdown constraints, EXPLAIN (COSTS OFF))
--Testcase 69:
EXPLAIN (COSTS OFF)
SELECT value1, asin(value5), asin(0.5) FROM numeric_tbl WHERE value2 != 200;

-- select asin (builtin function, pushdown constraints, result)
--Testcase 70:
SELECT value1, asin(value5), asin(0.5) FROM numeric_tbl WHERE value2 != 200;

-- select asin (builtin function, asin in constraints, EXPLAIN (COSTS OFF))
--Testcase 71:
EXPLAIN (COSTS OFF)
SELECT value1, asin(value5), asin(0.5) FROM numeric_tbl WHERE value1 != 1;

-- select asin (builtin function, asin in constraints, result)
--Testcase 72:
SELECT value1, asin(value5), asin(0.5) FROM numeric_tbl WHERE value1 != 1;

-- select asin (builtin function, asin in constraints, EXPLAIN (COSTS OFF))
--Testcase 73:
EXPLAIN (COSTS OFF)
SELECT value1, asin(value5), asin(0.5) FROM numeric_tbl WHERE asin(0.5) > value1;

-- select asin (builtin function, asin in constraints, result)
--Testcase 74:
SELECT value1, asin(value5), asin(0.5) FROM numeric_tbl WHERE asin(0.5) > value1;

-- select asin as nest function with agg (pushdown, EXPLAIN (COSTS OFF))
--Testcase 75:
EXPLAIN (COSTS OFF)
SELECT sum(value3),asin(sum(value1)) FROM numeric_tbl WHERE value2 != 200;

-- select asin as nest function with agg (pushdown, result)
--Testcase 76:
SELECT sum(value3),asin(sum(value1)) FROM numeric_tbl WHERE value2 != 200;

-- select asin as nest with log2 (pushdown, EXPLAIN (COSTS OFF))
--Testcase 77:
EXPLAIN (COSTS OFF)
SELECT value1, asin(log(2, value2)) FROM numeric_tbl;

-- select asin as nest with log2 (pushdown, result)
--Testcase 78:
SELECT value1, asin(log(2, value2)) FROM numeric_tbl;

-- select asin with non pushdown func and explicit constant (EXPLAIN (COSTS OFF))
--Testcase 79:
EXPLAIN (COSTS OFF)
SELECT value1, asin(value5), pi(), 4.1 FROM numeric_tbl;

-- select asin with non pushdown func and explicit constant (result)
--Testcase 80:
SELECT value1, asin(value5), pi(), 4.1 FROM numeric_tbl;

-- select asin with order by (EXPLAIN (COSTS OFF))
--Testcase 81:
EXPLAIN (COSTS OFF)
SELECT value1, asin(1-value5) FROM numeric_tbl ORDER BY asin(1-value5);

-- select asin with order by (result)
--Testcase 82:
SELECT value1, asin(1-value5) FROM numeric_tbl ORDER BY asin(1-value5);

-- select asin with order by index (result)
--Testcase 83:
SELECT value1, asin(1-value5) FROM numeric_tbl ORDER BY 2,1;

-- select asin with order by index (result)
--Testcase 84:
SELECT value1, asin(1-value5) FROM numeric_tbl ORDER BY 1,2;

-- select asin with group by (EXPLAIN (COSTS OFF))
--Testcase 85:
EXPLAIN (COSTS OFF)
SELECT value1, asin(1-value5) FROM numeric_tbl GROUP BY value1, asin(1-value5);

-- select asin with group by (result)
--Testcase 86:
SELECT value1, asin(1-value5) FROM numeric_tbl GROUP BY value1, asin(1-value5);

-- select asin with group by index (result)
--Testcase 87:
SELECT value1, asin(1-value5) FROM numeric_tbl GROUP BY 2,1;

-- select asin with group by index (result)
--Testcase 88:
SELECT value1, asin(1-value5) FROM numeric_tbl GROUP BY 1,2;

-- select asin with group by having (EXPLAIN (COSTS OFF))
--Testcase 89:
EXPLAIN (COSTS OFF)
SELECT value1, asin(1-value5) FROM numeric_tbl GROUP BY value1, asin(1-value5) HAVING avg(value1) > 0;

-- select asin with group by having (result)
--Testcase 90:
SELECT value1, asin(1-value5) FROM numeric_tbl GROUP BY value1, asin(1-value5) HAVING avg(value1) > 0;

-- select asin with group by index having (result)
--Testcase 91:
SELECT value1, asin(1-value5) FROM numeric_tbl GROUP BY 2,1 HAVING asin(1-value5) > 0;

-- select asin with group by index having (result)
--Testcase 92:
SELECT value1, asin(1-value5) FROM numeric_tbl GROUP BY 1,2 HAVING value1 > 1;

-- select asin and as
--Testcase 93:
SELECT value1, asin(value5) as asin1 FROM numeric_tbl;

-- ===============================================================================
-- test atan()
-- ===============================================================================
-- select atan (builtin function, EXPLAIN (COSTS OFF))
--Testcase 94:
EXPLAIN (COSTS OFF)
SELECT atan(value1), atan(value2), atan(value3), atan(value4), atan(0.5) FROM numeric_tbl;

-- select atan (builtin function, result)
--Testcase 95:
SELECT atan(value1), atan(value2), atan(value3), atan(value4), atan(0.5) FROM numeric_tbl;

-- select atan (builtin function, not pushdown constraints, EXPLAIN (COSTS OFF))
--Testcase 96:
EXPLAIN (COSTS OFF)
SELECT atan(value1), atan(value2), atan(value3), atan(value4), atan(0.5) FROM numeric_tbl WHERE to_hex(value2) = '64';

-- select atan (builtin function, not pushdown constraints, result)
--Testcase 97:
SELECT atan(value1), atan(value2), atan(value3), atan(value4), atan(0.5) FROM numeric_tbl WHERE to_hex(value2) = '64';

-- select atan (builtin function, pushdown constraints, EXPLAIN (COSTS OFF))
--Testcase 98:
EXPLAIN (COSTS OFF)
SELECT atan(value1), atan(value2), atan(value3), atan(value4), atan(0.5) FROM numeric_tbl WHERE value2 != 200;

-- select atan (builtin function, pushdown constraints, result)
--Testcase 99:
SELECT atan(value1), atan(value2), atan(value3), atan(value4), atan(0.5) FROM numeric_tbl WHERE value2 != 200;

-- select atan (builtin function, atan in constraints, EXPLAIN (COSTS OFF))
--Testcase 100:
EXPLAIN (COSTS OFF)
SELECT atan(value1), atan(value2), atan(value3), atan(value4), atan(0.5) FROM numeric_tbl WHERE atan(value1) != 1;

-- select atan (builtin function, atan in constraints, result)
--Testcase 101:
SELECT atan(value1), atan(value2), atan(value3), atan(value4), atan(0.5) FROM numeric_tbl WHERE atan(value1) != 1;

-- select atan (builtin function, atan in constraints, EXPLAIN (COSTS OFF))
--Testcase 102:
EXPLAIN (COSTS OFF)
SELECT atan(value1), atan(value2), atan(value3), atan(value4), atan(0.5) FROM numeric_tbl WHERE atan(0.5) > value1;

-- select atan (builtin function, atan in constraints, result)
--Testcase 103:
SELECT atan(value1), atan(value2), atan(value3), atan(value4), atan(0.5) FROM numeric_tbl WHERE atan(0.5) > value1;

-- select atan as nest function with agg (pushdown, EXPLAIN (COSTS OFF))
--Testcase 104:
EXPLAIN (COSTS OFF)
SELECT sum(value3),atan(sum(value3)) FROM numeric_tbl;

-- select atan as nest function with agg (pushdown, result)
--Testcase 105:
SELECT sum(value3),atan(sum(value3)) FROM numeric_tbl;

-- select atan as nest with log2 (pushdown, EXPLAIN (COSTS OFF))
--Testcase 106:
EXPLAIN (COSTS OFF)
SELECT atan(log(2, value2)) FROM numeric_tbl;

-- select atan as nest with log2 (pushdown, result)
--Testcase 107:
SELECT atan(log(2, value2)) FROM numeric_tbl;

-- select atan with non pushdown func and explicit constant (EXPLAIN (COSTS OFF))
--Testcase 108:
EXPLAIN (COSTS OFF)
SELECT atan(value3), pi(), 4.1 FROM numeric_tbl;

-- select atan with non pushdown func and explicit constant (result)
--Testcase 109:
SELECT atan(value3), pi(), 4.1 FROM numeric_tbl;

-- select atan with order by (EXPLAIN (COSTS OFF))
--Testcase 110:
EXPLAIN (COSTS OFF)
SELECT value1, atan(1-value3) FROM numeric_tbl ORDER BY atan(1-value3);

-- select atan with order by (result)
--Testcase 111:
SELECT value1, atan(1-value3) FROM numeric_tbl ORDER BY atan(1-value3);

-- select atan with order by index (result)
--Testcase 112:
SELECT value1, atan(1-value3) FROM numeric_tbl ORDER BY 2,1;

-- select atan with order by index (result)
--Testcase 113:
SELECT value1, atan(1-value3) FROM numeric_tbl ORDER BY 1,2;

-- select atan with group by (EXPLAIN (COSTS OFF))
--Testcase 114:
EXPLAIN (COSTS OFF)
SELECT count(value1), atan(1-value3) FROM numeric_tbl GROUP BY atan(1-value3);

-- select atan with group by (result)
--Testcase 115:
SELECT count(value1), atan(1-value3) FROM numeric_tbl GROUP BY atan(1-value3);

-- select atan with group by index (result)
--Testcase 116:
SELECT value1, atan(1-value3) FROM numeric_tbl GROUP BY 2,1;

-- select atan with group by index (result)
--Testcase 117:
SELECT value1, atan(1-value3) FROM numeric_tbl GROUP BY 1,2;

-- select atan with group by having (EXPLAIN (COSTS OFF))
--Testcase 118:
EXPLAIN (COSTS OFF)
SELECT count(value1), atan(1-value3) FROM numeric_tbl GROUP BY atan(1-value3) HAVING atan(avg(value1)) > 0;

-- select atan with group by having (result)
--Testcase 119:
SELECT count(value1), atan(1-value3) FROM numeric_tbl GROUP BY atan(1-value3) HAVING atan(avg(value1)) > 0;

-- select atan with group by index having (result)
--Testcase 120:
SELECT value1, atan(1-value3) FROM numeric_tbl GROUP BY 2,1 HAVING atan(1-value3) > 0;

-- select atan with group by index having (result)
--Testcase 121:
SELECT value1, atan(1-value3) FROM numeric_tbl GROUP BY 1,2 HAVING value1 > 1;

-- select atan and as
--Testcase 122:
SELECT atan(value3) as atan1 FROM numeric_tbl;

-- ===============================================================================
-- test atan2()
-- ===============================================================================
-- select atan2 (builtin function, EXPLAIN (COSTS OFF))
--Testcase 123:
EXPLAIN (COSTS OFF)
SELECT atan2(value1, 2), atan2(value2, 2), atan2(value3, 2), atan2(value4, 2), atan2(5, 2) FROM numeric_tbl;

-- select atan2 (builtin function, result)
--Testcase 124:
SELECT atan2(value1, 2), atan2(value2, 2), atan2(value3, 2), atan2(value4, 2), atan2(5, 2) FROM numeric_tbl;

-- select atan2 (builtin function, not pushdown constraints, EXPLAIN (COSTS OFF))
--Testcase 125:
EXPLAIN (COSTS OFF)
SELECT atan2(value1, 2), atan2(value2, 2), atan2(value3, 2), atan2(value4, 2), atan2(5, 2) FROM numeric_tbl WHERE to_hex(value2) = '64';

-- select atan2 (builtin function, not pushdown constraints, result)
--Testcase 126:
SELECT atan2(value1, 2), atan2(value2, 2), atan2(value3, 2), atan2(value4, 2), atan2(5, 2) FROM numeric_tbl WHERE to_hex(value2) = '64';

-- select atan2 (builtin function, pushdown constraints, EXPLAIN (COSTS OFF))
--Testcase 127:
EXPLAIN (COSTS OFF)
SELECT atan2(value1, 2), atan2(value2, 2), atan2(value3, 2), atan2(value4, 2), atan2(5, 2) FROM numeric_tbl WHERE value2 != 200;

-- select atan2 (builtin function, pushdown constraints, result)
--Testcase 128:
SELECT atan2(value1, 2), atan2(value2, 2), atan2(value3, 2), atan2(value4, 2), atan2(5, 2) FROM numeric_tbl WHERE value2 != 200;

-- select atan2 (builtin function, atan2 in constraints, EXPLAIN (COSTS OFF))
--Testcase 129:
EXPLAIN (COSTS OFF)
SELECT atan2(value1, 2), atan2(value2, 2), atan2(value3, 2), atan2(value4, 2), atan2(5, 2) FROM numeric_tbl WHERE atan2(value1, 2) != 1;

-- select atan2 (builtin function, atan2 in constraints, result)
--Testcase 130:
SELECT atan2(value1, 2), atan2(value2, 2), atan2(value3, 2), atan2(value4, 2), atan2(5, 2) FROM numeric_tbl WHERE atan2(value1, 2) != 1;

-- select atan2 (builtin function, atan2 in constraints, EXPLAIN (COSTS OFF))
--Testcase 131:
EXPLAIN (COSTS OFF)
SELECT atan2(value1, 2), atan2(value2, 2), atan2(value3, 2), atan2(value4, 2), atan2(5, 2) FROM numeric_tbl WHERE atan2(5, 2) > value1;

-- select atan2 (builtin function, atan2 in constraints, result)
--Testcase 132:
SELECT atan2(value1, 2), atan2(value2, 2), atan2(value3, 2), atan2(value4, 2), atan2(5, 2) FROM numeric_tbl WHERE atan2(5, 2) > value1;

-- select atan2 as nest function with agg (pushdown, EXPLAIN (COSTS OFF))
--Testcase 133:
EXPLAIN (COSTS OFF)
SELECT sum(value3),atan2(sum(value3), 2) FROM numeric_tbl;

-- select atan2 as nest function with agg (pushdown, result)
--Testcase 134:
SELECT sum(value3),atan2(sum(value3), 2) FROM numeric_tbl;

-- select atan2 as nest with log2 (pushdown, EXPLAIN (COSTS OFF))
--Testcase 135:
EXPLAIN (COSTS OFF)
SELECT atan2(log(2, value2), 2) FROM numeric_tbl;

-- select atan2 as nest with log2 (pushdown, result)
--Testcase 136:
SELECT atan2(log(2, value2), 2) FROM numeric_tbl;

-- select atan2 with non pushdown func and atan2licit constant (EXPLAIN (COSTS OFF))
--Testcase 137:
EXPLAIN (COSTS OFF)
SELECT atan2(value3, 2), pi(), 4.1 FROM numeric_tbl;

-- select atan2 with non pushdown func and atan2licit constant (result)
--Testcase 138:
SELECT atan2(value3, 2), pi(), 4.1 FROM numeric_tbl;

-- select atan2 with order by (EXPLAIN (COSTS OFF))
--Testcase 139:
EXPLAIN (COSTS OFF)
SELECT value1, atan2(1-value3, 2) FROM numeric_tbl ORDER BY atan2(1-value3, 2);

-- select atan2 with order by (result)
--Testcase 140:
SELECT value1, atan2(1-value3, 2) FROM numeric_tbl ORDER BY atan2(1-value3, 2);

-- select atan2 with order by index (result)
--Testcase 141:
SELECT value1, atan2(1-value3, 2) FROM numeric_tbl ORDER BY 2,1;

-- select atan2 with order by index (result)
--Testcase 142:
SELECT value1, atan2(1-value3, 2) FROM numeric_tbl ORDER BY 1,2;

-- select atan2 with group by (EXPLAIN (COSTS OFF))
--Testcase 143:
EXPLAIN (COSTS OFF)
SELECT count(value1), atan2(1-value3, 2) FROM numeric_tbl GROUP BY atan2(1-value3, 2);

-- select atan2 with group by (result)
--Testcase 144:
SELECT count(value1), atan2(1-value3, 2) FROM numeric_tbl GROUP BY atan2(1-value3, 2);

-- select atan2 with group by index (result)
--Testcase 145:
SELECT value1, atan2(1-value3, 2) FROM numeric_tbl GROUP BY 2,1;

-- select atan2 with group by index (result)
--Testcase 146:
SELECT value1, atan2(1-value3, 2) FROM numeric_tbl GROUP BY 1,2;

-- select atan2 with group by having (EXPLAIN (COSTS OFF))
--Testcase 147:
EXPLAIN (COSTS OFF)
SELECT count(value1), atan2(1-value3, 2) FROM numeric_tbl GROUP BY atan2(1-value3, 2) HAVING atan2(avg(value1), 2) > 0;

-- select atan2 with group by having (result)
--Testcase 148:
SELECT count(value1), atan2(1-value3, 2) FROM numeric_tbl GROUP BY atan2(1-value3, 2) HAVING atan2(avg(value1), 2) > 0;

-- select atan2 with group by index having (result)
--Testcase 149:
SELECT value1, atan2(1-value3, 2) FROM numeric_tbl GROUP BY 2,1 HAVING atan2(1-value3, 2) > 0;

-- select atan2 with group by index having (result)
--Testcase 150:
SELECT value1, atan2(1-value3, 2) FROM numeric_tbl GROUP BY 1,2 HAVING value1 > 1;

-- select atan2 and as
--Testcase 151:
SELECT atan2(value3, 2) as atan21 FROM numeric_tbl;

-- ===============================================================================
-- test ceil()
-- ===============================================================================
-- select ceil (builtin function, EXPLAIN (COSTS OFF))
--Testcase 152:
EXPLAIN (COSTS OFF)
SELECT ceil(value1), ceil(value2), ceil(value3), ceil(value4), ceil(0.5) FROM numeric_tbl;

-- select ceil (builtin function, result)
--Testcase 153:
SELECT ceil(value1), ceil(value2), ceil(value3), ceil(value4), ceil(0.5) FROM numeric_tbl;

-- select ceil (builtin function, not pushdown constraints, EXPLAIN (COSTS OFF))
--Testcase 154:
EXPLAIN (COSTS OFF)
SELECT ceil(value1), ceil(value2), ceil(value3), ceil(value4), ceil(0.5) FROM numeric_tbl WHERE to_hex(value2) = '64';

-- select ceil (builtin function, not pushdown constraints, result)
--Testcase 155:
SELECT ceil(value1), ceil(value2), ceil(value3), ceil(value4), ceil(0.5) FROM numeric_tbl WHERE to_hex(value2) = '64';

-- select ceil (builtin function, pushdown constraints, EXPLAIN (COSTS OFF))
--Testcase 156:
EXPLAIN (COSTS OFF)
SELECT ceil(value1), ceil(value2), ceil(value3), ceil(value4), ceil(0.5) FROM numeric_tbl WHERE value2 != 200;

-- select ceil (builtin function, pushdown constraints, result)
--Testcase 157:
SELECT ceil(value1), ceil(value2), ceil(value3), ceil(value4), ceil(0.5) FROM numeric_tbl WHERE value2 != 200;

-- select ceil (builtin function, ceil in constraints, EXPLAIN (COSTS OFF))
--Testcase 158:
EXPLAIN (COSTS OFF)
SELECT ceil(value1), ceil(value2), ceil(value3), ceil(value4), ceil(0.5) FROM numeric_tbl WHERE ceil(value1) != 1;

-- select ceil (builtin function, ceil in constraints, result)
--Testcase 159:
SELECT ceil(value1), ceil(value2), ceil(value3), ceil(value4), ceil(0.5) FROM numeric_tbl WHERE ceil(value1) != 1;

-- select ceil (builtin function, ceil in constraints, EXPLAIN (COSTS OFF))
--Testcase 160:
EXPLAIN (COSTS OFF)
SELECT ceil(value1), ceil(value2), ceil(value3), ceil(value4), ceil(0.5) FROM numeric_tbl WHERE ceil(0.5) > value1;

-- select ceil (builtin function, ceil in constraints, result)
--Testcase 161:
SELECT ceil(value1), ceil(value2), ceil(value3), ceil(value4), ceil(0.5) FROM numeric_tbl WHERE ceil(0.5) > value1;

-- select ceil as nest function with agg (pushdown, EXPLAIN (COSTS OFF))
--Testcase 162:
EXPLAIN (COSTS OFF)
SELECT sum(value3),ceil(sum(value3)) FROM numeric_tbl;

-- select ceil as nest function with agg (pushdown, result)
--Testcase 163:
SELECT sum(value3),ceil(sum(value3)) FROM numeric_tbl;

-- select ceil as nest with log2 (pushdown, EXPLAIN (COSTS OFF))
--Testcase 164:
EXPLAIN (COSTS OFF)
SELECT ceil(log(2, value2)) FROM numeric_tbl;

-- select ceil as nest with log2 (pushdown, result)
--Testcase 165:
SELECT ceil(log(2, value2)) FROM numeric_tbl;

-- select ceil with non pushdown func and explicit constant (EXPLAIN (COSTS OFF))
--Testcase 166:
EXPLAIN (COSTS OFF)
SELECT ceil(value3), pi(), 4.1 FROM numeric_tbl;

-- select ceil with non pushdown func and explicit constant (result)
--Testcase 167:
SELECT ceil(value3), pi(), 4.1 FROM numeric_tbl;

-- select ceil with order by (EXPLAIN (COSTS OFF))
--Testcase 168:
EXPLAIN (COSTS OFF)
SELECT value1, ceil(1-value3) FROM numeric_tbl ORDER BY ceil(1-value3);

-- select ceil with order by (result)
--Testcase 169:
SELECT value1, ceil(1-value3) FROM numeric_tbl ORDER BY ceil(1-value3);

-- select ceil with order by index (result)
--Testcase 170:
SELECT value1, ceil(1-value3) FROM numeric_tbl ORDER BY 2,1;

-- select ceil with order by index (result)
--Testcase 171:
SELECT value1, ceil(1-value3) FROM numeric_tbl ORDER BY 1,2;

-- select ceil with group by (EXPLAIN (COSTS OFF))
--Testcase 172:
EXPLAIN (COSTS OFF)
SELECT value1, ceil(1-value3) FROM numeric_tbl GROUP BY value1, ceil(1-value3);

-- select ceil with group by (result)
--Testcase 173:
SELECT value1, ceil(1-value3) FROM numeric_tbl GROUP BY value1, ceil(1-value3);

-- select ceil with group by index (result)
--Testcase 174:
SELECT value1, ceil(1-value3) FROM numeric_tbl GROUP BY 2,1;

-- select ceil with group by index (result)
--Testcase 175:
SELECT value1, ceil(1-value3) FROM numeric_tbl GROUP BY 1,2;

-- select ceil with group by having (EXPLAIN (COSTS OFF))
--Testcase 176:
EXPLAIN (COSTS OFF)
SELECT value1, ceil(1-value3) FROM numeric_tbl GROUP BY value1, ceil(1-value3) HAVING ceil(avg(value1)) > 0;

-- select ceil with group by having (result)
--Testcase 177:
SELECT value1, ceil(1-value3) FROM numeric_tbl GROUP BY value1, ceil(1-value3) HAVING ceil(avg(value1)) > 0;

-- select ceil with group by index having (result)
--Testcase 178:
SELECT value1, ceil(1-value3) FROM numeric_tbl GROUP BY 2,1 HAVING ceil(1-value3) > 0;

-- select ceil with group by index having (result)
--Testcase 179:
SELECT value1, ceil(1-value3) FROM numeric_tbl GROUP BY 1,2 HAVING value1 > 1;

-- select ceil and as
--Testcase 180:
SELECT ceil(value3) as ceil1 FROM numeric_tbl;

-- ===============================================================================
-- test ceiling()
-- ===============================================================================
-- select ceiling (builtin function, EXPLAIN (COSTS OFF))
--Testcase 181:
EXPLAIN (COSTS OFF)
SELECT ceiling(value1), ceiling(value2), ceiling(value3), ceiling(value4), ceiling(0.5) FROM numeric_tbl;

-- select ceiling (builtin function, result)
--Testcase 182:
SELECT ceiling(value1), ceiling(value2), ceiling(value3), ceiling(value4), ceiling(0.5) FROM numeric_tbl;

-- select ceiling (builtin function, not pushdown constraints, EXPLAIN (COSTS OFF))
--Testcase 183:
EXPLAIN (COSTS OFF)
SELECT ceiling(value1), ceiling(value2), ceiling(value3), ceiling(value4), ceiling(0.5) FROM numeric_tbl WHERE to_hex(value2) = '64';

-- select ceiling (builtin function, not pushdown constraints, result)
--Testcase 184:
SELECT ceiling(value1), ceiling(value2), ceiling(value3), ceiling(value4), ceiling(0.5) FROM numeric_tbl WHERE to_hex(value2) = '64';

-- select ceiling (builtin function, pushdown constraints, EXPLAIN (COSTS OFF))
--Testcase 185:
EXPLAIN (COSTS OFF)
SELECT ceiling(value1), ceiling(value2), ceiling(value3), ceiling(value4), ceiling(0.5) FROM numeric_tbl WHERE value2 != 200;

-- select ceiling (builtin function, pushdown constraints, result)
--Testcase 186:
SELECT ceiling(value1), ceiling(value2), ceiling(value3), ceiling(value4), ceiling(0.5) FROM numeric_tbl WHERE value2 != 200;

-- select ceiling (builtin function, ceiling in constraints, EXPLAIN (COSTS OFF))
--Testcase 187:
EXPLAIN (COSTS OFF)
SELECT ceiling(value1), ceiling(value2), ceiling(value3), ceiling(value4), ceiling(0.5) FROM numeric_tbl WHERE ceiling(value1) != 1;

-- select ceiling (builtin function, ceiling in constraints, result)
--Testcase 188:
SELECT ceiling(value1), ceiling(value2), ceiling(value3), ceiling(value4), ceiling(0.5) FROM numeric_tbl WHERE ceiling(value1) != 1;

-- select ceiling (builtin function, ceiling in constraints, EXPLAIN (COSTS OFF))
--Testcase 189:
EXPLAIN (COSTS OFF)
SELECT ceiling(value1), ceiling(value2), ceiling(value3), ceiling(value4), ceiling(0.5) FROM numeric_tbl WHERE ceiling(0.5) > value1;

-- select ceiling (builtin function, ceiling in constraints, result)
--Testcase 190:
SELECT ceiling(value1), ceiling(value2), ceiling(value3), ceiling(value4), ceiling(0.5) FROM numeric_tbl WHERE ceiling(0.5) > value1;

-- select ceiling as nest function with agg (pushdown, EXPLAIN (COSTS OFF))
--Testcase 191:
EXPLAIN (COSTS OFF)
SELECT sum(value3),ceiling(sum(value3)) FROM numeric_tbl;

-- select ceiling as nest function with agg (pushdown, result)
--Testcase 192:
SELECT sum(value3),ceiling(sum(value3)) FROM numeric_tbl;

-- select ceiling as nest with log2 (pushdown, EXPLAIN (COSTS OFF))
--Testcase 193:
EXPLAIN (COSTS OFF)
SELECT ceiling(log(2, value2)) FROM numeric_tbl;

-- select ceiling as nest with log2 (pushdown, result)
--Testcase 194:
SELECT ceiling(log(2, value2)) FROM numeric_tbl;

-- select ceiling with non pushdown func and explicit constant (EXPLAIN (COSTS OFF))
--Testcase 195:
EXPLAIN (COSTS OFF)
SELECT ceiling(value3), pi(), 4.1 FROM numeric_tbl;

-- select ceiling with non pushdown func and explicit constant (result)
--Testcase 196:
SELECT ceiling(value3), pi(), 4.1 FROM numeric_tbl;

-- select ceiling with order by (EXPLAIN (COSTS OFF))
--Testcase 197:
EXPLAIN (COSTS OFF)
SELECT value1, ceiling(1-value3) FROM numeric_tbl ORDER BY ceiling(1-value3);

-- select ceiling with order by (result)
--Testcase 198:
SELECT value1, ceiling(1-value3) FROM numeric_tbl ORDER BY ceiling(1-value3);

-- select ceiling with order by index (result)
--Testcase 199:
SELECT value1, ceiling(1-value3) FROM numeric_tbl ORDER BY 2,1;

-- select ceiling with order by index (result)
--Testcase 200:
SELECT value1, ceiling(1-value3) FROM numeric_tbl ORDER BY 1,2;

-- select ceiling with group by (EXPLAIN (COSTS OFF))
--Testcase 201:
EXPLAIN (COSTS OFF)
SELECT count(value1), ceiling(1-value3) FROM numeric_tbl GROUP BY ceiling(1-value3);

-- select ceiling with group by (result)
--Testcase 202:
SELECT count(value1), ceiling(1-value3) FROM numeric_tbl GROUP BY ceiling(1-value3);

-- select ceiling with group by index (result)
--Testcase 203:
SELECT value1, ceiling(1-value3) FROM numeric_tbl GROUP BY 2,1;

-- select ceiling with group by index (result)
--Testcase 204:
SELECT value1, ceiling(1-value3) FROM numeric_tbl GROUP BY 1,2;

-- select ceiling with group by having (EXPLAIN (COSTS OFF))
--Testcase 205:
EXPLAIN (COSTS OFF)
SELECT count(value1), ceiling(1-value3) FROM numeric_tbl GROUP BY ceiling(1-value3) HAVING ceiling(avg(value1)) > 0;

-- select ceiling with group by having (result)
--Testcase 206:
SELECT count(value1), ceiling(1-value3) FROM numeric_tbl GROUP BY ceiling(1-value3) HAVING ceiling(avg(value1)) > 0;

-- select ceiling with group by index having (result)
--Testcase 207:
SELECT value1, ceiling(1-value3) FROM numeric_tbl GROUP BY 2,1 HAVING ceiling(1-value3) > 0;

-- select ceiling with group by index having (result)
--Testcase 208:
SELECT value1, ceiling(1-value3) FROM numeric_tbl GROUP BY 1,2 HAVING value1 > 1;

-- select ceiling and as
--Testcase 209:
SELECT ceiling(value3) as ceiling1 FROM numeric_tbl;

-- ===============================================================================
-- test char_length()
-- ===============================================================================
-- select char_length (stub function, EXPLAIN (COSTS OFF))
--Testcase 210:
EXPLAIN (COSTS OFF)
SELECT char_length(str4), char_length(str1), char_length(str2) FROM numeric_tbl;
-- select char_length (stub function, result)
--Testcase 211:
SELECT char_length(str4), char_length(str1), char_length(str2) FROM numeric_tbl;

-- select char_length (stub function, not pushdown constraints, EXPLAIN (COSTS OFF))
--Testcase 212:
EXPLAIN (COSTS OFF)
SELECT id, char_length(str4) FROM numeric_tbl WHERE to_hex(value2) = '64';
-- select char_length (stub function, not pushdown constraints, EXPLAIN (COSTS OFF))
--Testcase 213:
SELECT id, char_length(str4) FROM numeric_tbl WHERE to_hex(value2) = '64';

-- select char_length (stub function, char_length in constraints, EXPLAIN (COSTS OFF))
--Testcase 214:
EXPLAIN (COSTS OFF)
SELECT id, char_length(str4) FROM numeric_tbl WHERE char_length(str4) > 0;
-- select char_length (stub function, char_length in constraints, result)
--Testcase 215:
SELECT id, char_length(str4) FROM numeric_tbl WHERE char_length(str4) > 0;

-- select char_length with non pushdown func and explicit constant (EXPLAIN (COSTS OFF))
--Testcase 216:
EXPLAIN (COSTS OFF)
SELECT char_length(str4), pi(), 4.1 FROM numeric_tbl;
-- select char_length with non pushdown func and explicit constant (result)
--Testcase 217:
SELECT char_length(str4), pi(), 4.1 FROM numeric_tbl;

-- select char_length with order by (EXPLAIN (COSTS OFF))
--Testcase 218:
EXPLAIN (COSTS OFF)
SELECT value1, char_length(str4) FROM numeric_tbl ORDER BY char_length(str4), 1 DESC;
-- select char_length with order by (result)
--Testcase 219:
SELECT value1, char_length(str4) FROM numeric_tbl ORDER BY char_length(str4), 1 DESC;

-- select char_length with group by (EXPLAIN (COSTS OFF))
--Testcase 220:
EXPLAIN (COSTS OFF)
SELECT count(value1), char_length(str4) FROM numeric_tbl GROUP BY char_length(str4);
-- select char_length with group by (result)
--Testcase 221:
SELECT count(value1), char_length(str4) FROM numeric_tbl GROUP BY char_length(str4);

-- select char_length with group by index (result)
--Testcase 222:
SELECT value1, char_length(str4) FROM numeric_tbl GROUP BY 2,1;

-- select char_length with group by having (EXPLAIN (COSTS OFF))
--Testcase 223:
EXPLAIN (COSTS OFF)
SELECT count(value1), char_length(str4) FROM numeric_tbl GROUP BY char_length(str4), str1 HAVING char_length(str4) > 0;
-- select char_length with group by having (result)
--Testcase 224:
SELECT count(value1), char_length(str4) FROM numeric_tbl GROUP BY char_length(str4), str1 HAVING char_length(str4) > 0;

-- select char_length with group by index having (result)
--Testcase 225:
SELECT value1, char_length(str4) FROM numeric_tbl GROUP BY 1,2 HAVING value1 > 1;

-- ===============================================================================
-- test character_length()
-- ===============================================================================
-- select character_length (stub function, EXPLAIN (COSTS OFF))
--Testcase 226:
EXPLAIN (COSTS OFF)
SELECT character_length(tag1), character_length(str1), character_length(str2) FROM numeric_tbl;
-- select character_length (stub function, result)
--Testcase 227:
SELECT character_length(tag1), character_length(str1), character_length(str2) FROM numeric_tbl;

-- select character_length (stub function, not pushdown constraints, EXPLAIN (COSTS OFF))
--Testcase 228:
EXPLAIN (COSTS OFF)
SELECT id, character_length(str1) FROM numeric_tbl WHERE to_hex(value2) = '64';
-- select character_length (stub function, not pushdown constraints, EXPLAIN (COSTS OFF))
--Testcase 229:
SELECT id, character_length(str1) FROM numeric_tbl WHERE to_hex(value2) = '64';

-- select character_length (stub function, character_length in constraints, EXPLAIN (COSTS OFF))
--Testcase 230:
EXPLAIN (COSTS OFF)
SELECT id, character_length(str1) FROM numeric_tbl WHERE character_length(str1) > 0;
-- select character_length (stub function, character_length in constraints, result)
--Testcase 231:
SELECT id, character_length(str1) FROM numeric_tbl WHERE character_length(str1) > 0;

-- select character_length with non pushdown func and explicit constant (EXPLAIN (COSTS OFF))
--Testcase 232:
EXPLAIN (COSTS OFF)
SELECT character_length(str1), pi(), 4.1 FROM numeric_tbl;
-- select character_length with non pushdown func and explicit constant (result)
--Testcase 233:
SELECT character_length(str1), pi(), 4.1 FROM numeric_tbl;

-- select character_length with order by (EXPLAIN (COSTS OFF))
--Testcase 234:
EXPLAIN (COSTS OFF)
SELECT value1, character_length(str1) FROM numeric_tbl ORDER BY character_length(str1), 1 DESC;
-- select character_length with order by (result)
--Testcase 235:
SELECT value1, character_length(str1) FROM numeric_tbl ORDER BY character_length(str1), 1 DESC;

-- select character_length with group by (EXPLAIN (COSTS OFF))
--Testcase 236:
EXPLAIN (COSTS OFF)
SELECT count(value1), character_length(str1) FROM numeric_tbl GROUP BY character_length(str1);
-- select character_length with group by (result)
--Testcase 237:
SELECT count(value1), character_length(str1) FROM numeric_tbl GROUP BY character_length(str1);

-- select character_length with group by index (result)
--Testcase 238:
SELECT value1, character_length(str1) FROM numeric_tbl GROUP BY 2,1;

-- select character_length with group by having (EXPLAIN (COSTS OFF))
--Testcase 239:
EXPLAIN (COSTS OFF)
SELECT count(value1), character_length(str1) FROM numeric_tbl GROUP BY character_length(str1), str1 HAVING character_length(str1) > 0;
-- select character_length with group by having (result)
--Testcase 240:
SELECT count(value1), character_length(str1) FROM numeric_tbl GROUP BY character_length(str1), str1 HAVING character_length(str1) > 0;

-- select character_length with group by index having (result)
--Testcase 241:
SELECT value1, character_length(str1) FROM numeric_tbl GROUP BY 1,2 HAVING value1 > 1;

-- ===============================================================================
-- test concat()
-- ===============================================================================
-- select concat (stub function, EXPLAIN (COSTS OFF))
--Testcase 242:
EXPLAIN (COSTS OFF)
SELECT concat(id), concat(tag1), concat(value1), concat(value2), concat(str1) FROM numeric_tbl;
-- select concat (stub function, result)
--Testcase 243:
SELECT concat(id), concat(tag1), concat(value1), concat(value2), concat(str1) FROM numeric_tbl;

-- select concat (stub function, pushdown constraints, EXPLAIN (COSTS OFF))
--Testcase 244:
EXPLAIN (COSTS OFF)
SELECT id, concat(str1, str2) FROM numeric_tbl WHERE value2 != 100;
-- select concat (stub function, pushdown constraints, result)
--Testcase 245:
SELECT id, concat(str1, str2) FROM numeric_tbl WHERE value2 != 100;

-- select concat (stub function, concat in constraints, EXPLAIN (COSTS OFF))
--Testcase 246:
EXPLAIN (COSTS OFF)
SELECT id, concat(str1, str2) FROM numeric_tbl WHERE concat(str1, str2) != 'XYZ';
-- select concat (stub function, concat in constraints, EXPLAIN (COSTS OFF))
--Testcase 247:
SELECT id, concat(str1, str2) FROM numeric_tbl WHERE concat(str1, str2) != 'XYZ';

-- select concat as nest function with agg (pushdown, EXPLAIN (COSTS OFF))
--Testcase 248:
EXPLAIN (COSTS OFF)
SELECT id, concat(sum(value1), str1) FROM numeric_tbl GROUP BY id, str1;
-- select concat as nest function with agg (pushdown, result)
--Testcase 249:
SELECT id, concat(sum(value1), str1) FROM numeric_tbl GROUP BY id, str1;

-- select concat with non pushdown func and explicit constant (EXPLAIN (COSTS OFF))
--Testcase 250:
EXPLAIN (COSTS OFF)
SELECT concat(str1, str2), pi(), 4.1 FROM numeric_tbl;
-- select concat with non pushdown func and explicit constant (result)
--Testcase 251:
SELECT concat(str1, str2), pi(), 4.1 FROM numeric_tbl;

-- select concat with order by (EXPLAIN (COSTS OFF))
--Testcase 252:
EXPLAIN (COSTS OFF)
SELECT value1, concat(value2, str2) FROM numeric_tbl ORDER BY concat(value2, str2);
-- select concat with order by (result)
--Testcase 253:
SELECT value1, concat(value2, str2) FROM numeric_tbl ORDER BY concat(value2, str2);

-- select concat with order by index (result)
--Testcase 254:
SELECT value1, concat(value2, str2) FROM numeric_tbl ORDER BY 2,1;

-- select concat with group by (EXPLAIN (COSTS OFF))
--Testcase 255:
EXPLAIN (COSTS OFF)
SELECT count(value1), concat(str1, str2) FROM numeric_tbl GROUP BY concat(str1, str2);
-- select concat with group by (result)
--Testcase 256:
SELECT count(value1), concat(str1, str2) FROM numeric_tbl GROUP BY concat(str1, str2);

-- select concat with group by index (explain)
--Testcase 257:
EXPLAIN (COSTS OFF)
SELECT value1, concat(str1, str2) FROM numeric_tbl GROUP BY 2,1;
-- select concat with group by index (result)
--Testcase 258:
SELECT value1, concat(str1, str2) FROM numeric_tbl GROUP BY 2,1;

-- select concat with group by having (EXPLAIN (COSTS OFF))
--Testcase 259:
EXPLAIN (COSTS OFF)
SELECT count(value1), concat(str1, str2) FROM numeric_tbl GROUP BY concat(str1, str2) HAVING concat(str1, str2) IS NOT NULL;
-- select concat with group by having (EXPLAIN (COSTS OFF))
--Testcase 260:
SELECT count(value1), concat(str1, str2) FROM numeric_tbl GROUP BY concat(str1, str2) HAVING concat(str1, str2) IS NOT NULL;

-- select concat with group by index having (explain)
--Testcase 261:
EXPLAIN (COSTS OFF)
SELECT value1, concat(str1, str2, value1, value2) FROM numeric_tbl GROUP BY 1,2 HAVING value1 > 1;
-- select concat with group by index having (result)
--Testcase 262:
SELECT value1, concat(str1, str2, value1, value2) FROM numeric_tbl GROUP BY 1,2 HAVING value1 > 1;


-- ===============================================================================
-- test cos()
-- ===============================================================================
-- select cos (builtin function, EXPLAIN (COSTS OFF))
--Testcase 263:
EXPLAIN (COSTS OFF)
SELECT value1, cos(value2), cos(value3), cos(value4), cos(0.5) FROM numeric_tbl;

-- select cos (builtin function, result)
--Testcase 264:
SELECT value1, cos(value2), cos(value3), cos(value4), cos(0.5) FROM numeric_tbl;

-- select cos (builtin function, not pushdown constraints, EXPLAIN (COSTS OFF))
--Testcase 265:
EXPLAIN (COSTS OFF)
SELECT cos(value1), cos(value2), cos(value3), cos(value4), cos(0.5) FROM numeric_tbl WHERE to_hex(value2) = '64';

-- select cos (builtin function, not pushdown constraints, result)
--Testcase 266:
SELECT cos(value1), cos(value2), cos(value3), cos(value4), cos(0.5) FROM numeric_tbl WHERE to_hex(value2) = '64';

-- select cos (builtin function, pushdown constraints, EXPLAIN (COSTS OFF))
--Testcase 267:
EXPLAIN (COSTS OFF)
SELECT cos(value1), cos(value2), cos(value3), cos(value4), cos(0.5) FROM numeric_tbl WHERE value2 != 200;

-- select cos (builtin function, pushdown constraints, result)
--Testcase 268:
SELECT cos(value1), cos(value2), cos(value3), cos(value4), cos(0.5) FROM numeric_tbl WHERE value2 != 200;

-- select cos (builtin function, cos in constraints, EXPLAIN (COSTS OFF))
--Testcase 269:
EXPLAIN (COSTS OFF)
SELECT value1, cos(value2), cos(value3), cos(value4), cos(0.5) FROM numeric_tbl WHERE cos(value1) != 1;

-- select cos (builtin function, cos in constraints, result)
--Testcase 270:
SELECT value1, cos(value2), cos(value3), cos(value4), cos(0.5) FROM numeric_tbl WHERE cos(value1) != 1;

-- select cos (builtin function, cos in constraints, EXPLAIN (COSTS OFF))
--Testcase 271:
EXPLAIN (COSTS OFF)
SELECT cos(value1), cos(value2), cos(value3), cos(value4), cos(0.5) FROM numeric_tbl WHERE cos(0.5) > value1;

-- select cos (builtin function, cos in constraints, result)
--Testcase 272:
SELECT cos(value1), cos(value2), cos(value3), cos(value4), cos(0.5) FROM numeric_tbl WHERE cos(0.5) > value1;

-- select cos as nest function with agg (pushdown, EXPLAIN (COSTS OFF))
--Testcase 273:
EXPLAIN (COSTS OFF)
SELECT sum(value3),cos(sum(value3)) FROM numeric_tbl;

-- select cos as nest function with agg (pushdown, result)
--Testcase 274:
SELECT sum(value3),cos(sum(value3)) FROM numeric_tbl;

-- select cos as nest with log2 (pushdown, EXPLAIN (COSTS OFF))
--Testcase 275:
EXPLAIN (COSTS OFF)
SELECT value1, cos(log(2, value2)) FROM numeric_tbl;

-- select cos as nest with log2 (pushdown, result)
--Testcase 276:
SELECT value1, cos(log(2, value2)) FROM numeric_tbl;

-- select cos with non pushdown func and explicit constant (EXPLAIN (COSTS OFF))
--Testcase 277:
EXPLAIN (COSTS OFF)
SELECT cos(value3), pi(), 4.1 FROM numeric_tbl;

-- select cos with non pushdown func and explicit constant (result)
--Testcase 278:
SELECT cos(value3), pi(), 4.1 FROM numeric_tbl;

-- select cos with order by (EXPLAIN (COSTS OFF))
--Testcase 279:
EXPLAIN (COSTS OFF)
SELECT value1, cos(1-value3) FROM numeric_tbl ORDER BY cos(1-value3);

-- select cos with order by (result)
--Testcase 280:
SELECT value1, cos(1-value3) FROM numeric_tbl ORDER BY cos(1-value3);

-- select cos with order by index (result)
--Testcase 281:
SELECT value1, cos(1-value3) FROM numeric_tbl ORDER BY 2,1;

-- select cos with order by index (result)
--Testcase 282:
SELECT value1, cos(1-value3) FROM numeric_tbl ORDER BY 1,2;

-- select cos with group by (EXPLAIN (COSTS OFF))
--Testcase 283:
EXPLAIN (COSTS OFF)
SELECT value1, cos(1-value3) FROM numeric_tbl GROUP BY value1, cos(1-value3);

-- select cos with group by (result)
--Testcase 284:
SELECT value1, cos(1-value3) FROM numeric_tbl GROUP BY value1, cos(1-value3);

-- select cos with group by index (result)
--Testcase 285:
SELECT value1, cos(1-value3) FROM numeric_tbl GROUP BY 2,1;

-- select cos with group by index (result)
--Testcase 286:
SELECT value1, cos(1-value3) FROM numeric_tbl GROUP BY 1,2;

-- select cos with group by having (EXPLAIN (COSTS OFF))
--Testcase 287:
EXPLAIN (COSTS OFF)
SELECT value1, cos(1-value3) FROM numeric_tbl GROUP BY value1, cos(1-value3) HAVING cos(avg(value1)) > 0;

-- select cos with group by having (result)
--Testcase 288:
SELECT value1, cos(1-value3) FROM numeric_tbl GROUP BY value1, cos(1-value3) HAVING cos(avg(value1)) > 0;

-- select cos with group by index having (result)
--Testcase 289:
SELECT value1, cos(1-value3) FROM numeric_tbl GROUP BY 2,1 HAVING cos(1-value3) > 0;

-- select cos with group by index having (result)
--Testcase 290:
SELECT value1, cos(1-value3) FROM numeric_tbl GROUP BY 1,2 HAVING value1 > 1;

-- select cos and as
--Testcase 291:
SELECT cos(value3) as cos1 FROM numeric_tbl;

-- ===============================================================================
-- test exp()
-- ===============================================================================
-- select exp (builtin function, EXPLAIN (COSTS OFF))
--Testcase 292:
EXPLAIN (COSTS OFF)
SELECT exp(value1), exp(value2), exp(value3), exp(value4), exp(0.5) FROM numeric_tbl;

-- select exp (builtin function, result)
--Testcase 293:
SELECT exp(value1), exp(value2), exp(value3), exp(value4), exp(0.5) FROM numeric_tbl;

-- select exp (builtin function, not pushdown constraints, EXPLAIN (COSTS OFF))
--Testcase 294:
EXPLAIN (COSTS OFF)
SELECT exp(value1), exp(value2), exp(value3), exp(value4), exp(0.5) FROM numeric_tbl WHERE to_hex(value2) = '64';

-- select exp (builtin function, not pushdown constraints, result)
--Testcase 295:
SELECT exp(value1), exp(value2), exp(value3), exp(value4), exp(0.5) FROM numeric_tbl WHERE to_hex(value2) = '64';

-- select exp (builtin function, pushdown constraints, EXPLAIN (COSTS OFF))
--Testcase 296:
EXPLAIN (COSTS OFF)
SELECT exp(value1), exp(value2), exp(value3), exp(value4), exp(0.5) FROM numeric_tbl WHERE value2 != 200;

-- select exp (builtin function, pushdown constraints, result)
--Testcase 297:
SELECT exp(value1), exp(value2), exp(value3), exp(value4), exp(0.5) FROM numeric_tbl WHERE value2 != 200;

-- select exp (builtin function, exp in constraints, EXPLAIN (COSTS OFF))
--Testcase 298:
EXPLAIN (COSTS OFF)
SELECT exp(value1), exp(value2), exp(value3), exp(value4), exp(0.5) FROM numeric_tbl WHERE exp(value1) != 1;

-- select exp (builtin function, exp in constraints, result)
--Testcase 299:
SELECT exp(value1), exp(value2), exp(value3), exp(value4), exp(0.5) FROM numeric_tbl WHERE exp(value1) != 1;

-- select exp (builtin function, exp in constraints, EXPLAIN (COSTS OFF))
--Testcase 300:
EXPLAIN (COSTS OFF)
SELECT exp(value1), exp(value2), exp(value3), exp(value4), exp(0.5) FROM numeric_tbl WHERE exp(0.5) > value1;

-- select exp (builtin function, exp in constraints, result)
--Testcase 301:
SELECT exp(value1), exp(value2), exp(value3), exp(value4), exp(0.5) FROM numeric_tbl WHERE exp(0.5) > value1;

-- select exp as nest function with agg (pushdown, EXPLAIN (COSTS OFF))
--Testcase 302:
EXPLAIN (COSTS OFF)
SELECT sum(value3),exp(sum(value3)) FROM numeric_tbl;

-- select exp as nest function with agg (pushdown, result)
--Testcase 303:
SELECT sum(value3),exp(sum(value3)) FROM numeric_tbl;

-- select exp as nest with log2 (pushdown, EXPLAIN (COSTS OFF))
--Testcase 304:
EXPLAIN (COSTS OFF)
SELECT exp(log(2, value2)) FROM numeric_tbl;

-- select exp as nest with log2 (pushdown, result)
--Testcase 305:
SELECT exp(log(2, value2)) FROM numeric_tbl;

-- select exp with non pushdown func and explicit constant (EXPLAIN (COSTS OFF))
--Testcase 306:
EXPLAIN (COSTS OFF)
SELECT exp(value3), pi(), 4.1 FROM numeric_tbl;

-- select exp with non pushdown func and explicit constant (result)
--Testcase 307:
SELECT exp(value3), pi(), 4.1 FROM numeric_tbl;

-- select exp with order by (EXPLAIN (COSTS OFF))
--Testcase 308:
EXPLAIN (COSTS OFF)
SELECT value1, exp(1-value3) FROM numeric_tbl ORDER BY exp(1-value3);

-- select exp with order by (result)
--Testcase 309:
SELECT value1, exp(1-value3) FROM numeric_tbl ORDER BY exp(1-value3);

-- select exp with order by index (result)
--Testcase 310:
SELECT value1, exp(1-value3) FROM numeric_tbl ORDER BY 2,1;

-- select exp with order by index (result)
--Testcase 311:
SELECT value1, exp(1-value3) FROM numeric_tbl ORDER BY 1,2;

-- select exp with group by (EXPLAIN (COSTS OFF))
--Testcase 312:
EXPLAIN (COSTS OFF)
SELECT count(value1), exp(1-value3) FROM numeric_tbl GROUP BY exp(1-value3);

-- select exp with group by (result)
--Testcase 313:
SELECT count(value1), exp(1-value3) FROM numeric_tbl GROUP BY exp(1-value3);

-- select exp with group by index (result)
--Testcase 314:
SELECT value1, exp(1-value3) FROM numeric_tbl GROUP BY 2,1;

-- select exp with group by index (result)
--Testcase 315:
SELECT value1, exp(1-value3) FROM numeric_tbl GROUP BY 1,2;

-- select exp with group by having (EXPLAIN (COSTS OFF))
--Testcase 316:
EXPLAIN (COSTS OFF)
SELECT count(value1), exp(1-value3) FROM numeric_tbl GROUP BY exp(1-value3) HAVING exp(avg(value1)) > 0;

-- select exp with group by having (result)
--Testcase 317:
SELECT count(value1), exp(1-value3) FROM numeric_tbl GROUP BY exp(1-value3) HAVING exp(avg(value1)) > 0;

-- select exp with group by index having (result)
--Testcase 318:
SELECT value1, exp(1-value3) FROM numeric_tbl GROUP BY 2,1 HAVING exp(1-value3) > 0;

-- select exp with group by index having (result)
--Testcase 319:
SELECT value1, exp(1-value3) FROM numeric_tbl GROUP BY 1,2 HAVING value1 > 1;

-- select exp and as
--Testcase 320:
SELECT exp(value3) as exp1 FROM numeric_tbl;

-- ===============================================================================
-- test length()
-- ===============================================================================
-- select length (stub function, EXPLAIN (COSTS OFF))
--Testcase 321:
EXPLAIN (COSTS OFF)
SELECT length(str1), length(str2) FROM numeric_tbl;
-- select length (stub function, result)
--Testcase 322:
SELECT length(str1), length(str2) FROM numeric_tbl;

-- select length (stub function, not pushdown constraints, EXPLAIN (COSTS OFF))
--Testcase 323:
EXPLAIN (COSTS OFF)
SELECT value1, length(str1) FROM numeric_tbl WHERE to_hex(value2) = '64';
-- select length (stub function, not pushdown constraints, result)
--Testcase 324:
SELECT value1, length(str1) FROM numeric_tbl WHERE to_hex(value2) = '64';

-- select length (stub function, pushdown constraints, EXPLAIN (COSTS OFF))
--Testcase 325:
EXPLAIN (COSTS OFF)
SELECT value1, length(str1) FROM numeric_tbl WHERE value2 != 200;
-- select length (stub function, pushdown constraints, result)
--Testcase 326:
SELECT value1, length(str1) FROM numeric_tbl WHERE value2 != 200;

-- select length with non pushdown func and explicit constant (EXPLAIN (COSTS OFF))
--Testcase 327:
EXPLAIN (COSTS OFF)
SELECT length(str1), pi(), 4.1 FROM numeric_tbl;
-- select length with non pushdown func and explicit constant (result)
--Testcase 328:
SELECT length(str1), pi(), 4.1 FROM numeric_tbl;

-- select length with order by (EXPLAIN (COSTS OFF))
--Testcase 329:
EXPLAIN (COSTS OFF)
SELECT value1, length(str1) FROM numeric_tbl ORDER BY length(str1);
-- select length with order by (result)
--Testcase 330:
SELECT value1, length(str1) FROM numeric_tbl ORDER BY length(str1);

-- select length with order by index (result)
--Testcase 331:
SELECT value1, length(str1) FROM numeric_tbl ORDER BY 2,1;
-- select length with order by index (result)
--Testcase 332:
SELECT value1, length(str1) FROM numeric_tbl ORDER BY 1,2;

-- select length with group by (EXPLAIN (COSTS OFF))
--Testcase 333:
EXPLAIN (COSTS OFF)
SELECT count(value1), length(str1) FROM numeric_tbl GROUP BY length(str1);
-- select length with group by (result)
--Testcase 334:
SELECT count(value1), length(str1) FROM numeric_tbl GROUP BY length(str1);

-- select length with group by index (result)
--Testcase 335:
SELECT value1, length(str1) FROM numeric_tbl GROUP BY 2,1;

-- select length with group by having (EXPLAIN (COSTS OFF))
--Testcase 336:
EXPLAIN (COSTS OFF)
SELECT count(value1), length(str1) FROM numeric_tbl GROUP BY length(str1), str1 HAVING length(str1) IS NOT NULL;
-- select length with group by having (result)
--Testcase 337:
SELECT count(value1), length(str1) FROM numeric_tbl GROUP BY length(str1), str1 HAVING length(str1) IS NOT NULL;

-- select length with group by index having (result)
--Testcase 338:
SELECT value1, length(str1) FROM numeric_tbl GROUP BY 1,2 HAVING value1 > 1;

-- ===============================================================================
-- test lower()
-- ===============================================================================
-- select lower (stub function, EXPLAIN (COSTS OFF))
--Testcase 339:
EXPLAIN (COSTS OFF)
SELECT lower(str1), lower(str2) FROM numeric_tbl;
-- select lower (stub function, result)
--Testcase 340:
SELECT lower(str1), lower(str2) FROM numeric_tbl;

-- select lower (stub function, not pushdown constraints, EXPLAIN (COSTS OFF))
--Testcase 341:
EXPLAIN (COSTS OFF)
SELECT value1, lower(str1) FROM numeric_tbl WHERE to_hex(value2) = '64';
-- select lower (stub function, not pushdown constraints, result)
--Testcase 342:
SELECT value1, lower(str1) FROM numeric_tbl WHERE to_hex(value2) = '64';

-- select lower (stub function, pushdown constraints, EXPLAIN (COSTS OFF))
--Testcase 343:
EXPLAIN (COSTS OFF)
SELECT value1, lower(str1) FROM numeric_tbl WHERE value2 != 200;
-- select lower (stub function, pushdown constraints, result)
--Testcase 344:
SELECT value1, lower(str1) FROM numeric_tbl WHERE value2 != 200;

-- select lower with non pushdown func and explicit constant (EXPLAIN (COSTS OFF))
--Testcase 345:
EXPLAIN (COSTS OFF)
SELECT lower(str1), pi(), 4.1 FROM numeric_tbl;
-- select lower with non pushdown func and explicit constant (result)
--Testcase 346:
SELECT lower(str1), pi(), 4.1 FROM numeric_tbl;

-- select lower with order by (EXPLAIN (COSTS OFF))
--Testcase 347:
EXPLAIN (COSTS OFF)
SELECT value1, lower(str1) FROM numeric_tbl ORDER BY lower(str1);
-- select lower with order by (result)
--Testcase 348:
SELECT value1, lower(str1) FROM numeric_tbl ORDER BY lower(str1);

-- select lower with order by index (result)
--Testcase 349:
SELECT value1, lower(str1) FROM numeric_tbl ORDER BY 2,1;
-- select lower with order by index (result)
--Testcase 350:
SELECT value1, lower(str1) FROM numeric_tbl ORDER BY 1,2;

-- select lower with group by (EXPLAIN (COSTS OFF))
--Testcase 351:
EXPLAIN (COSTS OFF)
SELECT count(value1), lower(str1) FROM numeric_tbl GROUP BY lower(str1);
-- select lower with group by (result)
--Testcase 352:
SELECT count(value1), lower(str1) FROM numeric_tbl GROUP BY lower(str1);

-- select lower with group by index (result)
--Testcase 353:
SELECT value1, lower(str1) FROM numeric_tbl GROUP BY 2,1;

-- select lower with group by having (EXPLAIN (COSTS OFF))
--Testcase 354:
EXPLAIN (COSTS OFF)
SELECT count(value1), lower(str1) FROM numeric_tbl GROUP BY lower(str1), str1 HAVING lower(str1) IS NOT NULL;
-- select lower with group by having (result)
--Testcase 355:
SELECT count(value1), lower(str1) FROM numeric_tbl GROUP BY lower(str1), str1 HAVING lower(str1) IS NOT NULL;

-- select lower with group by index having (result)
--Testcase 356:
SELECT value1, lower(str1) FROM numeric_tbl GROUP BY 1,2 HAVING value1 > 1;

-- ===============================================================================
-- test lpad()
-- ===============================================================================
-- select lpad (stub function, EXPLAIN (COSTS OFF))
--Testcase 357:
EXPLAIN (COSTS OFF)
SELECT lpad(str1, 4, 'ABCD'), lpad(str2, 4, 'ABCD') FROM numeric_tbl;
-- select lpad (stub function, result)
--Testcase 358:
SELECT lpad(str1, 4, 'ABCD'), lpad(str2, 4, 'ABCD') FROM numeric_tbl;

-- select lpad (stub function, not pushdown constraints, EXPLAIN (COSTS OFF))
--Testcase 359:
EXPLAIN (COSTS OFF)
SELECT value1, lpad(str1, 4, 'ABCD') FROM numeric_tbl WHERE to_hex(value2) = '64';
-- select lpad (stub function, not pushdown constraints, result)
--Testcase 360:
SELECT value1, lpad(str1, 4, 'ABCD') FROM numeric_tbl WHERE to_hex(value2) = '64';

-- select lpad (stub function, pushdown constraints, EXPLAIN (COSTS OFF))
--Testcase 361:
EXPLAIN (COSTS OFF)
SELECT value1, lpad(str1, 4, 'ABCD') FROM numeric_tbl WHERE value2 != 200;
-- select lpad (stub function, pushdown constraints, result)
--Testcase 362:
SELECT value1, lpad(str1, 4, 'ABCD') FROM numeric_tbl WHERE value2 != 200;

-- select lpad with non pushdown func and explicit constant (EXPLAIN (COSTS OFF))
--Testcase 363:
EXPLAIN (COSTS OFF)
SELECT lpad(str1, 4, 'ABCD'), pi(), 4.1 FROM numeric_tbl;
-- select lpad with non pushdown func and explicit constant (result)
--Testcase 364:
SELECT lpad(str1, 4, 'ABCD'), pi(), 4.1 FROM numeric_tbl;

-- select lpad with order by (EXPLAIN (COSTS OFF))
--Testcase 365:
EXPLAIN (COSTS OFF)
SELECT value1, lpad(str1, 4, 'ABCD') FROM numeric_tbl ORDER BY lpad(str1, 4, 'ABCD');
-- select lpad with order by (result)
--Testcase 366:
SELECT value1, lpad(str1, 4, 'ABCD') FROM numeric_tbl ORDER BY lpad(str1, 4, 'ABCD');

-- select lpad with order by index (result)
--Testcase 367:
SELECT value1, lpad(str1, 4, 'ABCD') FROM numeric_tbl ORDER BY 2,1;
-- select lpad with order by index (result)
--Testcase 368:
SELECT value1, lpad(str1, 4, 'ABCD') FROM numeric_tbl ORDER BY 1,2;

-- select lpad with group by (EXPLAIN (COSTS OFF))
--Testcase 369:
EXPLAIN (COSTS OFF)
SELECT count(value1), lpad(str1, 4, 'ABCD') FROM numeric_tbl GROUP BY lpad(str1, 4, 'ABCD');
-- select lpad with group by (result)
--Testcase 370:
SELECT count(value1), lpad(str1, 4, 'ABCD') FROM numeric_tbl GROUP BY lpad(str1, 4, 'ABCD');

-- select lpad with group by index (result)
--Testcase 371:
SELECT value1, lpad(str1, 4, 'ABCD') FROM numeric_tbl GROUP BY 2,1;

-- select lpad with group by having (EXPLAIN (COSTS OFF))
--Testcase 372:
EXPLAIN (COSTS OFF)
SELECT count(value1), lpad(str1, 4, 'ABCD') FROM numeric_tbl GROUP BY lpad(str1, 4, 'ABCD'), str1 HAVING lpad(str1, 4, 'ABCD') IS NOT NULL;
-- select lpad with group by having (result)
--Testcase 373:
SELECT count(value1), lpad(str1, 4, 'ABCD') FROM numeric_tbl GROUP BY lpad(str1, 4, 'ABCD'), str1 HAVING lpad(str1, 4, 'ABCD') IS NOT NULL;

-- select lpad with group by index having (result)
--Testcase 374:
SELECT value1, lpad(str1, 4, 'ABCD') FROM numeric_tbl GROUP BY 1,2 HAVING value1 > 1;

-- ===============================================================================
-- test ltrim()
-- ===============================================================================
-- select ltrim (stub function, EXPLAIN (COSTS OFF))
--Testcase 375:
EXPLAIN (COSTS OFF)
SELECT ltrim(str1), ltrim(str2, ' ') FROM numeric_tbl;
-- select ltrim (stub function, result)
--Testcase 376:
SELECT ltrim(str1), ltrim(str2, ' ') FROM numeric_tbl;

-- select ltrim (stub function, not pushdown constraints, EXPLAIN (COSTS OFF))
--Testcase 377:
EXPLAIN (COSTS OFF)
SELECT value1, ltrim(str1, '-') FROM numeric_tbl WHERE to_hex(value2) = '64';
-- select ltrim (stub function, not pushdown constraints, result)
--Testcase 378:
SELECT value1, ltrim(str1, '-') FROM numeric_tbl WHERE to_hex(value2) = '64';

-- select ltrim (stub function, pushdown constraints, EXPLAIN (COSTS OFF))
--Testcase 379:
EXPLAIN (COSTS OFF)
SELECT value1, ltrim(str1, '-') FROM numeric_tbl WHERE value2 != 200;
-- select ltrim (stub function, pushdown constraints, result)
--Testcase 380:
SELECT value1, ltrim(str1, '-') FROM numeric_tbl WHERE value2 != 200;

-- select ltrim with non pushdown func and explicit constant (EXPLAIN (COSTS OFF))
--Testcase 381:
EXPLAIN (COSTS OFF)
SELECT ltrim(str1, '-'), pi(), 4.1 FROM numeric_tbl;
-- select ltrim with non pushdown func and explicit constant (result)
--Testcase 382:
SELECT ltrim(str1, '-'), pi(), 4.1 FROM numeric_tbl;

-- select ltrim with order by (EXPLAIN (COSTS OFF))
--Testcase 383:
EXPLAIN (COSTS OFF)
SELECT value1, ltrim(str1, '-') FROM numeric_tbl ORDER BY ltrim(str1, '-');
-- select ltrim with order by (result)
--Testcase 384:
SELECT value1, ltrim(str1, '-') FROM numeric_tbl ORDER BY ltrim(str1, '-');

-- select ltrim with order by index (result)
--Testcase 385:
SELECT value1, ltrim(str1, '-') FROM numeric_tbl ORDER BY 2,1;
-- select ltrim with order by index (result)
--Testcase 386:
SELECT value1, ltrim(str1, '-') FROM numeric_tbl ORDER BY 1,2;

-- select ltrim with group by (EXPLAIN (COSTS OFF))
--Testcase 387:
EXPLAIN (COSTS OFF)
SELECT count(value1), ltrim(str1, '-') FROM numeric_tbl GROUP BY ltrim(str1, '-');
-- select ltrim with group by (result)
--Testcase 388:
SELECT count(value1), ltrim(str1, '-') FROM numeric_tbl GROUP BY ltrim(str1, '-');

-- select ltrim with group by index (result)
--Testcase 389:
SELECT value1, ltrim(str1, '-') FROM numeric_tbl GROUP BY 2,1;

-- select ltrim with group by having (EXPLAIN (COSTS OFF))
--Testcase 390:
EXPLAIN (COSTS OFF)
SELECT count(value1), ltrim(str1, '-') FROM numeric_tbl GROUP BY ltrim(str1, '-'), str2 HAVING ltrim(str1, '-') IS NOT NULL;
-- select ltrim with group by having (result)
--Testcase 391:
SELECT count(value1), ltrim(str1, '-') FROM numeric_tbl GROUP BY ltrim(str1, '-'), str2 HAVING ltrim(str1, '-') IS NOT NULL;

-- select ltrim with group by index having (result)
--Testcase 392:
SELECT value1, ltrim(str2) FROM numeric_tbl GROUP BY 1,2 HAVING value1 > 1;

-- ===============================================================================
-- test mod()
-- ===============================================================================
-- select mod (builtin function, EXPLAIN (COSTS OFF))
--Testcase 393:
EXPLAIN (COSTS OFF)
SELECT value1, mod(value1::numeric, 2), mod(value2::numeric, 2), mod(value3::numeric, 2), mod(value4::numeric, 2), mod(5, 2) FROM numeric_tbl;

-- select mod (builtin function, result)
--Testcase 394:
SELECT value1, mod(value1::numeric, 2), mod(value2::numeric, 2), mod(value3::numeric, 2), mod(value4::numeric, 2), mod(5, 2) FROM numeric_tbl;

-- select mod (builtin function, not pushdown constraints, EXPLAIN (COSTS OFF))
--Testcase 395:
EXPLAIN (COSTS OFF)
SELECT mod(value1::numeric, 2), mod(value2::numeric, 2), mod(value3::numeric, 2), mod(value4::numeric, 2), mod(5, 2) FROM numeric_tbl WHERE to_hex(value2) = '64';

-- select mod (builtin function, not pushdown constraints, result)
--Testcase 396:
SELECT mod(value1::numeric, 2), mod(value2::numeric, 2), mod(value3::numeric, 2), mod(value4::numeric, 2), mod(5, 2) FROM numeric_tbl WHERE to_hex(value2) = '64';

-- select mod (builtin function, pushdown constraints, EXPLAIN (COSTS OFF))
--Testcase 397:
EXPLAIN (COSTS OFF)
SELECT mod(value1::numeric, 2), mod(value2::numeric, 2), mod(value3::numeric, 2), mod(value4::numeric, 2), mod(5, 2) FROM numeric_tbl WHERE value2 != 200;

-- select mod (builtin function, pushdown constraints, result)
--Testcase 398:
SELECT mod(value1::numeric, 2), mod(value2::numeric, 2), mod(value3::numeric, 2), mod(value4::numeric, 2), mod(5, 2) FROM numeric_tbl WHERE value2 != 200;

-- select mod (builtin function, mod in constraints, EXPLAIN (COSTS OFF))
--Testcase 399:
EXPLAIN (COSTS OFF)
SELECT value1, mod(value1::numeric, 2), mod(value2::numeric, 2), mod(value3::numeric, 2), mod(value4::numeric, 2), mod(5, 2) FROM numeric_tbl WHERE mod(value1::numeric, 2) != 1;

-- select mod (builtin function, mod in constraints, result)
--Testcase 400:
SELECT value1, mod(value1::numeric, 2), mod(value2::numeric, 2), mod(value3::numeric, 2), mod(value4::numeric, 2), mod(5, 2) FROM numeric_tbl WHERE mod(value1::numeric, 2) != 1;

-- select mod (builtin function, mod in constraints, EXPLAIN (COSTS OFF))
--Testcase 401:
EXPLAIN (COSTS OFF)
SELECT mod(value1::numeric, 2), mod(value2::numeric, 2), mod(value3::numeric, 2), mod(value4::numeric, 2), mod(5, 2) FROM numeric_tbl WHERE mod(5, 2) > value1;

-- select mod (builtin function, mod in constraints, result)
--Testcase 402:
SELECT mod(value1::numeric, 2), mod(value2::numeric, 2), mod(value3::numeric, 2), mod(value4::numeric, 2), mod(5, 2) FROM numeric_tbl WHERE mod(5, 2) > value1;

-- select mod as nest function with agg (pushdown, EXPLAIN (COSTS OFF))
--Testcase 403:
EXPLAIN (COSTS OFF)
SELECT sum(value3),mod(sum(value3)::numeric, 2) FROM numeric_tbl;

-- select mod as nest function with agg (pushdown, result)
--Testcase 404:
SELECT sum(value3),mod(sum(value3)::numeric, 2) FROM numeric_tbl;

-- select mod as nest with log2 (pushdown, EXPLAIN (COSTS OFF))
--Testcase 405:
EXPLAIN (COSTS OFF)
SELECT value1, mod(log(2, value2)::numeric, 2) FROM numeric_tbl;

-- select mod as nest with log2 (pushdown, result)
--Testcase 406:
SELECT value1, mod(log(2, value2)::numeric, 2) FROM numeric_tbl;

-- select mod with non pushdown func and explicit constant (EXPLAIN (COSTS OFF))
--Testcase 407:
EXPLAIN (COSTS OFF)
SELECT value1, mod(value3::numeric, 2), pi(), 4.1 FROM numeric_tbl;

-- select mod with non pushdown func and explicit constant (result)
--Testcase 408:
SELECT value1, mod(value3::numeric, 2), pi(), 4.1 FROM numeric_tbl;

-- select mod with order by (EXPLAIN (COSTS OFF))
--Testcase 409:
EXPLAIN (COSTS OFF)
SELECT value1, mod((1-value3)::numeric, 2) FROM numeric_tbl ORDER BY mod((1-value3)::numeric, 2);

-- select mod with order by (result)
--Testcase 410:
SELECT value1, mod((1-value3)::numeric, 2) FROM numeric_tbl ORDER BY mod((1-value3)::numeric, 2);

-- select mod with order by index (result)
--Testcase 411:
SELECT value1, mod((1-value3)::numeric, 2) FROM numeric_tbl ORDER BY 2,1;

-- select mod with order by index (result)
--Testcase 412:
SELECT value1, mod((1-value3)::numeric, 2) FROM numeric_tbl ORDER BY 1,2;

-- select mod with group by (EXPLAIN (COSTS OFF))
--Testcase 413:
EXPLAIN (COSTS OFF)
SELECT value1, mod((1-value3)::numeric, 2) FROM numeric_tbl GROUP BY value1, mod((1-value3)::numeric, 2);

-- select mod with group by (result)
--Testcase 414:
SELECT value1, mod((1-value3)::numeric, 2) FROM numeric_tbl GROUP BY value1, mod((1-value3)::numeric, 2);

-- select mod with group by index (result)
--Testcase 415:
SELECT value1, mod((1-value3)::numeric, 2) FROM numeric_tbl GROUP BY 2,1;

-- select mod with group by index (result)
--Testcase 416:
SELECT value1, mod((1-value3)::numeric, 2) FROM numeric_tbl GROUP BY 1,2;

-- select mod with group by having (EXPLAIN (COSTS OFF))
--Testcase 417:
EXPLAIN (COSTS OFF)
SELECT value1, mod((1-value3)::numeric, 2) FROM numeric_tbl GROUP BY value1, mod((1-value3)::numeric, 2) HAVING avg(value1) > 0;

-- select mod with group by having (result)
--Testcase 418:
SELECT value1, mod((1-value3)::numeric, 2) FROM numeric_tbl GROUP BY value1, mod((1-value3)::numeric, 2) HAVING avg(value1) > 0;

-- select mod with group by index having (result)
--Testcase 419:
SELECT value1, mod((1-value3)::numeric, 2) FROM numeric_tbl GROUP BY 1,2 HAVING value1 > 1;

-- select mod and as
--Testcase 420:
SELECT value1, mod(value3::numeric, 2) as mod1 FROM numeric_tbl;

-- ===============================================================================
-- test octet_length()
-- ===============================================================================
-- select octet_length (stub function, EXPLAIN (COSTS OFF))
--Testcase 421:
EXPLAIN (COSTS OFF)
SELECT octet_length(str1), octet_length(str2) FROM numeric_tbl;
-- select octet_length (stub function, result)
--Testcase 422:
SELECT octet_length(str1), octet_length(str2) FROM numeric_tbl;

-- select octet_length (stub function, not pushdown constraints, EXPLAIN (COSTS OFF))
--Testcase 423:
EXPLAIN (COSTS OFF)
SELECT value1, octet_length(str1) FROM numeric_tbl WHERE to_hex(value2) = '64';
-- select octet_length (stub function, not pushdown constraints, result)
--Testcase 424:
SELECT value1, octet_length(str1) FROM numeric_tbl WHERE to_hex(value2) = '64';

-- select octet_length (stub function, pushdown constraints, EXPLAIN (COSTS OFF))
--Testcase 425:
EXPLAIN (COSTS OFF)
SELECT value1, octet_length(str1) FROM numeric_tbl WHERE value2 != 200;
-- select octet_length (stub function, pushdown constraints, result)
--Testcase 426:
SELECT value1, octet_length(str1) FROM numeric_tbl WHERE value2 != 200;

-- select octet_length with non pushdown func and explicit constant (EXPLAIN (COSTS OFF))
--Testcase 427:
EXPLAIN (COSTS OFF)
SELECT octet_length(str1), pi(), 4.1 FROM numeric_tbl;
-- select octet_length with non pushdown func and explicit constant (result)
--Testcase 428:
SELECT octet_length(str1), pi(), 4.1 FROM numeric_tbl;

-- select octet_length with order by (EXPLAIN (COSTS OFF))
--Testcase 429:
EXPLAIN (COSTS OFF)
SELECT value1, octet_length(str1) FROM numeric_tbl ORDER BY octet_length(str1);
-- select octet_length with order by (result)
--Testcase 430:
SELECT value1, octet_length(str1) FROM numeric_tbl ORDER BY octet_length(str1);

-- select octet_length with order by index (result)
--Testcase 431:
SELECT value1, octet_length(str1) FROM numeric_tbl ORDER BY 2,1;
-- select octet_length with order by index (result)
--Testcase 432:
SELECT value1, octet_length(str1) FROM numeric_tbl ORDER BY 1,2;

-- select octet_length with group by (EXPLAIN (COSTS OFF))
--Testcase 433:
EXPLAIN (COSTS OFF)
SELECT count(value1), octet_length(str1) FROM numeric_tbl GROUP BY octet_length(str1);
-- select octet_length with group by (result)
--Testcase 434:
SELECT count(value1), octet_length(str1) FROM numeric_tbl GROUP BY octet_length(str1);

-- select octet_length with group by index (result)
--Testcase 435:
SELECT value1, octet_length(str1) FROM numeric_tbl GROUP BY 2,1;

-- select octet_length with group by having (EXPLAIN (COSTS OFF))
--Testcase 436:
EXPLAIN (COSTS OFF)
SELECT count(value1), octet_length(str1) FROM numeric_tbl GROUP BY octet_length(str1), str1 HAVING octet_length(str1) IS NOT NULL;
-- select octet_length with group by having (result)
--Testcase 437:
SELECT count(value1), octet_length(str1) FROM numeric_tbl GROUP BY octet_length(str1), str1 HAVING octet_length(str1) IS NOT NULL;

-- select octet_length with group by index having (result)
--Testcase 438:
SELECT value1, octet_length(str1) FROM numeric_tbl GROUP BY 1,2 HAVING value1 > 1;

-- ===============================================================================
-- test position()
-- ===============================================================================
-- select position (stub function, EXPLAIN (COSTS OFF))
--Testcase 439:
EXPLAIN (COSTS OFF)
SELECT position('XYZ' IN str1), position('XYZ' IN str2) FROM numeric_tbl;
-- select position (stub function, result)
--Testcase 440:
SELECT position('XYZ' IN str1), position('XYZ' IN str2) FROM numeric_tbl;

-- select position (stub function, not pushdown constraints, EXPLAIN (COSTS OFF))
--Testcase 441:
EXPLAIN (COSTS OFF)
SELECT value1, position('XYZ' IN str1) FROM numeric_tbl WHERE to_hex(value2) = '64';
-- select position (stub function, not pushdown constraints, result)
--Testcase 442:
SELECT value1, position('XYZ' IN str1) FROM numeric_tbl WHERE to_hex(value2) = '64';

-- select position (stub function, pushdown constraints, EXPLAIN (COSTS OFF))
--Testcase 443:
EXPLAIN (COSTS OFF)
SELECT value1, position('XYZ' IN str1) FROM numeric_tbl WHERE value2 != 200;
-- select position (stub function, pushdown constraints, result)
--Testcase 444:
SELECT value1, position('XYZ' IN str1) FROM numeric_tbl WHERE value2 != 200;

-- select position with non pushdown func and explicit constant (EXPLAIN (COSTS OFF))
--Testcase 445:
EXPLAIN (COSTS OFF)
SELECT position('XYZ' IN str1), pi(), 4.1 FROM numeric_tbl;
-- select position with non pushdown func and explicit constant (result)
--Testcase 446:
SELECT position('XYZ' IN str1), pi(), 4.1 FROM numeric_tbl;

-- select position with order by (EXPLAIN (COSTS OFF))
--Testcase 447:
EXPLAIN (COSTS OFF)
SELECT value1, position('XYZ' IN str1) FROM numeric_tbl ORDER BY position('XYZ' IN str1);
-- select position with order by (result)
--Testcase 448:
SELECT value1, position('XYZ' IN str1) FROM numeric_tbl ORDER BY position('XYZ' IN str1);

-- select position with order by index (result)
--Testcase 449:
SELECT value1, position('XYZ' IN str1) FROM numeric_tbl ORDER BY 2,1;
-- select position with order by index (result)
--Testcase 450:
SELECT value1, position('XYZ' IN str1) FROM numeric_tbl ORDER BY 1,2;

-- select position with group by (EXPLAIN (COSTS OFF))
--Testcase 451:
EXPLAIN (COSTS OFF)
SELECT count(value1), position('XYZ' IN str1) FROM numeric_tbl GROUP BY position('XYZ' IN str1);
-- select position with group by (result)
--Testcase 452:
SELECT count(value1), position('XYZ' IN str1) FROM numeric_tbl GROUP BY position('XYZ' IN str1);

-- select position with group by index (result)
--Testcase 453:
SELECT value1, position('XYZ' IN str1) FROM numeric_tbl GROUP BY 2,1;

-- select position with group by having (EXPLAIN (COSTS OFF))
--Testcase 454:
EXPLAIN (COSTS OFF)
SELECT count(value1), position('XYZ' IN str1) FROM numeric_tbl GROUP BY position('XYZ' IN str1), str1 HAVING position('XYZ' IN str1) IS NOT NULL;
-- select position with group by having (result)
--Testcase 455:
SELECT count(value1), position('XYZ' IN str1) FROM numeric_tbl GROUP BY position('XYZ' IN str1), str1 HAVING position('XYZ' IN str1) IS NOT NULL;

-- select position with group by index having (result)
--Testcase 456:
SELECT value1, position('XYZ' IN str1) FROM numeric_tbl GROUP BY 1,2 HAVING value1 > 1;

-- ===============================================================================
-- test pow()
-- ===============================================================================
-- select pow (builtin function, EXPLAIN (COSTS OFF))
--Testcase 457:
EXPLAIN (COSTS OFF)
SELECT pow(value1, 2), pow(value2, 2), pow(value3, 2), pow(value4, 2) FROM numeric_tbl;

-- select pow (builtin function, result)
--Testcase 458:
SELECT pow(value1, 2), pow(value2, 2), pow(value3, 2), pow(value4, 2) FROM numeric_tbl;

-- select pow (builtin function, not pushdown constraints, EXPLAIN (COSTS OFF))
--Testcase 459:
EXPLAIN (COSTS OFF)
SELECT pow(value1, 2), pow(value2, 2), pow(value3, 2), pow(value4, 2) FROM numeric_tbl WHERE to_hex(value2) != '64';

-- select pow (builtin function, not pushdown constraints, result)
--Testcase 460:
SELECT pow(value1, 2), pow(value2, 2), pow(value3, 2), pow(value4, 2) FROM numeric_tbl WHERE to_hex(value2) != '64';

-- select pow (builtin function, pushdown constraints, EXPLAIN (COSTS OFF))
--Testcase 461:
EXPLAIN (COSTS OFF)
SELECT pow(value1, 2), pow(value2, 2), pow(value3, 2), pow(value4, 2) FROM numeric_tbl WHERE value2 != 200;

-- select pow (builtin function, pushdown constraints, result)
--Testcase 462:
SELECT pow(value1, 2), pow(value2, 2), pow(value3, 2), pow(value4, 2) FROM numeric_tbl WHERE value2 != 200;

-- select pow as nest function with agg (pushdown, EXPLAIN (COSTS OFF))
--Testcase 463:
EXPLAIN (COSTS OFF)
SELECT sum(value3),pow(sum(value3), 2) FROM numeric_tbl;

-- select pow as nest function with agg (pushdown, result)
--Testcase 464:
SELECT sum(value3),pow(sum(value3), 2) FROM numeric_tbl;

-- select pow as nest with log2 (pushdown, EXPLAIN (COSTS OFF))
--Testcase 465:
EXPLAIN (COSTS OFF)
SELECT value1, pow(log(2, value2), 2) FROM numeric_tbl;

-- select pow as nest with log2 (pushdown, result)
--Testcase 466:
SELECT value1, pow(log(2, value2), 2) FROM numeric_tbl;

-- select pow with non pushdown func and explicit constant (EXPLAIN (COSTS OFF))
--Testcase 467:
EXPLAIN (COSTS OFF)
SELECT pow(value3, 2), pi(), 4.1 FROM numeric_tbl;

-- select pow with non pushdown func and explicit constant (result)
--Testcase 468:
SELECT pow(value3, 2), pi(), 4.1 FROM numeric_tbl;

-- select pow with order by (EXPLAIN (COSTS OFF))
--Testcase 469:
EXPLAIN (COSTS OFF)
SELECT value3, pow(1-value3, 2) FROM numeric_tbl ORDER BY pow(1-value3, 2);

-- select pow with order by (result)
--Testcase 470:
SELECT value3, pow(1-value3, 2) FROM numeric_tbl ORDER BY pow(1-value3, 2);

-- select pow with order by index (result)
--Testcase 471:
SELECT value3, pow(1-value3, 2) FROM numeric_tbl ORDER BY 2,1;

-- select pow with order by index (result)
--Testcase 472:
SELECT value3, pow(1-value3, 2) FROM numeric_tbl ORDER BY 1,2;

-- select pow and as
--Testcase 473:
SELECT pow(value3, 2) as pow1 FROM numeric_tbl;

-- ===============================================================================
-- test power()
-- ===============================================================================
-- select power (builtin function, EXPLAIN (COSTS OFF))
--Testcase 474:
EXPLAIN (COSTS OFF)
SELECT power(value1, 2), power(value2, 2), power(value3, 2), power(value4, 2), power(5, 2) FROM numeric_tbl;

-- select power (builtin function, result)
--Testcase 475:
SELECT power(value1, 2), power(value2, 2), power(value3, 2), power(value4, 2), power(5, 2) FROM numeric_tbl;

-- select power (builtin function, not pushdown constraints, EXPLAIN (COSTS OFF))
--Testcase 476:
EXPLAIN (COSTS OFF)
SELECT power(value1, 2), power(value2, 2), power(value3, 2), power(value4, 2), power(5, 2) FROM numeric_tbl WHERE to_hex(value2) = '64';

-- select power (builtin function, not pushdown constraints, result)
--Testcase 477:
SELECT power(value1, 2), power(value2, 2), power(value3, 2), power(value4, 2), power(5, 2) FROM numeric_tbl WHERE to_hex(value2) = '64';

-- select power (builtin function, pushdown constraints, EXPLAIN (COSTS OFF))
--Testcase 478:
EXPLAIN (COSTS OFF)
SELECT power(value1, 2), power(value2, 2), power(value3, 2), power(value4, 2), power(5, 2) FROM numeric_tbl WHERE value2 != 200;

-- select power (builtin function, pushdown constraints, result)
--Testcase 479:
SELECT power(value1, 2), power(value2, 2), power(value3, 2), power(value4, 2), power(5, 2) FROM numeric_tbl WHERE value2 != 200;

-- select power (builtin function, power in constraints, EXPLAIN (COSTS OFF))
--Testcase 480:
EXPLAIN (COSTS OFF)
SELECT power(value1, 2), power(value2, 2), power(value3, 2), power(value4, 2), power(5, 2) FROM numeric_tbl WHERE power(value1, 2) != 1;

-- select power (builtin function, power in constraints, result)
--Testcase 481:
SELECT power(value1, 2), power(value2, 2), power(value3, 2), power(value4, 2), power(5, 2) FROM numeric_tbl WHERE power(value1, 2) != 1;

-- select power (builtin function, power in constraints, EXPLAIN (COSTS OFF))
--Testcase 482:
EXPLAIN (COSTS OFF)
SELECT power(value1, 2), power(value2, 2), power(value3, 2), power(value4, 2), power(5, 2) FROM numeric_tbl WHERE power(5, 2) > value1;

-- select power (builtin function, power in constraints, result)
--Testcase 483:
SELECT power(value1, 2), power(value2, 2), power(value3, 2), power(value4, 2), power(5, 2) FROM numeric_tbl WHERE power(5, 2) > value1;

-- select power as nest function with agg (pushdown, EXPLAIN (COSTS OFF))
--Testcase 484:
EXPLAIN (COSTS OFF)
SELECT sum(value3),power(sum(value3), 2) FROM numeric_tbl;

-- select power as nest function with agg (pushdown, result)
--Testcase 485:
SELECT sum(value3),power(sum(value3), 2) FROM numeric_tbl;

-- select power as nest with log2 (pushdown, EXPLAIN (COSTS OFF))
--Testcase 486:
EXPLAIN (COSTS OFF)
SELECT value1, power(log(2, value2), 2) FROM numeric_tbl;

-- select power as nest with log2 (pushdown, result)
--Testcase 487:
SELECT value1, power(log(2, value2), 2) FROM numeric_tbl;

-- select power with non pushdown func and explicit constant (EXPLAIN (COSTS OFF))
--Testcase 488:
EXPLAIN (COSTS OFF)
SELECT power(value3, 2), pi(), 4.1 FROM numeric_tbl;

-- select power with non pushdown func and explicit constant (result)
--Testcase 489:
SELECT power(value3, 2), pi(), 4.1 FROM numeric_tbl;

-- select power with order by (EXPLAIN (COSTS OFF))
--Testcase 490:
EXPLAIN (COSTS OFF)
SELECT value1, power(1-value3, 2) FROM numeric_tbl ORDER BY power(1-value3, 2);

-- select power with order by (result)
--Testcase 491:
SELECT value1, power(1-value3, 2) FROM numeric_tbl ORDER BY power(1-value3, 2);

-- select power with order by index (result)
--Testcase 492:
SELECT value1, power(1-value3, 2) FROM numeric_tbl ORDER BY 2,1;

-- select power with order by index (result)
--Testcase 493:
SELECT value1, power(1-value3, 2) FROM numeric_tbl ORDER BY 1,2;

-- select power with group by (EXPLAIN (COSTS OFF))
--Testcase 494:
EXPLAIN (COSTS OFF)
SELECT count(value1), power(1-value3, 2) FROM numeric_tbl GROUP BY power(1-value3, 2);

-- select power with group by (result)
--Testcase 495:
SELECT count(value1), power(1-value3, 2) FROM numeric_tbl GROUP BY power(1-value3, 2);

-- select power with group by index (result)
--Testcase 496:
SELECT value1, power(1-value3, 2) FROM numeric_tbl GROUP BY 2,1;

-- select power with group by index (result)
--Testcase 497:
SELECT value1, power(1-value3, 2) FROM numeric_tbl GROUP BY 1,2;

-- select power with group by having (EXPLAIN (COSTS OFF))
--Testcase 498:
EXPLAIN (COSTS OFF)
SELECT count(value1), power(1-value3, 2) FROM numeric_tbl GROUP BY power(1-value3, 2) HAVING power(avg(value1), 2) > 0;

-- select power with group by having (result)
--Testcase 499:
SELECT count(value1), power(1-value3, 2) FROM numeric_tbl GROUP BY power(1-value3, 2) HAVING power(avg(value1), 2) > 0;

-- select power with group by index having (result)
--Testcase 500:
SELECT value1, power(1-value3, 2) FROM numeric_tbl GROUP BY 2,1 HAVING power(1-value3, 2) > 0;

-- select power with group by index having (result)
--Testcase 501:
SELECT value1, power(1-value3, 2) FROM numeric_tbl GROUP BY 1,2 HAVING value1 > 1;

-- select power and as
--Testcase 502:
SELECT power(value3, 2) as power1 FROM numeric_tbl;

-- ===============================================================================
-- test replace()
-- ===============================================================================
-- select replace (stub function, EXPLAIN (COSTS OFF))
--Testcase 503:
EXPLAIN (COSTS OFF)
SELECT replace(str1, 'XYZ', 'ABC'), replace(str2, 'XYZ', 'ABC') FROM numeric_tbl;
-- select replace (stub function, result)
--Testcase 504:
SELECT replace(str1, 'XYZ', 'ABC'), replace(str2, 'XYZ', 'ABC') FROM numeric_tbl;

-- select replace (stub function, not pushdown constraints, EXPLAIN (COSTS OFF))
--Testcase 505:
EXPLAIN (COSTS OFF)
SELECT value1, replace(str1, 'XYZ', 'ABC') FROM numeric_tbl WHERE to_hex(value2) = '64';
-- select replace (stub function, not pushdown constraints, result)
--Testcase 506:
SELECT value1, replace(str1, 'XYZ', 'ABC') FROM numeric_tbl WHERE to_hex(value2) = '64';

-- select replace (stub function, pushdown constraints, EXPLAIN (COSTS OFF))
--Testcase 507:
EXPLAIN (COSTS OFF)
SELECT value1, replace(str1, 'XYZ', 'ABC') FROM numeric_tbl WHERE value2 != 200;
-- select replace (stub function, pushdown constraints, result)
--Testcase 508:
SELECT value1, replace(str1, 'XYZ', 'ABC') FROM numeric_tbl WHERE value2 != 200;

-- select replace with non pushdown func and explicit constant (EXPLAIN (COSTS OFF))
--Testcase 509:
EXPLAIN (COSTS OFF)
SELECT replace(str1, 'XYZ', 'ABC'), pi(), 4.1 FROM numeric_tbl;
-- select replace with non pushdown func and explicit constant (result)
--Testcase 510:
SELECT replace(str1, 'XYZ', 'ABC'), pi(), 4.1 FROM numeric_tbl;

-- select replace with order by (EXPLAIN (COSTS OFF))
--Testcase 511:
EXPLAIN (COSTS OFF)
SELECT value1, replace(str1, 'XYZ', 'ABC') FROM numeric_tbl ORDER BY replace(str1, 'XYZ', 'ABC');
-- select replace with order by (result)
--Testcase 512:
SELECT value1, replace(str1, 'XYZ', 'ABC') FROM numeric_tbl ORDER BY replace(str1, 'XYZ', 'ABC');

-- select replace with order by index (result)
--Testcase 513:
SELECT value1, replace(str1, 'XYZ', 'ABC') FROM numeric_tbl ORDER BY 2,1;
-- select replace with order by index (result)
--Testcase 514:
SELECT value1, replace(str1, 'XYZ', 'ABC') FROM numeric_tbl ORDER BY 1,2;

-- select replace with group by (EXPLAIN (COSTS OFF))
--Testcase 515:
EXPLAIN (COSTS OFF)
SELECT count(value1), replace(str1, 'XYZ', 'ABC') FROM numeric_tbl GROUP BY replace(str1, 'XYZ', 'ABC');
-- select replace with group by (result)
--Testcase 516:
SELECT count(value1), replace(str1, 'XYZ', 'ABC') FROM numeric_tbl GROUP BY replace(str1, 'XYZ', 'ABC');

-- select replace with group by index (result)
--Testcase 517:
SELECT value1, replace(str1, 'XYZ', 'ABC') FROM numeric_tbl GROUP BY 2,1;

-- select replace with group by having (EXPLAIN (COSTS OFF))
--Testcase 518:
EXPLAIN (COSTS OFF)
SELECT count(value1), replace(str1, 'XYZ', 'ABC') FROM numeric_tbl GROUP BY replace(str1, 'XYZ', 'ABC'), str1 HAVING replace(str1, 'XYZ', 'ABC') IS NOT NULL;
-- select replace with group by having (result)
--Testcase 519:
SELECT count(value1), replace(str1, 'XYZ', 'ABC') FROM numeric_tbl GROUP BY replace(str1, 'XYZ', 'ABC'), str1 HAVING replace(str1, 'XYZ', 'ABC') IS NOT NULL;

-- select replace with group by index having (result)
--Testcase 520:
SELECT value1, replace(str1, 'XYZ', 'ABC') FROM numeric_tbl GROUP BY 1,2 HAVING value1 > 1;

-- ===============================================================================
-- test round()
-- ===============================================================================
-- select round (builtin function, EXPLAIN (COSTS OFF))
--Testcase 521:
EXPLAIN (COSTS OFF)
SELECT round(value1), round(value2), round(value3), round(value4), round(0.5) FROM numeric_tbl;

-- select round (builtin function, result)
--Testcase 522:
SELECT round(value1), round(value2), round(value3), round(value4), round(0.5) FROM numeric_tbl;

-- select round (builtin function, not pushdown constraints, EXPLAIN (COSTS OFF))
--Testcase 523:
EXPLAIN (COSTS OFF)
SELECT round(value1), round(value2), round(value3), round(value4), round(0.5) FROM numeric_tbl WHERE to_hex(value2) = '64';

-- select round (builtin function, not pushdown constraints, result)
--Testcase 524:
SELECT round(value1), round(value2), round(value3), round(value4), round(0.5) FROM numeric_tbl WHERE to_hex(value2) = '64';

-- select round (builtin function, pushdown constraints, EXPLAIN (COSTS OFF))
--Testcase 525:
EXPLAIN (COSTS OFF)
SELECT round(value1), round(value2), round(value3), round(value4), round(0.5) FROM numeric_tbl WHERE value2 != 200;

-- select round (builtin function, pushdown constraints, result)
--Testcase 526:
SELECT round(value1), round(value2), round(value3), round(value4), round(0.5) FROM numeric_tbl WHERE value2 != 200;

-- select round (builtin function, round in constraints, EXPLAIN (COSTS OFF))
--Testcase 527:
EXPLAIN (COSTS OFF)
SELECT round(value1), round(value2), round(value3), round(value4), round(0.5) FROM numeric_tbl WHERE round(value1) != 1;

-- select round (builtin function, round in constraints, result)
--Testcase 528:
SELECT round(value1), round(value2), round(value3), round(value4), round(0.5) FROM numeric_tbl WHERE round(value1) != 1;

-- select round (builtin function, round in constraints, EXPLAIN (COSTS OFF))
--Testcase 529:
EXPLAIN (COSTS OFF)
SELECT round(value1), round(value2), round(value3), round(value4), round(0.5) FROM numeric_tbl WHERE round(0.5) > value1;

-- select round (builtin function, round in constraints, result)
--Testcase 530:
SELECT round(value1), round(value2), round(value3), round(value4), round(0.5) FROM numeric_tbl WHERE round(0.5) > value1;

-- select round as nest function with agg (pushdown, EXPLAIN (COSTS OFF))
--Testcase 531:
EXPLAIN (COSTS OFF)
SELECT sum(value3),round(sum(value3)) FROM numeric_tbl;

-- select round as nest function with agg (pushdown, result)
--Testcase 532:
SELECT sum(value3),round(sum(value3)) FROM numeric_tbl;

-- select round as nest with log2 (pushdown, EXPLAIN (COSTS OFF))
--Testcase 533:
EXPLAIN (COSTS OFF)
SELECT round(log(2, value2)) FROM numeric_tbl;

-- select round as nest with log2 (pushdown, result)
--Testcase 534:
SELECT round(log(2, value2)) FROM numeric_tbl;

-- select round with non pushdown func and explicit constant (EXPLAIN (COSTS OFF))
--Testcase 535:
EXPLAIN (COSTS OFF)
SELECT round(value3), pi(), 4.1 FROM numeric_tbl;

-- select round with non pushdown func and explicit constant (result)
--Testcase 536:
SELECT round(value3), pi(), 4.1 FROM numeric_tbl;

-- select round with order by (EXPLAIN (COSTS OFF))
--Testcase 537:
EXPLAIN (COSTS OFF)
SELECT value1, round(1-value3) FROM numeric_tbl ORDER BY round(1-value3);

-- select round with order by (result)
--Testcase 538:
SELECT value1, round(1-value3) FROM numeric_tbl ORDER BY round(1-value3);

-- select round with order by index (result)
--Testcase 539:
SELECT value1, round(1-value3) FROM numeric_tbl ORDER BY 2,1;

-- select round with order by index (result)
--Testcase 540:
SELECT value1, round(1-value3) FROM numeric_tbl ORDER BY 1,2;

-- select round with group by (EXPLAIN (COSTS OFF))
--Testcase 541:
EXPLAIN (COSTS OFF)
SELECT count(value1), round(1-value3) FROM numeric_tbl GROUP BY round(1-value3);

-- select round with group by (result)
--Testcase 542:
SELECT count(value1), round(1-value3) FROM numeric_tbl GROUP BY round(1-value3);

-- select round with group by index (result)
--Testcase 543:
SELECT value1, round(1-value3) FROM numeric_tbl GROUP BY 2,1;

-- select round with group by index (result)
--Testcase 544:
SELECT value1, round(1-value3) FROM numeric_tbl GROUP BY 1,2;

-- select round with group by having (EXPLAIN (COSTS OFF))
--Testcase 545:
EXPLAIN (COSTS OFF)
SELECT count(value1), round(1-value3) FROM numeric_tbl GROUP BY round(1-value3) HAVING round(avg(value1)) > 0;

-- select round with group by having (result)
--Testcase 546:
SELECT count(value1), round(1-value3) FROM numeric_tbl GROUP BY round(1-value3) HAVING round(avg(value1)) > 0;

-- select round with group by index having (result)
--Testcase 547:
SELECT value1, round(1-value3) FROM numeric_tbl GROUP BY 2,1 HAVING round(1-value3) > 0;

-- select round with group by index having (result)
--Testcase 548:
SELECT value1, round(1-value3) FROM numeric_tbl GROUP BY 1,2 HAVING value1 > 1;

-- select round and as
--Testcase 549:
SELECT round(value3) as round1 FROM numeric_tbl;

-- ===============================================================================
-- test rpad()
-- ===============================================================================
-- select rpad (stub function, EXPLAIN (COSTS OFF))
--Testcase 550:
EXPLAIN (COSTS OFF)
SELECT rpad(str1, 16, str2), rpad(str1, 4, str2) FROM numeric_tbl;
-- select rpad (stub function, result)
--Testcase 551:
SELECT rpad(str1, 16, str2), rpad(str1, 4, str2) FROM numeric_tbl;

-- select rpad (stub function, not pushdown constraints, EXPLAIN (COSTS OFF))
--Testcase 552:
EXPLAIN (COSTS OFF)
SELECT value1, rpad(str1, 16, str2) FROM numeric_tbl WHERE to_hex(value2) = '64';
-- select rpad (stub function, not pushdown constraints, result)
--Testcase 553:
SELECT value1, rpad(str1, 16, str2) FROM numeric_tbl WHERE to_hex(value2) = '64';

-- select rpad (stub function, pushdown constraints, EXPLAIN (COSTS OFF))
--Testcase 554:
EXPLAIN (COSTS OFF)
SELECT value1, rpad(str1, 16, str2) FROM numeric_tbl WHERE value2 != 200;
-- select rpad (stub function, pushdown constraints, result)
--Testcase 555:
SELECT value1, rpad(str1, 16, str2) FROM numeric_tbl WHERE value2 != 200;

-- select rpad with non pushdown func and explicit constant (EXPLAIN (COSTS OFF))
--Testcase 556:
EXPLAIN (COSTS OFF)
SELECT rpad(str1, 16, str2), pi(), 4.1 FROM numeric_tbl;
-- select rpad with non pushdown func and explicit constant (result)
--Testcase 557:
SELECT rpad(str1, 16, str2), pi(), 4.1 FROM numeric_tbl;

-- select rpad with order by (EXPLAIN (COSTS OFF))
--Testcase 558:
EXPLAIN (COSTS OFF)
SELECT value1, rpad(str1, 16, str2) FROM numeric_tbl ORDER BY rpad(str1, 16, str2);
-- select rpad with order by (result)
--Testcase 559:
SELECT value1, rpad(str1, 16, str2) FROM numeric_tbl ORDER BY rpad(str1, 16, str2);

-- select rpad with order by index (result)
--Testcase 560:
SELECT value1, rpad(str1, 16, str2) FROM numeric_tbl ORDER BY 2,1;
-- select rpad with order by index (result)
--Testcase 561:
SELECT value1, rpad(str1, 16, str2) FROM numeric_tbl ORDER BY 1,2;

-- select rpad with group by (EXPLAIN (COSTS OFF))
--Testcase 562:
EXPLAIN (COSTS OFF)
SELECT count(value1), rpad(str1, 16, str2) FROM numeric_tbl GROUP BY rpad(str1, 16, str2);
-- select rpad with group by (result)
--Testcase 563:
SELECT count(value1), rpad(str1, 16, str2) FROM numeric_tbl GROUP BY rpad(str1, 16, str2);

-- select rpad with group by index (result)
--Testcase 564:
SELECT value1, rpad(str1, 16, str2) FROM numeric_tbl GROUP BY 2,1;

-- select rpad with group by having (EXPLAIN (COSTS OFF))
--Testcase 565:
EXPLAIN (COSTS OFF)
SELECT count(value1), rpad(str1, 16, str2) FROM numeric_tbl GROUP BY rpad(str1, 16, str2) HAVING rpad(str1, 16, str2) IS NOT NULL;
-- select rpad with group by having (result)
--Testcase 566:
SELECT count(value1), rpad(str1, 16, str2) FROM numeric_tbl GROUP BY rpad(str1, 16, str2) HAVING rpad(str1, 16, str2) IS NOT NULL;

-- select rpad with group by index having (result)
--Testcase 567:
SELECT value1, rpad(str1, 16, str2) FROM numeric_tbl GROUP BY 1,2 HAVING value1 > 1;

-- ===============================================================================
-- test rtrim()
-- ===============================================================================
-- select rtrim (stub function, EXPLAIN (COSTS OFF))
--Testcase 568:
EXPLAIN (COSTS OFF)
SELECT rtrim(str1), rtrim(str2, ' ') FROM numeric_tbl;
-- select rtrim (stub function, result)
--Testcase 569:
SELECT rtrim(str1), rtrim(str2, ' ') FROM numeric_tbl;

-- select rtrim (stub function, not pushdown constraints, EXPLAIN (COSTS OFF))
--Testcase 570:
EXPLAIN (COSTS OFF)
SELECT value1, rtrim(str1, '-') FROM numeric_tbl WHERE to_hex(value2) = '64';
-- select rtrim (stub function, not pushdown constraints, result)
--Testcase 571:
SELECT value1, rtrim(str1, '-') FROM numeric_tbl WHERE to_hex(value2) = '64';

-- select rtrim (stub function, pushdown constraints, EXPLAIN (COSTS OFF))
--Testcase 572:
EXPLAIN (COSTS OFF)
SELECT value1, rtrim(str1, '-') FROM numeric_tbl WHERE value2 != 200;
-- select rtrim (stub function, pushdown constraints, result)
--Testcase 573:
SELECT value1, rtrim(str1, '-') FROM numeric_tbl WHERE value2 != 200;

-- select rtrim with non pushdown func and explicit constant (EXPLAIN (COSTS OFF))
--Testcase 574:
EXPLAIN (COSTS OFF)
SELECT rtrim(str1, '-'), pi(), 4.1 FROM numeric_tbl;
-- select rtrim with non pushdown func and explicit constant (result)
--Testcase 575:
SELECT rtrim(str1, '-'), pi(), 4.1 FROM numeric_tbl;

-- select rtrim with order by (EXPLAIN (COSTS OFF))
--Testcase 576:
EXPLAIN (COSTS OFF)
SELECT value1, rtrim(str1, '-') FROM numeric_tbl ORDER BY rtrim(str1, '-');
-- select rtrim with order by (result)
--Testcase 577:
SELECT value1, rtrim(str1, '-') FROM numeric_tbl ORDER BY rtrim(str1, '-');

-- select rtrim with order by index (result)
--Testcase 578:
SELECT value1, rtrim(str1, '-') FROM numeric_tbl ORDER BY 2,1;
-- select rtrim with order by index (result)
--Testcase 579:
SELECT value1, rtrim(str1, '-') FROM numeric_tbl ORDER BY 1,2;

-- select rtrim with group by (EXPLAIN (COSTS OFF))
--Testcase 580:
EXPLAIN (COSTS OFF)
SELECT count(value1), rtrim(str1, '-') FROM numeric_tbl GROUP BY rtrim(str1, '-');
-- select rtrim with group by (result)
--Testcase 581:
SELECT count(value1), rtrim(str1, '-') FROM numeric_tbl GROUP BY rtrim(str1, '-');

-- select rtrim with group by index (result)
--Testcase 582:
SELECT value1, rtrim(str2) FROM numeric_tbl GROUP BY 2,1;

-- select rtrim with group by having (EXPLAIN (COSTS OFF))
--Testcase 583:
EXPLAIN (COSTS OFF)
SELECT count(value1), rtrim(str1, '-') FROM numeric_tbl GROUP BY rtrim(str1, '-') HAVING rtrim(str1, '-') IS NOT NULL;
-- select rtrim with group by having (result)
--Testcase 584:
SELECT count(value1), rtrim(str1, '-') FROM numeric_tbl GROUP BY rtrim(str1, '-') HAVING rtrim(str1, '-') IS NOT NULL;

-- select rtrim with group by index having (result)
--Testcase 585:
SELECT value1, rtrim(str1, '-') FROM numeric_tbl GROUP BY 1,2 HAVING value1 > 1;

-- ===============================================================================
-- test sign()
-- ===============================================================================
-- select sign (builtin function, EXPLAIN (COSTS OFF))
--Testcase 586:
EXPLAIN (COSTS OFF)
SELECT sign(value1), sign(value2), sign(value3), sign(value4), sign(0.5) FROM numeric_tbl;

-- select sign (builtin function, result)
--Testcase 587:
SELECT sign(value1), sign(value2), sign(value3), sign(value4), sign(0.5) FROM numeric_tbl;

-- select sign (builtin function, not pushdown constraints, EXPLAIN (COSTS OFF))
--Testcase 588:
EXPLAIN (COSTS OFF)
SELECT sign(value1), sign(value2), sign(value3), sign(value4), sign(0.5) FROM numeric_tbl WHERE to_hex(value2) = '64';

-- select sign (builtin function, not pushdown constraints, result)
--Testcase 589:
SELECT sign(value1), sign(value2), sign(value3), sign(value4), sign(0.5) FROM numeric_tbl WHERE to_hex(value2) = '64';

-- select sign (builtin function, pushdown constraints, EXPLAIN (COSTS OFF))
--Testcase 590:
EXPLAIN (COSTS OFF)
SELECT sign(value1), sign(value2), sign(value3), sign(value4), sign(0.5) FROM numeric_tbl WHERE value2 != 200;

-- select sign (builtin function, pushdown constraints, result)
--Testcase 591:
SELECT sign(value1), sign(value2), sign(value3), sign(value4), sign(0.5) FROM numeric_tbl WHERE value2 != 200;

-- select sign (builtin function, sign in constraints, EXPLAIN (COSTS OFF))
--Testcase 592:
EXPLAIN (COSTS OFF)
SELECT sign(value1), sign(value2), sign(value3), sign(value4), sign(0.5) FROM numeric_tbl WHERE sign(value1) != -1;

-- select sign (builtin function, sign in constraints, result)
--Testcase 593:
SELECT sign(value1), sign(value2), sign(value3), sign(value4), sign(0.5) FROM numeric_tbl WHERE sign(value1) != -1;

-- select sign (builtin function, sign in constraints, EXPLAIN (COSTS OFF))
--Testcase 594:
EXPLAIN (COSTS OFF)
SELECT sign(value1), sign(value2), sign(value3), sign(value4), sign(0.5) FROM numeric_tbl WHERE sign(0.5) > value1;

-- select sign (builtin function, sign in constraints, result)
--Testcase 595:
SELECT sign(value1), sign(value2), sign(value3), sign(value4), sign(0.5) FROM numeric_tbl WHERE sign(0.5) > value1;

-- select sign as nest function with agg (pushdown, EXPLAIN (COSTS OFF))
--Testcase 596:
EXPLAIN (COSTS OFF)
SELECT sum(value3),sign(sum(value3)) FROM numeric_tbl;

-- select sign as nest function with agg (pushdown, result)
--Testcase 597:
SELECT sum(value3),sign(sum(value3)) FROM numeric_tbl;

-- select sign as nest with log2 (pushdown, EXPLAIN (COSTS OFF))
--Testcase 598:
EXPLAIN (COSTS OFF)
SELECT sign(log(2, value2)) FROM numeric_tbl;

-- select sign as nest with log2 (pushdown, result)
--Testcase 599:
SELECT sign(log(2, value2)) FROM numeric_tbl;

-- select sign with non pushdown func and explicit constant (EXPLAIN (COSTS OFF))
--Testcase 600:
EXPLAIN (COSTS OFF)
SELECT sign(value3), pi(), 4.1 FROM numeric_tbl;

-- select sign with non pushdown func and explicit constant (result)
--Testcase 601:
SELECT sign(value3), pi(), 4.1 FROM numeric_tbl;

-- select sign with order by (EXPLAIN (COSTS OFF))
--Testcase 602:
EXPLAIN (COSTS OFF)
SELECT value1, sign(1-value3) FROM numeric_tbl ORDER BY sign(1-value3);

-- select sign with order by (result)
--Testcase 603:
SELECT value1, sign(1-value3) FROM numeric_tbl ORDER BY sign(1-value3);

-- select sign with order by index (result)
--Testcase 604:
SELECT value1, sign(1-value3) FROM numeric_tbl ORDER BY 2,1;

-- select sign with order by index (result)
--Testcase 605:
SELECT value1, sign(1-value3) FROM numeric_tbl ORDER BY 1,2;

-- select sign with group by (EXPLAIN (COSTS OFF))
--Testcase 606:
EXPLAIN (COSTS OFF)
SELECT count(value1), sign(1-value3) FROM numeric_tbl GROUP BY sign(1-value3);

-- select sign with group by (result)
--Testcase 607:
SELECT count(value1), sign(1-value3) FROM numeric_tbl GROUP BY sign(1-value3);

-- select sign with group by index (result)
--Testcase 608:
SELECT value1, sign(1-value3) FROM numeric_tbl GROUP BY 2,1;

-- select sign with group by index (result)
--Testcase 609:
SELECT value1, sign(1-value3) FROM numeric_tbl GROUP BY 1,2;

-- select sign with group by having (EXPLAIN (COSTS OFF))
--Testcase 610:
EXPLAIN (COSTS OFF)
SELECT count(value1), sign(1-value3) FROM numeric_tbl GROUP BY sign(1-value3) HAVING sign(avg(value1)) > 0;

-- select sign with group by having (result)
--Testcase 611:
SELECT count(value1), sign(1-value3) FROM numeric_tbl GROUP BY sign(1-value3) HAVING sign(avg(value1)) > 0;

-- select sign with group by index having (result)
--Testcase 612:
SELECT value1, sign(1-value3) FROM numeric_tbl GROUP BY 2,1 HAVING sign(1-value3) > 0;

-- select sign with group by index having (result)
--Testcase 613:
SELECT value1, sign(1-value3) FROM numeric_tbl GROUP BY 1,2 HAVING value1 > 1;

-- select sign and as
--Testcase 614:
SELECT sign(value3) as sign1 FROM numeric_tbl;

-- ===============================================================================
-- test sin()
-- ===============================================================================
-- select sin (builtin function, EXPLAIN (COSTS OFF))
--Testcase 615:
EXPLAIN (COSTS OFF)
SELECT value1, sin(value2), sin(value3), sin(value4), sin(0.5) FROM numeric_tbl;

-- select sin (builtin function, result)
--Testcase 616:
SELECT value1, sin(value2), sin(value3), sin(value4), sin(0.5) FROM numeric_tbl;

-- select sin (builtin function, not pushdown constraints, EXPLAIN (COSTS OFF))
--Testcase 617:
EXPLAIN (COSTS OFF)
SELECT sin(value1), sin(value2), sin(value3), sin(value4), sin(0.5) FROM numeric_tbl WHERE to_hex(value2) = '64';

-- select sin (builtin function, not pushdown constraints, result)
--Testcase 618:
SELECT sin(value1), sin(value2), sin(value3), sin(value4), sin(0.5) FROM numeric_tbl WHERE to_hex(value2) = '64';

-- select sin (builtin function, pushdown constraints, EXPLAIN (COSTS OFF))
--Testcase 619:
EXPLAIN (COSTS OFF)
SELECT sin(value1), sin(value2), sin(value3), sin(value4), sin(0.5) FROM numeric_tbl WHERE value2 != 200;

-- select sin (builtin function, pushdown constraints, result)
--Testcase 620:
SELECT sin(value1), sin(value2), sin(value3), sin(value4), sin(0.5) FROM numeric_tbl WHERE value2 != 200;

-- select sin (builtin function, sin in constraints, EXPLAIN (COSTS OFF))
--Testcase 621:
EXPLAIN (COSTS OFF)
SELECT value1, sin(value2), sin(value3), sin(value4), sin(0.5) FROM numeric_tbl WHERE sin(value1) != 1;

-- select sin (builtin function, sin in constraints, result)
--Testcase 622:
SELECT value1, sin(value2), sin(value3), sin(value4), sin(0.5) FROM numeric_tbl WHERE sin(value1) != 1;

-- select sin (builtin function, sin in constraints, EXPLAIN (COSTS OFF))
--Testcase 623:
EXPLAIN (COSTS OFF)
SELECT sin(value1), sin(value2), sin(value3), sin(value4), sin(0.5) FROM numeric_tbl WHERE sin(0.5) > value1;

-- select sin (builtin function, sin in constraints, result)
--Testcase 624:
SELECT sin(value1), sin(value2), sin(value3), sin(value4), sin(0.5) FROM numeric_tbl WHERE sin(0.5) > value1;

-- select sin as nest function with agg (pushdown, EXPLAIN (COSTS OFF))
--Testcase 625:
EXPLAIN (COSTS OFF)
SELECT sum(value3),sin(sum(value3)) FROM numeric_tbl;

-- select sin as nest function with agg (pushdown, result)
--Testcase 626:
SELECT sum(value3),sin(sum(value3)) FROM numeric_tbl;

-- select sin as nest with log2 (pushdown, EXPLAIN (COSTS OFF))
--Testcase 627:
EXPLAIN (COSTS OFF)
SELECT value1, sin(log(2, value2)) FROM numeric_tbl;

-- select sin as nest with log2 (pushdown, result)
--Testcase 628:
SELECT value1, sin(log(2, value2)) FROM numeric_tbl;

-- select sin with non pushdown func and explicit constant (EXPLAIN (COSTS OFF))
--Testcase 629:
EXPLAIN (COSTS OFF)
SELECT value1, sin(value3), pi(), 4.1 FROM numeric_tbl;

-- select sin with non pushdown func and explicit constant (result)
--Testcase 630:
SELECT value1, sin(value3), pi(), 4.1 FROM numeric_tbl;

-- select sin with order by (EXPLAIN (COSTS OFF))
--Testcase 631:
EXPLAIN (COSTS OFF)
SELECT value1, sin(1-value3) FROM numeric_tbl ORDER BY sin(1-value3);

-- select sin with order by (result)
--Testcase 632:
SELECT value1, sin(1-value3) FROM numeric_tbl ORDER BY sin(1-value3);

-- select sin with order by index (result)
--Testcase 633:
SELECT value1, sin(1-value3) FROM numeric_tbl ORDER BY 2,1;

-- select sin with order by index (result)
--Testcase 634:
SELECT value1, sin(1-value3) FROM numeric_tbl ORDER BY 1,2;

-- select sin with group by (EXPLAIN (COSTS OFF))
--Testcase 635:
EXPLAIN (COSTS OFF)
SELECT value1, sin(1-value3) FROM numeric_tbl GROUP BY value1, sin(1-value3);

-- select sin with group by (result)
--Testcase 636:
SELECT value1, sin(1-value3) FROM numeric_tbl GROUP BY value1, sin(1-value3);

-- select sin with group by index (result)
--Testcase 637:
SELECT value1, sin(1-value3) FROM numeric_tbl GROUP BY 2,1;

-- select sin with group by index (result)
--Testcase 638:
SELECT value1, sin(1-value3) FROM numeric_tbl GROUP BY 1,2;

-- select sin with group by having (EXPLAIN (COSTS OFF))
--Testcase 639:
EXPLAIN (COSTS OFF)
SELECT value1, sin(1-value3) FROM numeric_tbl GROUP BY value1, sin(1-value3) HAVING sin(avg(value1)) > 0;

-- select sin with group by having (result)
--Testcase 640:
SELECT value1, sin(1-value3) FROM numeric_tbl GROUP BY value1, sin(1-value3) HAVING sin(avg(value1)) > 0;

-- select sin with group by index having (result)
--Testcase 641:
SELECT value1, sin(1-value3) FROM numeric_tbl GROUP BY 2,1 HAVING sin(1-value3) > 0;

-- select sin with group by index having (result)
--Testcase 642:
SELECT value1, sin(1-value3) FROM numeric_tbl GROUP BY 1,2 HAVING value1 > 1;

-- select sin and as
--Testcase 643:
SELECT value1, sin(value3) as sin1 FROM numeric_tbl;

-- ===============================================================================
-- test sqrt()
-- ===============================================================================
-- select sqrt (builtin function, EXPLAIN (COSTS OFF))
--Testcase 644:
EXPLAIN (COSTS OFF)
SELECT sqrt(value1), sqrt(value2), sqrt(0.5) FROM numeric_tbl;

-- select sqrt (builtin function, result)
--Testcase 645:
SELECT sqrt(value1), sqrt(value2), sqrt(0.5) FROM numeric_tbl;

-- select sqrt (builtin function, not pushdown constraints, EXPLAIN (COSTS OFF))
--Testcase 646:
EXPLAIN (COSTS OFF)
SELECT sqrt(value1), sqrt(value2), sqrt(0.5) FROM numeric_tbl WHERE to_hex(value2) = '64';

-- select sqrt (builtin function, not pushdown constraints, result)
--Testcase 647:
SELECT sqrt(value1), sqrt(value2), sqrt(0.5) FROM numeric_tbl WHERE to_hex(value2) = '64';

-- select sqrt (builtin function, pushdown constraints, EXPLAIN (COSTS OFF))
--Testcase 648:
EXPLAIN (COSTS OFF)
SELECT sqrt(value1), sqrt(value2), sqrt(0.5) FROM numeric_tbl WHERE value2 != 200;

-- select sqrt (builtin function, pushdown constraints, result)
--Testcase 649:
SELECT sqrt(value1), sqrt(value2), sqrt(0.5) FROM numeric_tbl WHERE value2 != 200;

-- select sqrt (builtin function, sqrt in constraints, EXPLAIN (COSTS OFF))
--Testcase 650:
EXPLAIN (COSTS OFF)
SELECT sqrt(value1), sqrt(value2), sqrt(0.5) FROM numeric_tbl WHERE sqrt(value1) != 1;

-- select sqrt (builtin function, sqrt in constraints, result)
--Testcase 651:
SELECT sqrt(value1), sqrt(value2), sqrt(0.5) FROM numeric_tbl WHERE sqrt(value1) != 1;

-- select sqrt (builtin function, sqrt in constraints, EXPLAIN (COSTS OFF))
--Testcase 652:
EXPLAIN (COSTS OFF)
SELECT sqrt(value1), sqrt(value2), sqrt(0.5) FROM numeric_tbl WHERE sqrt(0.5) > value1;

-- select sqrt (builtin function, sqrt in constraints, result)
--Testcase 653:
SELECT sqrt(value1), sqrt(value2), sqrt(0.5) FROM numeric_tbl WHERE sqrt(0.5) > value1;

-- select sqrt as nest function with agg (pushdown, EXPLAIN (COSTS OFF))
--Testcase 654:
EXPLAIN (COSTS OFF)
SELECT sum(value3),sqrt(sum(value1)) FROM numeric_tbl;

-- select sqrt as nest function with agg (pushdown, result)
--Testcase 655:
SELECT sum(value3),sqrt(sum(value1)) FROM numeric_tbl;

-- select sqrt as nest with log2 (pushdown, EXPLAIN (COSTS OFF))
--Testcase 656:
EXPLAIN (COSTS OFF)
SELECT value1, sqrt(log(2, value2)) FROM numeric_tbl;

-- select sqrt as nest with log2 (pushdown, result)
--Testcase 657:
SELECT value1, sqrt(log(2, value2)) FROM numeric_tbl;

-- select sqrt with non pushdown func and explicit constant (EXPLAIN (COSTS OFF))
--Testcase 658:
EXPLAIN (COSTS OFF)
SELECT sqrt(value2), pi(), 4.1 FROM numeric_tbl;

-- select sqrt with non pushdown func and explicit constant (result)
--Testcase 659:
SELECT sqrt(value2), pi(), 4.1 FROM numeric_tbl;

-- select sqrt with order by (EXPLAIN (COSTS OFF))
--Testcase 660:
EXPLAIN (COSTS OFF)
SELECT value1, sqrt(1-value3) FROM numeric_tbl ORDER BY sqrt(1-value3);

-- select sqrt with order by (result)
--Testcase 661:
SELECT value1, sqrt(1-value3) FROM numeric_tbl ORDER BY sqrt(1-value3);

-- select sqrt with order by index (result)
--Testcase 662:
SELECT value1, sqrt(1-value3) FROM numeric_tbl ORDER BY 2,1;

-- select sqrt with order by index (result)
--Testcase 663:
SELECT value1, sqrt(1-value3) FROM numeric_tbl ORDER BY 1,2;

-- select sqrt with group by (EXPLAIN (COSTS OFF))
--Testcase 664:
EXPLAIN (COSTS OFF)
SELECT count(value1), sqrt(1-value3) FROM numeric_tbl GROUP BY sqrt(1-value3);

-- select sqrt with group by (result)
--Testcase 665:
SELECT count(value1), sqrt(1-value3) FROM numeric_tbl GROUP BY sqrt(1-value3);

-- select sqrt with group by index (result)
--Testcase 666:
SELECT value1, sqrt(1-value3) FROM numeric_tbl GROUP BY 2,1;

-- select sqrt with group by index (result)
--Testcase 667:
SELECT value1, sqrt(1-value3) FROM numeric_tbl GROUP BY 1,2;

-- select sqrt with group by having (EXPLAIN (COSTS OFF))
--Testcase 668:
EXPLAIN (COSTS OFF)
SELECT count(value1), sqrt(1-value3) FROM numeric_tbl GROUP BY sqrt(1-value3) HAVING sqrt(avg(value1)) > 0;

-- select sqrt with group by having (result)
--Testcase 669:
SELECT count(value1), sqrt(1-value3) FROM numeric_tbl GROUP BY sqrt(1-value3) HAVING sqrt(avg(value1)) > 0;

-- select sqrt with group by index having (result)
--Testcase 670:
SELECT value1, sqrt(1-value3) FROM numeric_tbl GROUP BY 2,1 HAVING sqrt(1-value3) > 0;

-- select sqrt with group by index having (result)
--Testcase 671:
SELECT value1, sqrt(1-value3) FROM numeric_tbl GROUP BY 1,2 HAVING value1 > 1;

-- select sqrt and as (return null with negative number)
--Testcase 672:
SELECT value1, value3 + 1, sqrt(value1 + 1) as sqrt1 FROM numeric_tbl;

-- ===============================================================================
-- test substr()
-- ===============================================================================
-- select substr (stub function, EXPLAIN (COSTS OFF))
--Testcase 673:
EXPLAIN (COSTS OFF)
SELECT substr(str1, 3), substr(str2, 3, 4) FROM numeric_tbl;
-- select substr (stub function, result)
--Testcase 674:
SELECT substr(str1, 3), substr(str2, 3, 4) FROM numeric_tbl;

-- select substr (stub function, not pushdown constraints, EXPLAIN (COSTS OFF))
--Testcase 675:
EXPLAIN (COSTS OFF)
SELECT value1, substr(str2, 3, 4) FROM numeric_tbl WHERE to_hex(value2) = '64';
-- select substr (stub function, not pushdown constraints, result)
--Testcase 676:
SELECT value1, substr(str2, 3, 4) FROM numeric_tbl WHERE to_hex(value2) = '64';

-- select substr (stub function, pushdown constraints, EXPLAIN (COSTS OFF))
--Testcase 677:
EXPLAIN (COSTS OFF)
SELECT value1, substr(str2, 3, 4) FROM numeric_tbl WHERE value2 != 200;
-- select substr (stub function, pushdown constraints, result)
--Testcase 678:
SELECT value1, substr(str2, 3, 4) FROM numeric_tbl WHERE value2 != 200;

-- select substr with non pushdown func and explicit constant (EXPLAIN (COSTS OFF))
--Testcase 679:
EXPLAIN (COSTS OFF)
SELECT substr(str2, 3, 4), pi(), 4.1 FROM numeric_tbl;
-- select substr with non pushdown func and explicit constant (result)
--Testcase 680:
SELECT substr(str2, 3, 4), pi(), 4.1 FROM numeric_tbl;

-- select substr with order by (EXPLAIN (COSTS OFF))
--Testcase 681:
EXPLAIN (COSTS OFF)
SELECT value1, substr(str2, 3, 4) FROM numeric_tbl ORDER BY substr(str2, 3, 4);
-- select substr with order by (result)
--Testcase 682:
SELECT value1, substr(str2, 3, 4) FROM numeric_tbl ORDER BY substr(str2, 3, 4);

-- select substr with order by index (result)
--Testcase 683:
SELECT value1, substr(str2, 3, 4) FROM numeric_tbl ORDER BY 2,1;
-- select substr with order by index (result)
--Testcase 684:
SELECT value1, substr(str2, 3, 4) FROM numeric_tbl ORDER BY 1,2;

-- select substr with group by (EXPLAIN (COSTS OFF))
--Testcase 685:
EXPLAIN (COSTS OFF)
SELECT count(value1), substr(str2, 3, 4) FROM numeric_tbl GROUP BY substr(str2, 3, 4);
-- select substr with group by (result)
--Testcase 686:
SELECT count(value1), substr(str2, 3, 4) FROM numeric_tbl GROUP BY substr(str2, 3, 4);

-- select substr with group by index (result)
--Testcase 687:
SELECT value1, substr(str2, 3, 4) FROM numeric_tbl GROUP BY 2,1;

-- select substr with group by having (EXPLAIN (COSTS OFF))
--Testcase 688:
EXPLAIN (COSTS OFF)
SELECT count(value1), substr(str2, 3, 4) FROM numeric_tbl GROUP BY substr(str2, 3, 4), str2 HAVING substr(str2, 3, 4) IS NOT NULL;
-- select substr with group by having (result)
--Testcase 689:
SELECT count(value1), substr(str2, 3, 4) FROM numeric_tbl GROUP BY substr(str2, 3, 4), str2 HAVING substr(str2, 3, 4) IS NOT NULL;

-- select substr with group by index having (result)
--Testcase 690:
SELECT value1, substr(str2, 3, 4) FROM numeric_tbl GROUP BY 1,2 HAVING value1 > 1;

-- ===============================================================================
-- test substring()
-- ===============================================================================
-- select substring (stub function, EXPLAIN (COSTS OFF))
--Testcase 691:
EXPLAIN (COSTS OFF)
SELECT substring(str1, 3), substring(str2, 3, 4) FROM numeric_tbl;
-- select substring (stub function, result)
--Testcase 692:
SELECT substring(str1, 3), substring(str2, 3, 4) FROM numeric_tbl;

-- select substring (stub function, EXPLAIN (COSTS OFF))
--Testcase 693:
EXPLAIN (COSTS OFF)
SELECT substring(str1 FROM 3), substring(str2 FROM 3 FOR 4) FROM numeric_tbl;
-- select substring (stub function, result)
--Testcase 694:
SELECT substring(str1 FROM 3), substring(str2 FROM 3 FOR 4) FROM numeric_tbl;

-- select substring (stub function, not pushdown constraints, EXPLAIN (COSTS OFF))
--Testcase 695:
EXPLAIN (COSTS OFF)
SELECT value1, substring(str2, 3, 4) FROM numeric_tbl WHERE to_hex(value2) = '64';
-- select substring (stub function, not pushdown constraints, result)
--Testcase 696:
SELECT value1, substring(str2, 3, 4) FROM numeric_tbl WHERE to_hex(value2) = '64';

-- select substring (stub function, pushdown constraints, EXPLAIN (COSTS OFF))
--Testcase 697:
EXPLAIN (COSTS OFF)
SELECT value1, substring(str2 FROM 3 FOR 4) FROM numeric_tbl WHERE value2 != 200;
-- select substring (stub function, pushdown constraints, result)
--Testcase 698:
SELECT value1, substring(str2 FROM 3 FOR 4) FROM numeric_tbl WHERE value2 != 200;

-- select substring with non pushdown func and explicit constant (EXPLAIN (COSTS OFF))
--Testcase 699:
EXPLAIN (COSTS OFF)
SELECT substring(str2 FROM 3 FOR 4), pi(), 4.1 FROM numeric_tbl;
-- select substring with non pushdown func and explicit constant (result)
--Testcase 700:
SELECT substring(str2 FROM 3 FOR 4), pi(), 4.1 FROM numeric_tbl;

-- select substring with order by (EXPLAIN (COSTS OFF))
--Testcase 701:
EXPLAIN (COSTS OFF)
SELECT value1, substring(str2 FROM 3 FOR 4) FROM numeric_tbl ORDER BY substring(str2 FROM 3 FOR 4);
-- select substring with order by (result)
--Testcase 702:
SELECT value1, substring(str2 FROM 3 FOR 4) FROM numeric_tbl ORDER BY substring(str2 FROM 3 FOR 4);

-- select substring with order by index (result)
--Testcase 703:
SELECT value1, substring(str2 FROM 3 FOR 4) FROM numeric_tbl ORDER BY 2,1;
-- select substring with order by index (result)
--Testcase 704:
SELECT value1, substring(str2 FROM 3 FOR 4) FROM numeric_tbl ORDER BY 1,2;

-- select substring with group by (EXPLAIN (COSTS OFF))
--Testcase 705:
EXPLAIN (COSTS OFF)
SELECT count(value1), substring(str2, 3, 4) FROM numeric_tbl GROUP BY substring(str2, 3, 4);
-- select substring with group by (result)
--Testcase 706:
SELECT count(value1), substring(str2, 3, 4) FROM numeric_tbl GROUP BY substring(str2, 3, 4);

-- select substring with group by index (result)
--Testcase 707:
SELECT value1, substring(str2, 3, 4) FROM numeric_tbl GROUP BY 2,1;

-- select substring with group by having (EXPLAIN (COSTS OFF))
--Testcase 708:
EXPLAIN (COSTS OFF)
SELECT count(value1), substring(str2, 3, 4) FROM numeric_tbl GROUP BY substring(str2, 3, 4), str2 HAVING substring(str2, 3, 4) IS NOT NULL;
-- select substring with group by having (result)
--Testcase 709:
SELECT count(value1), substring(str2, 3, 4) FROM numeric_tbl GROUP BY substring(str2, 3, 4), str2 HAVING substring(str2, 3, 4) IS NOT NULL;

-- select substring with group by index having (result)
--Testcase 710:
SELECT value1, substring(str2, 3, 4) FROM numeric_tbl GROUP BY 1,2 HAVING value1 > 1;

-- ===============================================================================
-- test tan()
-- ===============================================================================
-- select tan (builtin function, EXPLAIN (COSTS OFF))
--Testcase 711:
EXPLAIN (COSTS OFF)
SELECT value1, tan(value2), tan(value3), tan(value4), tan(0.5) FROM numeric_tbl;

-- select tan (builtin function, result)
--Testcase 712:
SELECT value1, tan(value2), tan(value3), tan(value4), tan(0.5) FROM numeric_tbl;

-- select tan (builtin function, not pushdown constraints, EXPLAIN (COSTS OFF))
--Testcase 713:
EXPLAIN (COSTS OFF)
SELECT tan(value1), tan(value2), tan(value3), tan(value4), tan(0.5) FROM numeric_tbl WHERE to_hex(value2) = '64';

-- select tan (builtin function, not pushdown constraints, result)
--Testcase 714:
SELECT tan(value1), tan(value2), tan(value3), tan(value4), tan(0.5) FROM numeric_tbl WHERE to_hex(value2) = '64';

-- select tan (builtin function, pushdown constraints, EXPLAIN (COSTS OFF))
--Testcase 715:
EXPLAIN (COSTS OFF)
SELECT tan(value1), tan(value2), tan(value3), tan(value4), tan(0.5) FROM numeric_tbl WHERE value2 != 200;

-- select tan (builtin function, pushdown constraints, result)
--Testcase 716:
SELECT tan(value1), tan(value2), tan(value3), tan(value4), tan(0.5) FROM numeric_tbl WHERE value2 != 200;

-- select tan (builtin function, tan in constraints, EXPLAIN (COSTS OFF))
--Testcase 717:
EXPLAIN (COSTS OFF)
SELECT value1, tan(value2), tan(value3), tan(value4), tan(0.5) FROM numeric_tbl WHERE tan(value1) != 1;

-- select tan (builtin function, tan in constraints, result)
--Testcase 718:
SELECT value1, tan(value2), tan(value3), tan(value4), tan(0.5) FROM numeric_tbl WHERE tan(value1) != 1;

-- select tan (builtin function, tan in constraints, EXPLAIN (COSTS OFF))
--Testcase 719:
EXPLAIN (COSTS OFF)
SELECT tan(value1), tan(value2), tan(value3), tan(value4), tan(0.5) FROM numeric_tbl WHERE tan(0.5) > value1;

-- select tan (builtin function, tan in constraints, result)
--Testcase 720:
SELECT tan(value1), tan(value2), tan(value3), tan(value4), tan(0.5) FROM numeric_tbl WHERE tan(0.5) > value1;

-- select tan as nest function with agg (pushdown, EXPLAIN (COSTS OFF))
--Testcase 721:
EXPLAIN (COSTS OFF)
SELECT sum(value3),tan(sum(value3)) FROM numeric_tbl;

-- select tan as nest function with agg (pushdown, result)
--Testcase 722:
SELECT sum(value3),tan(sum(value3)) FROM numeric_tbl;

-- select tan as nest with log2 (pushdown, EXPLAIN (COSTS OFF))
--Testcase 723:
EXPLAIN (COSTS OFF)
SELECT value1, tan(log(2, value2)) FROM numeric_tbl;

-- select tan as nest with log2 (pushdown, result)
--Testcase 724:
SELECT value1, tan(log(2, value2)) FROM numeric_tbl;

-- select tan with non pushdown func and explicit constant (EXPLAIN (COSTS OFF))
--Testcase 725:
EXPLAIN (COSTS OFF)
SELECT value1, tan(value3), pi(), 4.1 FROM numeric_tbl;

-- select tan with non pushdown func and explicit constant (result)
--Testcase 726:
SELECT value1, tan(value3), pi(), 4.1 FROM numeric_tbl;

-- select tan with order by (EXPLAIN (COSTS OFF))
--Testcase 727:
EXPLAIN (COSTS OFF)
SELECT value1, tan(1-value3) FROM numeric_tbl ORDER BY tan(1-value3);

-- select tan with order by (result)
--Testcase 728:
SELECT value1, tan(1-value3) FROM numeric_tbl ORDER BY tan(1-value3);

-- select tan with order by index (result)
--Testcase 729:
SELECT value1, tan(1-value3) FROM numeric_tbl ORDER BY 2,1;

-- select tan with order by index (result)
--Testcase 730:
SELECT value1, tan(1-value3) FROM numeric_tbl ORDER BY 1,2;

-- select tan with group by (EXPLAIN (COSTS OFF))
--Testcase 731:
EXPLAIN (COSTS OFF)
SELECT value1, tan(1-value3) FROM numeric_tbl GROUP BY value1, tan(1-value3);

-- select tan with group by (result)
--Testcase 732:
SELECT value1, tan(1-value3) FROM numeric_tbl GROUP BY value1, tan(1-value3);

-- select tan with group by index (result)
--Testcase 733:
SELECT value1, tan(1-value3) FROM numeric_tbl GROUP BY 2,1;

-- select tan with group by index (result)
--Testcase 734:
SELECT value1, tan(1-value3) FROM numeric_tbl GROUP BY 1,2;

-- select tan with group by having (EXPLAIN (COSTS OFF))
--Testcase 735:
EXPLAIN (COSTS OFF)
SELECT value1, tan(1-value3) FROM numeric_tbl GROUP BY value1, tan(1-value3) HAVING tan(avg(value1)) > 0;

-- select tan with group by having (result)
--Testcase 736:
SELECT value1, tan(1-value3) FROM numeric_tbl GROUP BY value1, tan(1-value3) HAVING tan(avg(value1)) > 0;

-- select tan with group by index having (result)
--Testcase 737:
SELECT value1, tan(1-value3) FROM numeric_tbl GROUP BY 2,1 HAVING tan(1-value3) > 0;

-- select tan with group by index having (result)
--Testcase 738:
SELECT value1, tan(1-value3) FROM numeric_tbl GROUP BY 1,2 HAVING value1 > 1;

-- select tan and as
--Testcase 739:
SELECT value1, tan(value3) as tan1 FROM numeric_tbl;

-- ===============================================================================
-- test upper()
-- ===============================================================================
-- select upper (stub function, EXPLAIN (COSTS OFF))
--Testcase 740:
EXPLAIN (COSTS OFF)
SELECT upper(tag1), upper(str1), upper(str2) FROM numeric_tbl;
-- select upper (stub function, result)
--Testcase 741:
SELECT upper(tag1), upper(str1), upper(str2) FROM numeric_tbl;

-- select upper (stub function, not pushdown constraints, EXPLAIN (COSTS OFF))
--Testcase 742:
EXPLAIN (COSTS OFF)
SELECT value1, upper(tag1) FROM numeric_tbl WHERE to_hex(value2) = '64';
-- select upper (stub function, not pushdown constraints, result)
--Testcase 743:
SELECT value1, upper(tag1) FROM numeric_tbl WHERE to_hex(value2) = '64';

-- select upper (stub function, pushdown constraints, EXPLAIN (COSTS OFF))
--Testcase 744:
EXPLAIN (COSTS OFF)
SELECT value1, upper(str1) FROM numeric_tbl WHERE value2 != 200;
-- select upper (stub function, pushdown constraints, result)
--Testcase 745:
SELECT value1, upper(str1) FROM numeric_tbl WHERE value2 != 200;

-- select upper with non pushdown func and explicit constant (EXPLAIN (COSTS OFF))
--Testcase 746:
EXPLAIN (COSTS OFF)
SELECT upper(str1), pi(), 4.1 FROM numeric_tbl;
-- select ucase with non pushdown func and explicit constant (result)
--Testcase 747:
SELECT upper(str1), pi(), 4.1 FROM numeric_tbl;

-- select upper with order by (EXPLAIN (COSTS OFF))
--Testcase 748:
EXPLAIN (COSTS OFF)
SELECT value1, upper(str1) FROM numeric_tbl ORDER BY upper(str1);
-- select upper with order by (result)
--Testcase 749:
SELECT value1, upper(str1) FROM numeric_tbl ORDER BY upper(str1);

-- select upper with order by index (result)
--Testcase 750:
SELECT value1, upper(str1) FROM numeric_tbl ORDER BY 2,1;
-- select upper with order by index (result)
--Testcase 751:
SELECT value1, upper(str1) FROM numeric_tbl ORDER BY 1,2;

-- select upper with group by (EXPLAIN (COSTS OFF))
--Testcase 752:
EXPLAIN (COSTS OFF)
SELECT count(value1), upper(str1) FROM numeric_tbl GROUP BY upper(str1);
-- select upper with group by (result)
--Testcase 753:
SELECT count(value1), upper(str1) FROM numeric_tbl GROUP BY upper(str1);

-- select upper with group by index (result)
--Testcase 754:
SELECT value1, upper(str1) FROM numeric_tbl GROUP BY 2,1;

-- select upper with group by having (EXPLAIN (COSTS OFF))
--Testcase 755:
EXPLAIN (COSTS OFF)
SELECT count(value1), upper(str1) FROM numeric_tbl GROUP BY upper(str1), tag1 HAVING upper(str1) IS NOT NULL;
-- select upper with group by having (result)
--Testcase 756:
SELECT count(value1), upper(str1) FROM numeric_tbl GROUP BY upper(str1), tag1 HAVING upper(str1) IS NOT NULL;

-- select upper with group by index having (result)
--Testcase 757:
SELECT value1, upper(tag1) FROM numeric_tbl GROUP BY 1,2 HAVING value1 > 1;

-- ===============================================================================
-- test log()
-- ===============================================================================
-- select log (builtin function, numeric cast, EXPLAIN (COSTS OFF))
-- log_<base>(v) : postgresql (base, v), mysql (base, v)
--Testcase 758:
EXPLAIN (COSTS OFF)
SELECT value1, log(value1::numeric, value2::numeric) FROM numeric_tbl WHERE value1 != 1;

-- select log (builtin function, numeric cast, result)
--Testcase 759:
SELECT value1, log(value1::numeric, value2::numeric) FROM numeric_tbl WHERE value1 != 1;

-- select log (builtin function,  float8, EXPLAIN (COSTS OFF))
--Testcase 760:
EXPLAIN (COSTS OFF)
SELECT value1, log(value1::numeric, 0.1) FROM numeric_tbl WHERE value1 != 1;

-- select log (builtin function, float8, result)
--Testcase 761:
SELECT value1, log(value1::numeric, 0.1) FROM numeric_tbl WHERE value1 != 1;

-- select log (builtin function, bigint, EXPLAIN (COSTS OFF))
--Testcase 762:
EXPLAIN (COSTS OFF)
SELECT value1, log(value2::numeric, 3) FROM numeric_tbl WHERE value1 != 1;

-- select log (builtin function, bigint, result)
--Testcase 763:
SELECT value1, log(value2::numeric, 3) FROM numeric_tbl WHERE value1 != 1;

-- select log (builtin function, mix type, EXPLAIN (COSTS OFF))
--Testcase 764:
EXPLAIN (COSTS OFF)
SELECT value1, log(value1::numeric, value2::numeric) FROM numeric_tbl WHERE value1 != 1;

-- select log (builtin function,  mix type, result)
--Testcase 765:
SELECT value1, log(value1::numeric, value2::numeric) FROM numeric_tbl WHERE value1 != 1;

-- select log(v) -- built in function
-- log(v): postgreSQL base 10 logarithm
--Testcase 766:
EXPLAIN (COSTS OFF)
SELECT log(10, value2) FROM numeric_tbl WHERE value1 != 1;
--Testcase 767:
SELECT log(10, value2) FROM numeric_tbl WHERE value1 != 1;

-- select log (builtin function, EXPLAIN (COSTS OFF))
--Testcase 768:
EXPLAIN (COSTS OFF)
SELECT log(10, value2), log(10, value2), log(0.5) FROM numeric_tbl;

-- select log (builtin function, result)
--Testcase 769:
SELECT log(10, value2), log(10, value2), log(0.5) FROM numeric_tbl;

-- select log (builtin function, not pushdown constraints, EXPLAIN (COSTS OFF))
--Testcase 770:
EXPLAIN (COSTS OFF)
SELECT log(10, value2), log(10, value2), log(0.5) FROM numeric_tbl WHERE to_hex(value2) = '64';

-- select log (builtin function, not pushdown constraints, result)
--Testcase 771:
SELECT log(10, value2), log(10, value2), log(0.5) FROM numeric_tbl WHERE to_hex(value2) = '64';

-- select log (builtin function, pushdown constraints, EXPLAIN (COSTS OFF))
--Testcase 772:
EXPLAIN (COSTS OFF)
SELECT log(10, value2), log(10, value2), log(0.5) FROM numeric_tbl WHERE value2 != 200;

-- select log (builtin function, pushdown constraints, result)
--Testcase 773:
SELECT log(10, value2), log(10, value2), log(0.5) FROM numeric_tbl WHERE value2 != 200;

-- select log (builtin function, log in constraints, EXPLAIN (COSTS OFF))
--Testcase 774:
EXPLAIN (COSTS OFF)
SELECT log(10, value2), log(10, value2), log(0.5) FROM numeric_tbl WHERE log(10, value2) != 1;

-- select log (builtin function, log in constraints, result)
--Testcase 775:
SELECT log(10, value2), log(10, value2), log(0.5) FROM numeric_tbl WHERE log(10, value2) != 1;

-- select log (builtin function, log in constraints, EXPLAIN (COSTS OFF))
--Testcase 776:
EXPLAIN (COSTS OFF)
SELECT log(10, value2), log(10, value2), log(0.5) FROM numeric_tbl WHERE log(5) > value1;

-- select log (builtin function, log in constraints, result)
--Testcase 777:
SELECT log(10, value2), log(10, value2), log(0.5) FROM numeric_tbl WHERE log(5) > value1;

-- select log as nest function with agg (pushdown, EXPLAIN (COSTS OFF))
--Testcase 778:
EXPLAIN (COSTS OFF)
SELECT sum(value3),log(10, sum(value2)) FROM numeric_tbl;

-- select log as nest function with agg (pushdown, result)
--Testcase 779:
SELECT sum(value3),log(10, sum(value2)) FROM numeric_tbl;

-- select log as nest with log2 (pushdown, EXPLAIN (COSTS OFF))
--Testcase 780:
EXPLAIN (COSTS OFF)
SELECT value1, log(log(2, value2)) FROM numeric_tbl;

-- select log as nest with log2 (pushdown, result)
--Testcase 781:
SELECT value1, log(log(2, value2)) FROM numeric_tbl;

-- select log with non pushdown func and explicit constant (EXPLAIN (COSTS OFF))
--Testcase 782:
EXPLAIN (COSTS OFF)
SELECT log(10, value2), pi(), 4.1 FROM numeric_tbl;

-- select log with non pushdown func and explicit constant (result)
--Testcase 783:
SELECT log(10, value2), pi(), 4.1 FROM numeric_tbl;

-- select log with order by (EXPLAIN (COSTS OFF))
--Testcase 784:
EXPLAIN (COSTS OFF)
SELECT value3, log(10, value2) FROM numeric_tbl ORDER BY log(10, value2);

-- select log with order by (result)
--Testcase 785:
SELECT value3, log(10, value2) FROM numeric_tbl ORDER BY log(10, value2);

-- select log with order by index (result)
--Testcase 786:
SELECT value3, log(10, value2) FROM numeric_tbl ORDER BY 2,1;

-- select log with order by index (result)
--Testcase 787:
SELECT value3, log(10, value2) FROM numeric_tbl ORDER BY 1,2;

-- select log with group by (EXPLAIN (COSTS OFF))
--Testcase 788:
EXPLAIN (COSTS OFF)
SELECT count(value1), log(10, value2) FROM numeric_tbl GROUP BY log(10, value2);

-- select log with group by (result)
--Testcase 789:
SELECT count(value1), log(10, value2) FROM numeric_tbl GROUP BY log(10, value2);

-- select log with group by index (result)
--Testcase 790:
SELECT value1, log(10, value2) FROM numeric_tbl GROUP BY 2,1;

-- select log with group by index (result)
--Testcase 791:
SELECT value1, log(10, value2) FROM numeric_tbl GROUP BY 1,2;

-- select log with group by having (EXPLAIN (COSTS OFF))
--Testcase 792:
EXPLAIN (COSTS OFF)
SELECT count(value1), log(10, value2) FROM numeric_tbl GROUP BY log(10, value2) HAVING log(10, avg(value2)) > 0;

-- select log with group by having (result)
--Testcase 793:
SELECT count(value1), log(10, value2) FROM numeric_tbl GROUP BY log(10, value2) HAVING log(10, avg(value2)) > 0;

-- select log with group by index having (result)
--Testcase 794:
SELECT value3, log(10, value2) FROM numeric_tbl GROUP BY 2,1 HAVING log(10, value2) < 0;

-- select log with group by index having (result)
--Testcase 795:
SELECT value3, log(10, value2) FROM numeric_tbl GROUP BY 1,2 HAVING value3 > 1;

-- select log and as
--Testcase 796:
SELECT log(10, value2) as log1 FROM numeric_tbl;

-- ===============================================================================
-- test ln()
-- ===============================================================================
-- select ln as nest function with agg (pushdown, EXPLAIN (COSTS OFF))
--Testcase 797:
EXPLAIN (COSTS OFF)
SELECT sum(value3),ln(sum(value1)) FROM numeric_tbl;

-- select ln as nest function with agg (pushdown, result)
--Testcase 798:
SELECT sum(value3),ln(sum(value1)) FROM numeric_tbl;

-- select ln as nest with log2 (pushdown, EXPLAIN (COSTS OFF))
--Testcase 799:
EXPLAIN (COSTS OFF)
SELECT value1, ln(log(2, value2)) FROM numeric_tbl;

-- select ln as nest with log2 (pushdown, result)
--Testcase 800:
SELECT value1, ln(log(2, value2)) FROM numeric_tbl;

-- select ln with non pushdown func and explicit constant (EXPLAIN (COSTS OFF))
--Testcase 801:
EXPLAIN (COSTS OFF)
SELECT ln(value2), pi(), 4.1 FROM numeric_tbl;

-- select ln with non pushdown func and explicit constant (result)
--Testcase 802:
SELECT ln(value2), pi(), 4.1 FROM numeric_tbl;

-- select ln with order by (EXPLAIN (COSTS OFF))
--Testcase 803:
EXPLAIN (COSTS OFF)
SELECT value1, ln(1-value3) FROM numeric_tbl ORDER BY ln(1-value3);

-- select ln with order by (result)
--Testcase 804:
SELECT value1, ln(1-value3) FROM numeric_tbl ORDER BY ln(1-value3);

-- select ln with order by index (result)
--Testcase 805:
SELECT value1, ln(1-value3) FROM numeric_tbl ORDER BY 2,1;

-- select ln with order by index (result)
--Testcase 806:
SELECT value1, ln(1-value3) FROM numeric_tbl ORDER BY 1,2;

-- select ln with group by (EXPLAIN (COSTS OFF))
--Testcase 807:
EXPLAIN (COSTS OFF)
SELECT count(value1), ln(1-value3) FROM numeric_tbl GROUP BY ln(1-value3);

-- select ln with group by (result)
--Testcase 808:
SELECT count(value1), ln(1-value3) FROM numeric_tbl GROUP BY ln(1-value3);

-- select ln with group by index (result)
--Testcase 809:
SELECT value1, ln(1-value3) FROM numeric_tbl GROUP BY 2,1;

-- select ln with group by index (result)
--Testcase 810:
SELECT value1, ln(1-value3) FROM numeric_tbl GROUP BY 1,2;

-- select ln with group by having (EXPLAIN (COSTS OFF))
--Testcase 811:
EXPLAIN (COSTS OFF)
SELECT count(value1), ln(1-value3) FROM numeric_tbl GROUP BY ln(1-value3) HAVING ln(avg(value1)) > 0;

-- select ln with group by having (result)
--Testcase 812:
SELECT count(value1), ln(1-value3) FROM numeric_tbl GROUP BY ln(1-value3) HAVING ln(avg(value1)) > 0;

-- select ln with group by index having (result)
--Testcase 813:
SELECT value1, ln(1-value3) FROM numeric_tbl GROUP BY 2,1 HAVING ln(1-value3) < 0;

-- select ln with group by index having (result)
--Testcase 814:
SELECT value1, ln(1-value3) FROM numeric_tbl GROUP BY 1,2 HAVING value1 > 1;

-- select ln and as
--Testcase 815:
SELECT ln(value1) as ln1 FROM numeric_tbl;

-- select ln (builtin function, EXPLAIN (COSTS OFF))
--Testcase 816:
EXPLAIN (COSTS OFF)
SELECT ln(value1), ln(value2), ln(value3 + 10), ln(0.5) FROM numeric_tbl;

-- select ln (builtin function, result)
--Testcase 817:
SELECT ln(value1), ln(value2), ln(value3 + 10), ln(0.5) FROM numeric_tbl;

-- select ln (builtin function, not pushdown constraints, EXPLAIN (COSTS OFF))
--Testcase 818:
EXPLAIN (COSTS OFF)
SELECT ln(value1), ln(value2), ln(value3 + 10), ln(0.5) FROM numeric_tbl WHERE to_hex(value2) = '64';

-- select ln (builtin function, not pushdown constraints, result)
--Testcase 819:
SELECT ln(value1), ln(value2), ln(value3 + 10), ln(0.5) FROM numeric_tbl WHERE to_hex(value2) = '64';

-- select ln (builtin function, pushdown constraints, EXPLAIN (COSTS OFF))
--Testcase 820:
EXPLAIN (COSTS OFF)
SELECT ln(value1), ln(value2), ln(value3 + 10), ln(0.5) FROM numeric_tbl WHERE value2 != 200;

-- select ln (builtin function, pushdown constraints, result)
--Testcase 821:
SELECT ln(value1), ln(value2), ln(value3 + 10), ln(0.5) FROM numeric_tbl WHERE value2 != 200;

-- select ln (builtin function, ln in constraints, EXPLAIN (COSTS OFF))
--Testcase 822:
EXPLAIN (COSTS OFF)
SELECT ln(value1), ln(value2), ln(value3 + 10), ln(0.5) FROM numeric_tbl WHERE ln(value1) != 1;

-- select ln (builtin function, ln in constraints, result)
--Testcase 823:
SELECT ln(value1), ln(value2), ln(value3 + 10), ln(0.5) FROM numeric_tbl WHERE ln(value1) != 1;

-- select ln (builtin function, ln in constraints, EXPLAIN (COSTS OFF))
--Testcase 824:
EXPLAIN (COSTS OFF)
SELECT ln(value1), ln(value2), ln(value3 + 10), ln(0.5) FROM numeric_tbl WHERE ln(0.5) < value1;

-- select ln (builtin function, ln in constraints, result)
--Testcase 825:
SELECT ln(value1), ln(value2), ln(value3 + 10), ln(0.5) FROM numeric_tbl WHERE ln(0.5) < value1;

-- ===============================================================================
-- test floor()
-- ===============================================================================
-- select floor (builtin function, EXPLAIN (COSTS OFF))
--Testcase 826:
EXPLAIN (COSTS OFF)
SELECT floor(value1), floor(value2), floor(value3), floor(value4), floor(1.5) FROM numeric_tbl;

-- select floor (builtin function, result)
--Testcase 827:
SELECT floor(value1), floor(value2), floor(value3), floor(value4), floor(1.5) FROM numeric_tbl;

-- select floor (builtin function, not pushdown constraints, EXPLAIN (COSTS OFF))
--Testcase 828:
EXPLAIN (COSTS OFF)
SELECT floor(value1), floor(value2), floor(value3), floor(value4), floor(1.5) FROM numeric_tbl WHERE to_hex(value2) = '64';

-- select floor (builtin function, not pushdown constraints, result)
--Testcase 829:
SELECT floor(value1), floor(value2), floor(value3), floor(value4), floor(1.5) FROM numeric_tbl WHERE to_hex(value2) = '64';

-- select floor (builtin function, pushdown constraints, EXPLAIN (COSTS OFF))
--Testcase 830:
EXPLAIN (COSTS OFF)
SELECT floor(value1), floor(value2), floor(value3), floor(value4), floor(1.5) FROM numeric_tbl WHERE value2 != 200;

-- select floor (builtin function, pushdown constraints, result)
--Testcase 831:
SELECT floor(value1), floor(value2), floor(value3), floor(value4), floor(1.5) FROM numeric_tbl WHERE value2 != 200;

-- select floor (builtin function, floor in constraints, EXPLAIN (COSTS OFF))
--Testcase 832:
EXPLAIN (COSTS OFF)
SELECT floor(value1), floor(value2), floor(value3), floor(value4), floor(1.5) FROM numeric_tbl WHERE floor(value1) != 1;

-- select floor (builtin function, floor in constraints, result)
--Testcase 833:
SELECT floor(value1), floor(value2), floor(value3), floor(value4), floor(1.5) FROM numeric_tbl WHERE floor(value1) != 1;

-- select floor (builtin function, floor in constraints, EXPLAIN (COSTS OFF))
--Testcase 834:
EXPLAIN (COSTS OFF)
SELECT floor(value1), floor(value2), floor(value3), floor(value4), floor(1.5) FROM numeric_tbl WHERE floor(1.5) > value1;

-- select floor (builtin function, floor in constraints, result)
--Testcase 835:
SELECT floor(value1), floor(value2), floor(value3), floor(value4), floor(1.5) FROM numeric_tbl WHERE floor(1.5) > value1;

-- select floor as nest function with agg (pushdown, EXPLAIN (COSTS OFF))
--Testcase 836:
EXPLAIN (COSTS OFF)
SELECT sum(value3),floor(sum(value3)) FROM numeric_tbl;

-- select floor as nest function with agg (pushdown, result)
--Testcase 837:
SELECT sum(value3),floor(sum(value3)) FROM numeric_tbl;

-- select floor as nest with log2 (pushdown, EXPLAIN (COSTS OFF))
--Testcase 838:
EXPLAIN (COSTS OFF)
SELECT floor(log(2, value2)) FROM numeric_tbl;

-- select floor as nest with log2 (pushdown, result)
--Testcase 839:
SELECT floor(log(2, value2)) FROM numeric_tbl;

-- select floor with non pushdown func and explicit constant (EXPLAIN (COSTS OFF))
--Testcase 840:
EXPLAIN (COSTS OFF)
SELECT floor(value3), pi(), 4.1 FROM numeric_tbl;

-- select floor with non pushdown func and explicit constant (result)
--Testcase 841:
SELECT floor(value3), pi(), 4.1 FROM numeric_tbl;

-- select floor with order by (EXPLAIN (COSTS OFF))
--Testcase 842:
EXPLAIN (COSTS OFF)
SELECT value1, floor(10 - value1) FROM numeric_tbl ORDER BY floor(10 - value1);

-- select floor with order by (result)
--Testcase 843:
SELECT value1, floor(10 - value1) FROM numeric_tbl ORDER BY floor(10 - value1);

-- select floor with order by index (result)
--Testcase 844:
SELECT value1, floor(10 - value1) FROM numeric_tbl ORDER BY 2,1;

-- select floor with order by index (result)
--Testcase 845:
SELECT value1, floor(10 - value1) FROM numeric_tbl ORDER BY 1,2;

-- select floor with group by (EXPLAIN (COSTS OFF))
--Testcase 846:
EXPLAIN (COSTS OFF)
SELECT value1, floor(10 - value1) FROM numeric_tbl GROUP BY value1, floor(10 - value1);

-- select floor with group by (result)
--Testcase 847:
SELECT value1, floor(10 - value1) FROM numeric_tbl GROUP BY value1, floor(10 - value1);

-- select floor with group by index (result)
--Testcase 848:
SELECT value1, floor(10 - value1) FROM numeric_tbl GROUP BY 2,1;

-- select floor with group by index (result)
--Testcase 849:
SELECT value1, floor(10 - value1) FROM numeric_tbl GROUP BY 1,2;

-- select floor with group by having (EXPLAIN (COSTS OFF))
--Testcase 850:
EXPLAIN (COSTS OFF)
SELECT value1, floor(10 - value1) FROM numeric_tbl GROUP BY value1, floor(10 - value1) HAVING floor(avg(value1)) > 0;

-- select floor with group by having (result)
--Testcase 851:
SELECT value1, floor(10 - value1) FROM numeric_tbl GROUP BY value1, floor(10 - value1) HAVING floor(avg(value1)) > 0;

-- select floor with group by index having (result)
--Testcase 852:
SELECT value1, floor(10 - value1) FROM numeric_tbl GROUP BY 2,1 HAVING floor(10 - value1) > 0;

-- select floor with group by index having (result)
--Testcase 853:
SELECT value1, floor(10 - value1) FROM numeric_tbl GROUP BY 1,2 HAVING value1 > 1;

-- select floor and as
--Testcase 854:
SELECT floor(value3) as floor1 FROM numeric_tbl;

-- ===============================================================================
-- test cosh()
-- ===============================================================================
-- select cosh (builtin function, EXPLAIN (COSTS OFF))
--Testcase 855:
EXPLAIN (COSTS OFF)
SELECT value1, cosh(value2), cosh(value3), cosh(value4), cosh(0.5) FROM numeric_tbl;

-- select cosh (builtin function, result)
--Testcase 856:
SELECT value1, cosh(value2), cosh(value3), cosh(value4), cosh(0.5) FROM numeric_tbl;

-- select cosh (builtin function, not pushdown constraints, EXPLAIN (COSTS OFF))
--Testcase 857:
EXPLAIN (COSTS OFF)
SELECT cosh(value1), cosh(value2), cosh(value3), cosh(value4), cosh(0.5) FROM numeric_tbl WHERE to_hex(value2) = '64';

-- select cosh (builtin function, not pushdown constraints, result)
--Testcase 858:
SELECT cosh(value1), cosh(value2), cosh(value3), cosh(value4), cosh(0.5) FROM numeric_tbl WHERE to_hex(value2) = '64';

-- select cosh (builtin function, pushdown constraints, EXPLAIN (COSTS OFF))
--Testcase 859:
EXPLAIN (COSTS OFF)
SELECT cosh(value1), cosh(value2), cosh(value3), cosh(value4), cosh(0.5) FROM numeric_tbl WHERE value2 != 200;

-- select cosh (builtin function, pushdown constraints, result)
--Testcase 860:
SELECT cosh(value1), cosh(value2), cosh(value3), cosh(value4), cosh(0.5) FROM numeric_tbl WHERE value2 != 200;

-- select cosh (builtin function, cosh in constraints, EXPLAIN (COSTS OFF))
--Testcase 861:
EXPLAIN (COSTS OFF)
SELECT value1, cosh(value2), cosh(value3), cosh(value4), cosh(0.5) FROM numeric_tbl WHERE cosh(value1) != 1;

-- select cosh (builtin function, cosh in constraints, result)
--Testcase 862:
SELECT value1, cosh(value2), cosh(value3), cosh(value4), cosh(0.5) FROM numeric_tbl WHERE cosh(value1) != 1;

-- select cosh (builtin function, cosh in constraints, EXPLAIN (COSTS OFF))
--Testcase 863:
EXPLAIN (COSTS OFF)
SELECT cosh(value1), cosh(value2), cosh(value3), cosh(value4), cosh(0.5) FROM numeric_tbl WHERE cosh(0.5) > value1;

-- select cosh (builtin function, cosh in constraints, result)
--Testcase 864:
SELECT cosh(value1), cosh(value2), cosh(value3), cosh(value4), cosh(0.5) FROM numeric_tbl WHERE cosh(0.5) > value1;

-- select cosh as nest function with agg (pushdown, EXPLAIN (COSTS OFF))
--Testcase 865:
EXPLAIN (COSTS OFF)
SELECT sum(value3),cosh(sum(value3)) FROM numeric_tbl;

-- select cosh as nest function with agg (pushdown, result)
--Testcase 866:
SELECT sum(value3),cosh(sum(value3)) FROM numeric_tbl;

-- select cosh as nest with log2 (pushdown, EXPLAIN (COSTS OFF))
--Testcase 867:
EXPLAIN (COSTS OFF)
SELECT value1, cosh(log(2, value2)) FROM numeric_tbl;

-- select cosh as nest with log2 (pushdown, result)
--Testcase 868:
SELECT value1, cosh(log(2, value2)) FROM numeric_tbl;

-- select cosh with non pushdown func and explicit constant (EXPLAIN (COSTS OFF))
--Testcase 869:
EXPLAIN (COSTS OFF)
SELECT value1, cosh(value3), pi(), 4.1 FROM numeric_tbl;

-- select cosh with non pushdown func and explicit constant (result)
--Testcase 870:
SELECT value1, cosh(value3), pi(), 4.1 FROM numeric_tbl;

-- select cosh with order by (EXPLAIN (COSTS OFF))
--Testcase 871:
EXPLAIN (COSTS OFF)
SELECT value1, cosh(1-value3) FROM numeric_tbl ORDER BY cosh(1-value3);

-- select cosh with order by (result)
--Testcase 872:
SELECT value1, cosh(1-value3) FROM numeric_tbl ORDER BY cosh(1-value3);

-- select cosh with order by index (result)
--Testcase 873:
SELECT value1, cosh(1-value3) FROM numeric_tbl ORDER BY 2,1;

-- select cosh with order by index (result)
--Testcase 874:
SELECT value1, cosh(1-value3) FROM numeric_tbl ORDER BY 1,2;

-- select cosh with group by (EXPLAIN (COSTS OFF))
--Testcase 875:
EXPLAIN (COSTS OFF)
SELECT value1, cosh(1-value3) FROM numeric_tbl GROUP BY value1, cosh(1-value3);

-- select cosh with group by (result)
--Testcase 876:
SELECT value1, cosh(1-value3) FROM numeric_tbl GROUP BY value1, cosh(1-value3);

-- select cosh with group by index (result)
--Testcase 877:
SELECT value1, cosh(1-value3) FROM numeric_tbl GROUP BY 2,1;

-- select cosh with group by index (result)
--Testcase 878:
SELECT value1, cosh(1-value3) FROM numeric_tbl GROUP BY 1,2;

-- select cosh with group by having (EXPLAIN (COSTS OFF))
--Testcase 879:
EXPLAIN (COSTS OFF)
SELECT value1, cosh(1-value3) FROM numeric_tbl GROUP BY value1, cosh(1-value3) HAVING cosh(avg(value1)) > 0;

-- select cosh with group by having (result)
--Testcase 880:
SELECT value1, cosh(1-value3) FROM numeric_tbl GROUP BY value1, cosh(1-value3) HAVING cosh(avg(value1)) > 0;

-- select cosh with group by index having (result)
--Testcase 881:
SELECT value1, cosh(1-value3) FROM numeric_tbl GROUP BY 2,1 HAVING cosh(1-value3) > 0;

-- select cosh with group by index having (result)
--Testcase 882:
SELECT value1, cosh(1-value3) FROM numeric_tbl GROUP BY 1,2 HAVING value1 > 1;

-- select cosh and as
--Testcase 883:
SELECT value1, cosh(value3) as sin1 FROM numeric_tbl;

-- ===============================================================================
-- test sinh()
-- ===============================================================================
-- select sinh (builtin function, EXPLAIN (COSTS OFF))
--Testcase 884:
EXPLAIN (COSTS OFF)
SELECT value1, sinh(value2), sinh(value3), sinh(value4), sinh(0.5) FROM numeric_tbl;

-- select sinh (builtin function, result)
--Testcase 885:
SELECT value1, sinh(value2), sinh(value3), sinh(value4), sinh(0.5) FROM numeric_tbl;

-- select sinh (builtin function, not pushdown constraints, EXPLAIN (COSTS OFF))
--Testcase 886:
EXPLAIN (COSTS OFF)
SELECT sinh(value1), sinh(value2), sinh(value3), sinh(value4), sinh(0.5) FROM numeric_tbl WHERE to_hex(value2) = '64';

-- select sinh (builtin function, not pushdown constraints, result)
--Testcase 887:
SELECT sinh(value1), sinh(value2), sinh(value3), sinh(value4), sinh(0.5) FROM numeric_tbl WHERE to_hex(value2) = '64';

-- select sinh (builtin function, pushdown constraints, EXPLAIN (COSTS OFF))
--Testcase 888:
EXPLAIN (COSTS OFF)
SELECT sinh(value1), sinh(value2), sinh(value3), sinh(value4), sinh(0.5) FROM numeric_tbl WHERE value2 != 200;

-- select sinh (builtin function, pushdown constraints, result)
--Testcase 889:
SELECT sinh(value1), sinh(value2), sinh(value3), sinh(value4), sinh(0.5) FROM numeric_tbl WHERE value2 != 200;

-- select sinh (builtin function, sinh in constraints, EXPLAIN (COSTS OFF))
--Testcase 890:
EXPLAIN (COSTS OFF)
SELECT value1, sinh(value2), sinh(value3), sinh(value4), sinh(0.5) FROM numeric_tbl WHERE sinh(value1) != 1;

-- select sinh (builtin function, sinh in constraints, result)
--Testcase 891:
SELECT value1, sinh(value2), sinh(value3), sinh(value4), sinh(0.5) FROM numeric_tbl WHERE sinh(value1) != 1;

-- select sinh (builtin function, sinh in constraints, EXPLAIN (COSTS OFF))
--Testcase 892:
EXPLAIN (COSTS OFF)
SELECT sinh(value1), sinh(value2), sinh(value3), sinh(value4), sinh(0.5) FROM numeric_tbl WHERE sinh(0.5) > value1;

-- select sinh (builtin function, sinh in constraints, result)
--Testcase 893:
SELECT sinh(value1), sinh(value2), sinh(value3), sinh(value4), sinh(0.5) FROM numeric_tbl WHERE sinh(0.5) > value1;

-- select sinh as nest function with agg (pushdown, EXPLAIN (COSTS OFF))
--Testcase 894:
EXPLAIN (COSTS OFF)
SELECT sum(value3),sinh(sum(value3)) FROM numeric_tbl;

-- select sinh as nest function with agg (pushdown, result)
--Testcase 895:
SELECT sum(value3),sinh(sum(value3)) FROM numeric_tbl;

-- select sinh as nest with log2 (pushdown, EXPLAIN (COSTS OFF))
--Testcase 896:
EXPLAIN (COSTS OFF)
SELECT value1, sinh(log(2, value2)) FROM numeric_tbl;

-- select sinh as nest with log2 (pushdown, result)
--Testcase 897:
SELECT value1, sinh(log(2, value2)) FROM numeric_tbl;

-- select sinh with non pushdown func and explicit constant (EXPLAIN (COSTS OFF))
--Testcase 898:
EXPLAIN (COSTS OFF)
SELECT value1, sinh(value3), pi(), 4.1 FROM numeric_tbl;

-- select sinh with non pushdown func and explicit constant (result)
--Testcase 899:
SELECT value1, sinh(value3), pi(), 4.1 FROM numeric_tbl;

-- select sinh with order by (EXPLAIN (COSTS OFF))
--Testcase 900:
EXPLAIN (COSTS OFF)
SELECT value1, sinh(1-value3) FROM numeric_tbl ORDER BY sinh(1-value3);

-- select sinh with order by (result)
--Testcase 901:
SELECT value1, sinh(1-value3) FROM numeric_tbl ORDER BY sinh(1-value3);

-- select sinh with order by index (result)
--Testcase 902:
SELECT value1, sinh(1-value3) FROM numeric_tbl ORDER BY 2,1;

-- select sinh with order by index (result)
--Testcase 903:
SELECT value1, sinh(1-value3) FROM numeric_tbl ORDER BY 1,2;

-- select sinh with group by (EXPLAIN (COSTS OFF))
--Testcase 904:
EXPLAIN (COSTS OFF)
SELECT value1, sinh(1-value3) FROM numeric_tbl GROUP BY value1, sinh(1-value3);

-- select sinh with group by (result)
--Testcase 905:
SELECT value1, sinh(1-value3) FROM numeric_tbl GROUP BY value1, sinh(1-value3);

-- select sinh with group by index (result)
--Testcase 906:
SELECT value1, sinh(1-value3) FROM numeric_tbl GROUP BY 2,1;

-- select sinh with group by index (result)
--Testcase 907:
SELECT value1, sinh(1-value3) FROM numeric_tbl GROUP BY 1,2;

-- select sinh with group by having (EXPLAIN (COSTS OFF))
--Testcase 908:
EXPLAIN (COSTS OFF)
SELECT value1, sinh(1-value3) FROM numeric_tbl GROUP BY value1, sinh(1-value3) HAVING sinh(avg(value1)) > 0;

-- select sinh with group by having (result)
--Testcase 909:
SELECT value1, sinh(1-value3) FROM numeric_tbl GROUP BY value1, sinh(1-value3) HAVING sinh(avg(value1)) > 0;

-- select sinh with group by index having (result)
--Testcase 910:
SELECT value1, sinh(1-value3) FROM numeric_tbl GROUP BY 2,1 HAVING sinh(1-value3) > 0;

-- select sinh with group by index having (result)
--Testcase 911:
SELECT value1, sinh(1-value3) FROM numeric_tbl GROUP BY 1,2 HAVING value1 > 1;

-- select sinh and as
--Testcase 912:
SELECT value1, sinh(value3) as sin1 FROM numeric_tbl;

-- ===============================================================================
-- test tanh()
-- ===============================================================================
-- select tanh (builtin function, EXPLAIN (COSTS OFF))
--Testcase 913:
EXPLAIN (COSTS OFF)
SELECT value1, tanh(value2), tanh(value3), tanh(value4), tanh(0.5) FROM numeric_tbl;

-- select tanh (builtin function, result)
--Testcase 914:
SELECT value1, tanh(value2), tanh(value3), tanh(value4), tanh(0.5) FROM numeric_tbl;

-- select tanh (builtin function, not pushdown constraints, EXPLAIN (COSTS OFF))
--Testcase 915:
EXPLAIN (COSTS OFF)
SELECT tanh(value1), tanh(value2), tanh(value3), tanh(value4), tanh(0.5) FROM numeric_tbl WHERE to_hex(value2) = '64';

-- select tanh (builtin function, not pushdown constraints, result)
--Testcase 916:
SELECT tanh(value1), tanh(value2), tanh(value3), tanh(value4), tanh(0.5) FROM numeric_tbl WHERE to_hex(value2) = '64';

-- select tanh (builtin function, pushdown constraints, EXPLAIN (COSTS OFF))
--Testcase 917:
EXPLAIN (COSTS OFF)
SELECT tanh(value1), tanh(value2), tanh(value3), tanh(value4), tanh(0.5) FROM numeric_tbl WHERE value2 != 200;

-- select tanh (builtin function, pushdown constraints, result)
--Testcase 918:
SELECT tanh(value1), tanh(value2), tanh(value3), tanh(value4), tanh(0.5) FROM numeric_tbl WHERE value2 != 200;

-- select tanh (builtin function, tanh in constraints, EXPLAIN (COSTS OFF))
--Testcase 919:
EXPLAIN (COSTS OFF)
SELECT value1, tanh(value2), tanh(value3), tanh(value4), tanh(0.5) FROM numeric_tbl WHERE tanh(value1) != 1;

-- select tanh (builtin function, tanh in constraints, result)
--Testcase 920:
SELECT value1, tanh(value2), tanh(value3), tanh(value4), tanh(0.5) FROM numeric_tbl WHERE tanh(value1) != 1;

-- select tanh (builtin function, tanh in constraints, EXPLAIN (COSTS OFF))
--Testcase 921:
EXPLAIN (COSTS OFF)
SELECT tanh(value1), tanh(value2), tanh(value3), tanh(value4), tanh(0.5) FROM numeric_tbl WHERE tanh(0.5) > value1;

-- select tanh (builtin function, tanh in constraints, result)
--Testcase 922:
SELECT tanh(value1), tanh(value2), tanh(value3), tanh(value4), tanh(0.5) FROM numeric_tbl WHERE tanh(0.5) > value1;

-- select tanh as nest function with agg (pushdown, EXPLAIN (COSTS OFF))
--Testcase 923:
EXPLAIN (COSTS OFF)
SELECT sum(value3),tanh(sum(value3)) FROM numeric_tbl;

-- select tanh as nest function with agg (pushdown, result)
--Testcase 924:
SELECT sum(value3),tanh(sum(value3)) FROM numeric_tbl;

-- select tanh as nest with log2 (pushdown, EXPLAIN (COSTS OFF))
--Testcase 925:
EXPLAIN (COSTS OFF)
SELECT value1, tanh(log(2, value2)) FROM numeric_tbl;

-- select tanh as nest with log2 (pushdown, result)
--Testcase 926:
SELECT value1, tanh(log(2, value2)) FROM numeric_tbl;

-- select tanh with non pushdown func and explicit constant (EXPLAIN (COSTS OFF))
--Testcase 927:
EXPLAIN (COSTS OFF)
SELECT value1, tanh(value3), pi(), 4.1 FROM numeric_tbl;

-- select tanh with non pushdown func and explicit constant (result)
--Testcase 928:
SELECT value1, tanh(value3), pi(), 4.1 FROM numeric_tbl;

-- select tanh with order by (EXPLAIN (COSTS OFF))
--Testcase 929:
EXPLAIN (COSTS OFF)
SELECT value1, tanh(1-value3) FROM numeric_tbl ORDER BY tanh(1-value3);

-- select tanh with order by (result)
--Testcase 930:
SELECT value1, tanh(1-value3) FROM numeric_tbl ORDER BY tanh(1-value3);

-- select tanh with order by index (result)
--Testcase 931:
SELECT value1, tanh(1-value3) FROM numeric_tbl ORDER BY 2,1;

-- select tanh with order by index (result)
--Testcase 932:
SELECT value1, tanh(1-value3) FROM numeric_tbl ORDER BY 1,2;

-- select tanh with group by (EXPLAIN (COSTS OFF))
--Testcase 933:
EXPLAIN (COSTS OFF)
SELECT value1, tanh(1-value3) FROM numeric_tbl GROUP BY value1, tanh(1-value3);

-- select tanh with group by (result)
--Testcase 934:
SELECT value1, tanh(1-value3) FROM numeric_tbl GROUP BY value1, tanh(1-value3);

-- select tanh with group by index (result)
--Testcase 935:
SELECT value1, tanh(1-value3) FROM numeric_tbl GROUP BY 2,1;

-- select tanh with group by index (result)
--Testcase 936:
SELECT value1, tanh(1-value3) FROM numeric_tbl GROUP BY 1,2;

-- select tanh with group by having (EXPLAIN (COSTS OFF))
--Testcase 937:
EXPLAIN (COSTS OFF)
SELECT value1, tanh(1-value3) FROM numeric_tbl GROUP BY value1, tanh(1-value3) HAVING tanh(avg(value1)) > 0;

-- select tanh with group by having (result)
--Testcase 938:
SELECT value1, tanh(1-value3) FROM numeric_tbl GROUP BY value1, tanh(1-value3) HAVING tanh(avg(value1)) > 0;

-- select tanh with group by index having (result)
--Testcase 939:
SELECT value1, tanh(1-value3) FROM numeric_tbl GROUP BY 2,1 HAVING tanh(1-value3) > 0;

-- select tanh with group by index having (result)
--Testcase 940:
SELECT value1, tanh(1-value3) FROM numeric_tbl GROUP BY 1,2 HAVING value1 > 1;

-- select tanh and as
--Testcase 941:
SELECT value1, tanh(value3) as sin1 FROM numeric_tbl;

-- ===============================================================================
-- test width_bucket
-- ===============================================================================
-- select width_bucket (builtin function, EXPLAIN (COSTS OFF))
--Testcase 942:
EXPLAIN (COSTS OFF)
SELECT width_bucket(value5, value6, value7, value8), width_bucket(value5, value6, value7, value8), width_bucket(value5, value6, value7, value8), width_bucket(value5, value6, value7, 2), width_bucket(value5, value6, value7, value8) FROM numeric_tbl;

-- select width_bucket (builtin function, result)
--Testcase 943:
SELECT width_bucket(value5, value6, value7, value8), width_bucket(value5, value6, value7, value8), width_bucket(value5, value6, value7, value8), width_bucket(value5, value6, value7, 2), width_bucket(value5, value6, value7, value8) FROM numeric_tbl;

-- select width_bucket (builtin function, not pushdown constraints, EXPLAIN (COSTS OFF))
--Testcase 944:
EXPLAIN (COSTS OFF)
SELECT width_bucket(value5, value6, value7, value8), width_bucket(value5, value6, value7, value8), width_bucket(value5, value6, value7, value8), width_bucket(value5, value6, value7, 2), width_bucket(value5, value6, value7, value8) FROM numeric_tbl WHERE to_hex(value2) = '64';

-- select width_bucket (builtin function, not pushdown constraints, result)
--Testcase 945:
SELECT width_bucket(value5, value6, value7, value8), width_bucket(value5, value6, value7, value8), width_bucket(value5, value6, value7, value8), width_bucket(value5, value6, value7, 2), width_bucket(value5, value6, value7, value8) FROM numeric_tbl WHERE to_hex(value2) = '64';

-- select width_bucket (builtin function, pushdown constraints, EXPLAIN (COSTS OFF))
--Testcase 946:
EXPLAIN (COSTS OFF)
SELECT width_bucket(value5, value6, value7, value8), width_bucket(value5, value6, value7, value8), width_bucket(value5, value6, value7, value8), width_bucket(value5, value6, value7, 2), width_bucket(value5, value6, value7, value8) FROM numeric_tbl WHERE value2 != 200;

-- select width_bucket (builtin function, pushdown constraints, result)
--Testcase 947:
SELECT width_bucket(value5, value6, value7, value8), width_bucket(value5, value6, value7, value8), width_bucket(value5, value6, value7, value8), width_bucket(value5, value6, value7, 2), width_bucket(value5, value6, value7, value8) FROM numeric_tbl WHERE value2 != 200;

-- select width_bucket (builtin function, width_bucket in constraints, EXPLAIN (COSTS OFF))
--Testcase 948:
EXPLAIN (COSTS OFF)
SELECT width_bucket(value5, value6, value7, value8), width_bucket(value5, value6, value7, value8), width_bucket(value5, value6, value7, value8), width_bucket(value5, value6, value7, 2), width_bucket(value5, value6, value7, value8) FROM numeric_tbl WHERE width_bucket(value5, value6, value7, value8) != 1;

-- select width_bucket (builtin function, width_bucket in constraints, result)
--Testcase 949:
SELECT width_bucket(value5, value6, value7, value8), width_bucket(value5, value6, value7, value8), width_bucket(value5, value6, value7, value8), width_bucket(value5, value6, value7, 2), width_bucket(value5, value6, value7, value8) FROM numeric_tbl WHERE width_bucket(value5, value6, value7, value8) != 1;

-- select width_bucket (builtin function, width_bucket in constraints, EXPLAIN (COSTS OFF))
--Testcase 950:
EXPLAIN (COSTS OFF)
SELECT width_bucket(value5, value6, value7, value8), width_bucket(value5, value6, value7, value8), width_bucket(value5, value6, value7, value8), width_bucket(value5, value6, value7, 2), width_bucket(value5, value6, value7, value8) FROM numeric_tbl WHERE width_bucket(value5, value6, value7, value8) > value1;

-- select width_bucket (builtin function, width_bucket in constraints, result)
--Testcase 951:
SELECT width_bucket(value5, value6, value7, value8), width_bucket(value5, value6, value7, value8), width_bucket(value5, value6, value7, value8), width_bucket(value5, value6, value7, 2), width_bucket(value5, value6, value7, value8) FROM numeric_tbl WHERE width_bucket(value5, value6, value7, value8) > value1;

-- select width_bucket as nest function with agg (pushdown, EXPLAIN (COSTS OFF))
--Testcase 952:
EXPLAIN (COSTS OFF)
SELECT sum(value3),width_bucket(value5, value6, value7, value8) FROM numeric_tbl GROUP BY value5, value6, value7, value8;

-- select width_bucket as nest function with agg (pushdown, result)
--Testcase 953:
SELECT sum(value3),width_bucket(value5, value6, value7, value8) FROM numeric_tbl GROUP BY value5, value6, value7, value8;

-- select width_bucket as nest with log2 (pushdown, EXPLAIN (COSTS OFF))
--Testcase 954:
EXPLAIN (COSTS OFF)
SELECT width_bucket(value5, value6, value7, value8) FROM numeric_tbl;

-- select width_bucket as nest with log2 (pushdown, result)
--Testcase 955:
SELECT width_bucket(value5, value6, value7, value8) FROM numeric_tbl;

-- select width_bucket with non pushdown func and explicit constant (EXPLAIN (COSTS OFF))
--Testcase 956:
EXPLAIN (COSTS OFF)
SELECT width_bucket(value5, value6, value7, value8), pi(), 4.1 FROM numeric_tbl;

-- select width_bucket with non pushdown func and explicit constant (result)
--Testcase 957:
SELECT width_bucket(value5, value6, value7, value8), pi(), 4.1 FROM numeric_tbl;

-- select width_bucket with order by (EXPLAIN (COSTS OFF))
--Testcase 958:
EXPLAIN (COSTS OFF)
SELECT value1, width_bucket(value5, value6, value7, value8) FROM numeric_tbl ORDER BY width_bucket(value5, value6, value7, value8);

-- select width_bucket with order by (result)
--Testcase 959:
SELECT value1, width_bucket(value5, value6, value7, value8) FROM numeric_tbl ORDER BY width_bucket(value5, value6, value7, value8);

-- select width_bucket with order by index (result)
--Testcase 960:
SELECT value1, width_bucket(value5, value6, value7, value8) FROM numeric_tbl ORDER BY 2,1;

-- select width_bucket with order by index (result)
--Testcase 961:
SELECT value1, width_bucket(value5, value6, value7, value8) FROM numeric_tbl ORDER BY 1,2;

-- select width_bucket with group by (EXPLAIN (COSTS OFF))
--Testcase 962:
EXPLAIN (COSTS OFF)
SELECT value1, width_bucket(value5, value6, value7, value8) FROM numeric_tbl GROUP BY value1, width_bucket(value5, value6, value7, value8);

-- select width_bucket with group by (result)
--Testcase 963:
SELECT value1, width_bucket(value5, value6, value7, value8) FROM numeric_tbl GROUP BY value1, width_bucket(value5, value6, value7, value8);

-- select width_bucket with group by index (result)
--Testcase 964:
SELECT value1, width_bucket(value5, value6, value7, value8) FROM numeric_tbl GROUP BY 2,1;

-- select width_bucket with group by index (result)
--Testcase 965:
SELECT value1, width_bucket(value5, value6, value7, value8) FROM numeric_tbl GROUP BY 1,2;

-- select width_bucket with group by having (EXPLAIN (COSTS OFF))
--Testcase 966:
EXPLAIN (COSTS OFF)
SELECT value1, width_bucket(value5, value6, value7, value8) FROM numeric_tbl GROUP BY value1, width_bucket(value5, value6, value7, value8) HAVING width_bucket(value5, value6, value7, value8) > 0;

-- select width_bucket with group by having (result)
--Testcase 967:
SELECT value1, width_bucket(value5, value6, value7, value8) FROM numeric_tbl GROUP BY value1, width_bucket(value5, value6, value7, value8) HAVING width_bucket(value5, value6, value7, value8) > 0;

-- select width_bucket with group by index having (result)
--Testcase 968:
SELECT value1, width_bucket(value5, value6, value7, value8) FROM numeric_tbl GROUP BY 2,1 HAVING width_bucket(value5, value6, value7, value8) > 0;

-- select width_bucket with group by index having (result)
--Testcase 969:
SELECT value1, width_bucket(value5, value6, value7, value8) FROM numeric_tbl GROUP BY 1,2 HAVING value1 > 1;

-- select width_bucket and as
--Testcase 970:
SELECT width_bucket(value5, value6, value7, value8) as floor1 FROM numeric_tbl;

-- ===============================================================================
-- test initcap
-- ===============================================================================
-- select initcap (stub function, EXPLAIN (COSTS OFF))
--Testcase 971:
EXPLAIN (COSTS OFF)
SELECT initcap(str3), initcap(str2) FROM numeric_tbl;
-- select initcap (stub function, result)
--Testcase 972:
SELECT initcap(str3), initcap(str2) FROM numeric_tbl;

-- select initcap (stub function, not pushdown constraints, EXPLAIN (COSTS OFF))
--Testcase 973:
EXPLAIN (COSTS OFF)
SELECT value1, initcap(str3) FROM numeric_tbl WHERE to_hex(value2) = '64';
-- select initcap (stub function, not pushdown constraints, result)
--Testcase 974:
SELECT value1, initcap(str3) FROM numeric_tbl WHERE to_hex(value2) = '64';

-- select initcap (stub function, pushdown constraints, EXPLAIN (COSTS OFF))
--Testcase 975:
EXPLAIN (COSTS OFF)
SELECT value1, initcap(str3) FROM numeric_tbl WHERE value2 != 200;
-- select initcap (stub function, pushdown constraints, result)
--Testcase 976:
SELECT value1, initcap(str3) FROM numeric_tbl WHERE value2 != 200;

-- select initcap with non pushdown func and explicit constant (EXPLAIN (COSTS OFF))
--Testcase 977:
EXPLAIN (COSTS OFF)
SELECT initcap(str3), pi(), 4.1 FROM numeric_tbl;
-- select initcap with non pushdown func and explicit constant (result)
--Testcase 978:
SELECT initcap(str3), pi(), 4.1 FROM numeric_tbl;

-- select initcap with order by (EXPLAIN (COSTS OFF))
--Testcase 979:
EXPLAIN (COSTS OFF)
SELECT value1, initcap(str3) FROM numeric_tbl ORDER BY initcap(str3);
-- select initcap with order by (result)
--Testcase 980:
SELECT value1, initcap(str3) FROM numeric_tbl ORDER BY initcap(str3);

-- select initcap with order by index (result)
--Testcase 981:
SELECT value1, initcap(str3) FROM numeric_tbl ORDER BY 2,1;
-- select initcap with order by index (result)
--Testcase 982:
SELECT value1, initcap(str3) FROM numeric_tbl ORDER BY 1,2;

-- select initcap with group by (EXPLAIN (COSTS OFF))
--Testcase 983:
EXPLAIN (COSTS OFF)
SELECT count(value1), initcap(str3) FROM numeric_tbl GROUP BY initcap(str3);
-- select initcap with group by (result)
--Testcase 984:
SELECT count(value1), initcap(str3) FROM numeric_tbl GROUP BY initcap(str3);

-- select initcap with group by index (result)
--Testcase 985:
SELECT value1, initcap(str3) FROM numeric_tbl GROUP BY 2,1;

-- select initcap with group by having (EXPLAIN (COSTS OFF))
--Testcase 986:
EXPLAIN (COSTS OFF)
SELECT count(value1), initcap(str3) FROM numeric_tbl GROUP BY initcap(str3), str1 HAVING initcap(str3) IS NOT NULL;
-- select initcap with group by having (result)
--Testcase 987:
SELECT count(value1), initcap(str3) FROM numeric_tbl GROUP BY initcap(str3), str1 HAVING initcap(str3) IS NOT NULL;

-- select initcap with group by index having (result)
--Testcase 988:
SELECT value1, initcap(str3) FROM numeric_tbl GROUP BY 1,2 HAVING value1 > 1;

-- ===============================================================================
-- test to_timestamp()
-- ===============================================================================
-- select to_timestamp (stub function, EXPLAIN (COSTS OFF))
--Testcase 989:
EXPLAIN (COSTS OFF)
SELECT to_timestamp(str4, 'YYYY-MM-DD HH:MI:SS'), to_timestamp(str4, 'YYYY-MM-DD HH:MI:SS'), to_timestamp(str4, 'YYYY-MM-DD HH:MI:SS') FROM numeric_tbl;
-- select to_timestamp (stub function, result)
--Testcase 990:
SELECT to_timestamp(str4, 'YYYY-MM-DD HH:MI:SS'), to_timestamp(str4, 'YYYY-MM-DD HH:MI:SS'), to_timestamp(str4, 'YYYY-MM-DD HH:MI:SS') FROM numeric_tbl;

-- select to_timestamp (stub function, not pushdown constraints, EXPLAIN (COSTS OFF))
--Testcase 991:
EXPLAIN (COSTS OFF)
SELECT id, to_timestamp(str4, 'YYYY-MM-DD HH:MI:SS') FROM numeric_tbl WHERE to_hex(value2) = '64';
-- select to_timestamp (stub function, not pushdown constraints, EXPLAIN (COSTS OFF))
--Testcase 992:
SELECT id, to_timestamp(str4, 'YYYY-MM-DD HH:MI:SS') FROM numeric_tbl WHERE to_hex(value2) = '64';

-- select to_timestamp (stub function, to_timestamp in constraints, EXPLAIN (COSTS OFF))
--Testcase 993:
EXPLAIN (COSTS OFF)
SELECT id, to_timestamp(str4, 'YYYY-MM-DD HH:MI:SS') FROM numeric_tbl WHERE to_timestamp(str4, 'YYYY-MM-DD HH:MI:SS') > to_timestamp('2016-03-31 9:30:20', 'YYYY-MM-DD HH:MI:SS');
-- select to_timestamp (stub function, to_timestamp in constraints, result)
--Testcase 994:
SELECT id, to_timestamp(str4, 'YYYY-MM-DD HH:MI:SS') FROM numeric_tbl WHERE to_timestamp(str4, 'YYYY-MM-DD HH:MI:SS') > to_timestamp('2016-03-31 9:30:20', 'YYYY-MM-DD HH:MI:SS');

-- select to_timestamp with non pushdown func and explicit constant (EXPLAIN (COSTS OFF))
--Testcase 995:
EXPLAIN (COSTS OFF)
SELECT to_timestamp(str4, 'YYYY-MM-DD HH:MI:SS'), pi(), 4.1 FROM numeric_tbl;
-- select to_timestamp with non pushdown func and explicit constant (result)
--Testcase 996:
SELECT to_timestamp(str4, 'YYYY-MM-DD HH:MI:SS'), pi(), 4.1 FROM numeric_tbl;

-- select to_timestamp with order by (EXPLAIN (COSTS OFF))
--Testcase 997:
EXPLAIN (COSTS OFF)
SELECT value1, to_timestamp(str4, 'YYYY-MM-DD HH:MI:SS') FROM numeric_tbl ORDER BY to_timestamp(str4, 'YYYY-MM-DD HH:MI:SS'), 1 DESC;
-- select to_timestamp with order by (result)
--Testcase 998:
SELECT value1, to_timestamp(str4, 'YYYY-MM-DD HH:MI:SS') FROM numeric_tbl ORDER BY to_timestamp(str4, 'YYYY-MM-DD HH:MI:SS'), 1 DESC;

-- select to_timestamp with group by (EXPLAIN (COSTS OFF))
--Testcase 999:
EXPLAIN (COSTS OFF)
SELECT count(value1), to_timestamp(str4, 'YYYY-MM-DD HH:MI:SS') FROM numeric_tbl GROUP BY to_timestamp(str4, 'YYYY-MM-DD HH:MI:SS');
-- select to_timestamp with group by (result)
--Testcase 1000:
SELECT count(value1), to_timestamp(str4, 'YYYY-MM-DD HH:MI:SS') FROM numeric_tbl GROUP BY to_timestamp(str4, 'YYYY-MM-DD HH:MI:SS');

-- select to_timestamp with group by index (result)
--Testcase 1001:
SELECT value1, to_timestamp(str4, 'YYYY-MM-DD HH:MI:SS') FROM numeric_tbl GROUP BY 2,1;

-- select to_timestamp with group by having (EXPLAIN (COSTS OFF))
--Testcase 1002:
EXPLAIN (COSTS OFF)
SELECT count(value1), to_timestamp(str4, 'YYYY-MM-DD HH:MI:SS') FROM numeric_tbl GROUP BY to_timestamp(str4, 'YYYY-MM-DD HH:MI:SS'), str1 HAVING to_timestamp(str4, 'YYYY-MM-DD HH:MI:SS') > to_timestamp('2016-03-31 9:30:20', 'YYYY-MM-DD HH:MI:SS');
-- select to_timestamp with group by having (result)
--Testcase 1003:
SELECT count(value1), to_timestamp(str4, 'YYYY-MM-DD HH:MI:SS') FROM numeric_tbl GROUP BY to_timestamp(str4, 'YYYY-MM-DD HH:MI:SS'), str1 HAVING to_timestamp(str4, 'YYYY-MM-DD HH:MI:SS') > to_timestamp('2016-03-31 9:30:20', 'YYYY-MM-DD HH:MI:SS');

-- select to_timestamp with group by index having (result)
--Testcase 1004:
SELECT value1, to_timestamp(str4, 'YYYY-MM-DD HH:MI:SS') FROM numeric_tbl GROUP BY 1,2 HAVING value1 > 1;


-- ===============================================================================
-- test trunc()
-- ===============================================================================
-- select trunc (builtin function, EXPLAIN (COSTS OFF))
--Testcase 1005:
EXPLAIN (COSTS OFF)
SELECT trunc(value6), trunc(value7), trunc(value5), trunc(value1) FROM numeric_tbl;
-- select trunc (buitin function, result)
--Testcase 1006:
SELECT trunc(value6), trunc(value7), trunc(value5), trunc(value1) FROM numeric_tbl;
-- select trunc (builtin function, not pushdown constraints, EXPLAIN (COSTS OFF))
--Testcase 1007:
EXPLAIN (COSTS OFF)
SELECT trunc(value6), trunc(value7), trunc(value5), trunc(value1) FROM numeric_tbl WHERE to_hex(value2) != '64';
-- select trunc (builtin function, not pushdown constraints, result)
--Testcase 1008:
SELECT trunc(value6), trunc(value7), trunc(value5), trunc(value1) FROM numeric_tbl WHERE to_hex(value2) != '64';
-- select trunc (builtin function, pushdown constraints, EXPLAIN (COSTS OFF))
--Testcase 1009:
EXPLAIN (COSTS OFF)
SELECT trunc(value6), trunc(value7), trunc(value5), trunc(value1) FROM numeric_tbl WHERE value2 != 200;
-- select trunc (builtin function, pushdown constraints, result)
--Testcase 1010:
SELECT trunc(value6), trunc(value7), trunc(value5), trunc(value1) FROM numeric_tbl WHERE value2 != 200;

-- ===============================================================================
-- test translate()
-- ===============================================================================
-- select translate (stub function, EXPLAIN (COSTS OFF))
--Testcase 1011:
EXPLAIN (COSTS OFF)
SELECT translate(str3, 'abc', '123'),translate(str1, 'abc', '123') FROM numeric_tbl;
-- select translate (stub function, result)
--Testcase 1012:
SELECT translate(str3, 'abc', '123'),translate(str1, 'abc', '123') FROM numeric_tbl;

-- select translate (stub function, not pushdown constraints, EXPLAIN (COSTS OFF))
--Testcase 1013:
EXPLAIN (COSTS OFF)
SELECT value1, translate(str3, 'abc', '123') FROM numeric_tbl WHERE to_hex(value2) = '64';
-- select translate (stub function, not pushdown constraints, result)
--Testcase 1014:
SELECT value1, translate(str3, 'abc', '123') FROM numeric_tbl WHERE to_hex(value2) = '64';

-- select translate (stub function, pushdown constraints, EXPLAIN (COSTS OFF))
--Testcase 1015:
EXPLAIN (COSTS OFF)
SELECT value1, translate(str3, 'abc', '123') FROM numeric_tbl WHERE value2 != 200;
-- select translate (stub function, pushdown constraints, result)
--Testcase 1016:
SELECT value1, translate(str3, 'abc', '123') FROM numeric_tbl WHERE value2 != 200;

-- select translate with non pushdown func and explicit constant (EXPLAIN (COSTS OFF))
--Testcase 1017:
EXPLAIN (COSTS OFF)
SELECT translate(str3, 'abc', '123'), pi(), 4.1 FROM numeric_tbl;
-- select translate with non pushdown func and explicit constant (result)
--Testcase 1018:
SELECT translate(str3, 'abc', '123'), pi(), 4.1 FROM numeric_tbl;

-- select translate with order by (EXPLAIN (COSTS OFF))
--Testcase 1019:
EXPLAIN (COSTS OFF)
SELECT value1, translate(str3, 'abc', '123') FROM numeric_tbl ORDER BY translate(str3, 'abc', '123');
-- select translate with order by (result)
--Testcase 1020:
SELECT value1, translate(str3, 'abc', '123') FROM numeric_tbl ORDER BY translate(str3, 'abc', '123');

-- select translate with order by index (result)
--Testcase 1021:
SELECT value1, translate(str3, 'abc', '123') FROM numeric_tbl ORDER BY 2,1;
-- select translate with order by index (result)
--Testcase 1022:
SELECT value1, translate(str3, 'abc', '123') FROM numeric_tbl ORDER BY 1,2;

-- select translate with group by (EXPLAIN (COSTS OFF))
--Testcase 1023:
EXPLAIN (COSTS OFF)
SELECT count(value1), translate(str3, 'abc', '123') FROM numeric_tbl GROUP BY translate(str3, 'abc', '123');
-- select translate with group by (result)
--Testcase 1024:
SELECT count(value1), translate(str3, 'abc', '123') FROM numeric_tbl GROUP BY translate(str3, 'abc', '123');

-- select translate with group by index (result)
--Testcase 1025:
SELECT value1, translate(str3, 'abc', '123') FROM numeric_tbl GROUP BY 2,1;

-- select translate with group by having (EXPLAIN (COSTS OFF))
--Testcase 1026:
EXPLAIN (COSTS OFF)
SELECT count(value1), translate(str3, 'abc', '123') FROM numeric_tbl GROUP BY translate(str3, 'abc', '123'), str2 HAVING translate(str3, 'abc', '123') IS NOT NULL;
-- select translate with group by having (result)
--Testcase 1027:
SELECT count(value1), translate(str3, 'abc', '123') FROM numeric_tbl GROUP BY translate(str3, 'abc', '123'), str2 HAVING translate(str3, 'abc', '123') IS NOT NULL;

-- select translate with group by index having (result)
--Testcase 1028:
SELECT value1, translate(str3, 'abc', '123') FROM numeric_tbl GROUP BY 1,2 HAVING value1 > 1;



-- ===============================================================================
-- test to_char()
-- ===============================================================================
-- select to_char (stub function, EXPLAIN (COSTS OFF))
--Testcase 1029:
EXPLAIN (COSTS OFF)
SELECT to_char(value2, '999'), to_char(value2, '999'), to_char(value2, '999') FROM numeric_tbl;
-- select to_char (stub function, result)
--Testcase 1030:
SELECT to_char(value2, '999'), to_char(value2, '999'), to_char(value2, '999') FROM numeric_tbl;

-- select to_char (stub function, not pushdown constraints, EXPLAIN (COSTS OFF))
--Testcase 1031:
EXPLAIN (COSTS OFF)
SELECT id, to_char(value2, '999') FROM numeric_tbl WHERE to_hex(value2) = '64';
-- select to_char (stub function, not pushdown constraints, EXPLAIN (COSTS OFF))
--Testcase 1032:
SELECT id, to_char(value2, '999') FROM numeric_tbl WHERE to_hex(value2) = '64';

-- select to_char (stub function, to_char in constraints, EXPLAIN (COSTS OFF))
--Testcase 1033:
EXPLAIN (COSTS OFF)
SELECT id, to_char(value2, '999') FROM numeric_tbl WHERE to_char(value2, '999') > to_char(value4, '999');
-- select to_char (stub function, to_char in constraints, result)
--Testcase 1034:
SELECT id, to_char(value2, '999') FROM numeric_tbl WHERE to_char(value2, '999') > to_char(value4, '999');

-- select to_char with non pushdown func and explicit constant (EXPLAIN (COSTS OFF))
--Testcase 1035:
EXPLAIN (COSTS OFF)
SELECT to_char(value2, '999'), pi(), 4.1 FROM numeric_tbl;
-- select to_char with non pushdown func and explicit constant (result)
--Testcase 1036:
SELECT to_char(value2, '999'), pi(), 4.1 FROM numeric_tbl;

-- select to_char with order by (EXPLAIN (COSTS OFF))
--Testcase 1037:
EXPLAIN (COSTS OFF)
SELECT value1, to_char(value2, '999') FROM numeric_tbl ORDER BY to_char(value2, '999'), 1 DESC;
-- select to_char with order by (result)
--Testcase 1038:
SELECT value1, to_char(value2, '999') FROM numeric_tbl ORDER BY to_char(value2, '999'), 1 DESC;

-- select to_char with group by (EXPLAIN (COSTS OFF))
--Testcase 1039:
EXPLAIN (COSTS OFF)
SELECT count(value1), to_char(value2, '999') FROM numeric_tbl GROUP BY to_char(value2, '999');
-- select to_char with group by (result)
--Testcase 1040:
SELECT count(value1), to_char(value2, '999') FROM numeric_tbl GROUP BY to_char(value2, '999');

-- select to_char with group by index (result)
--Testcase 1041:
SELECT value1, to_char(value2, '999') FROM numeric_tbl GROUP BY 2,1;

-- select to_char with group by having (EXPLAIN (COSTS OFF))
--Testcase 1042:
EXPLAIN (COSTS OFF)
SELECT count(value1), to_char(value2, '999') FROM numeric_tbl GROUP BY value4, to_char(value2, '999'), str1 HAVING to_char(value2, '999') > to_char(value4, '999');
-- select to_char with group by having (result)
--Testcase 1043:
SELECT count(value1), to_char(value2, '999') FROM numeric_tbl GROUP BY value4, to_char(value2, '999'), str1 HAVING to_char(value2, '999') > to_char(value4, '999');

-- select to_char with group by index having (result)
--Testcase 1044:
SELECT value1, to_char(value2, '999') FROM numeric_tbl GROUP BY 1,2 HAVING value1 > 1;


-- ===============================================================================
-- test to_date()
-- ===============================================================================
-- select to_date (stub function, EXPLAIN (COSTS OFF))
--Testcase 1045:
EXPLAIN (COSTS OFF)
SELECT to_date(str4, 'YYYY-MM-DD HH:MI:SS'), to_date(str4, 'YYYY-MM-DD HH:MI:SS'), to_date(str4, 'YYYY-MM-DD HH:MI:SS') FROM numeric_tbl;
-- select to_date (stub function, result)
--Testcase 1046:
SELECT to_date(str4, 'YYYY-MM-DD HH:MI:SS'), to_date(str4, 'YYYY-MM-DD HH:MI:SS'), to_date(str4, 'YYYY-MM-DD HH:MI:SS') FROM numeric_tbl;

-- select to_date (stub function, not pushdown constraints, EXPLAIN (COSTS OFF))
--Testcase 1047:
EXPLAIN (COSTS OFF)
SELECT id, to_date(str4, 'YYYY-MM-DD HH:MI:SS') FROM numeric_tbl WHERE to_hex(value2) = '64';
-- select to_date (stub function, not pushdown constraints, EXPLAIN (COSTS OFF))
--Testcase 1048:
SELECT id, to_date(str4, 'YYYY-MM-DD HH:MI:SS') FROM numeric_tbl WHERE to_hex(value2) = '64';

-- select to_date (stub function, to_date in constraints, EXPLAIN (COSTS OFF))
--Testcase 1049:
EXPLAIN (COSTS OFF)
SELECT id, to_date(str4, 'YYYY-MM-DD HH:MI:SS') FROM numeric_tbl WHERE to_date(str4, 'YYYY-MM-DD HH:MI:SS') > to_date('2016-03-31 9:30:20', 'YYYY-MM-DD HH:MI:SS');
-- select to_date (stub function, to_date in constraints, result)
--Testcase 1050:
SELECT id, to_date(str4, 'YYYY-MM-DD HH:MI:SS') FROM numeric_tbl WHERE to_date(str4, 'YYYY-MM-DD HH:MI:SS') > to_date('2016-03-31 9:30:20', 'YYYY-MM-DD HH:MI:SS');

-- select to_date with non pushdown func and explicit constant (EXPLAIN (COSTS OFF))
--Testcase 1051:
EXPLAIN (COSTS OFF)
SELECT to_date(str4, 'YYYY-MM-DD HH:MI:SS'), pi(), 4.1 FROM numeric_tbl;
-- select to_date with non pushdown func and explicit constant (result)
--Testcase 1052:
SELECT to_date(str4, 'YYYY-MM-DD HH:MI:SS'), pi(), 4.1 FROM numeric_tbl;

-- select to_date with order by (EXPLAIN (COSTS OFF))
--Testcase 1053:
EXPLAIN (COSTS OFF)
SELECT value1, to_date(str4, 'YYYY-MM-DD HH:MI:SS') FROM numeric_tbl ORDER BY to_date(str4, 'YYYY-MM-DD HH:MI:SS'), 1 DESC;
-- select to_date with order by (result)
--Testcase 1054:
SELECT value1, to_date(str4, 'YYYY-MM-DD HH:MI:SS') FROM numeric_tbl ORDER BY to_date(str4, 'YYYY-MM-DD HH:MI:SS'), 1 DESC;

-- select to_date with group by (EXPLAIN (COSTS OFF))
--Testcase 1055:
EXPLAIN (COSTS OFF)
SELECT count(value1), to_date(str4, 'YYYY-MM-DD HH:MI:SS') FROM numeric_tbl GROUP BY to_date(str4, 'YYYY-MM-DD HH:MI:SS');
-- select to_date with group by (result)
--Testcase 1056:
SELECT count(value1), to_date(str4, 'YYYY-MM-DD HH:MI:SS') FROM numeric_tbl GROUP BY to_date(str4, 'YYYY-MM-DD HH:MI:SS');

-- select to_date with group by index (result)
--Testcase 1057:
SELECT value1, to_date(str4, 'YYYY-MM-DD HH:MI:SS') FROM numeric_tbl GROUP BY 2,1;

-- select to_date with group by having (EXPLAIN (COSTS OFF))
--Testcase 1058:
EXPLAIN (COSTS OFF)
SELECT count(value1), to_date(str4, 'YYYY-MM-DD HH:MI:SS') FROM numeric_tbl GROUP BY to_date(str4, 'YYYY-MM-DD HH:MI:SS'), str1 HAVING to_date(str4, 'YYYY-MM-DD HH:MI:SS') > to_date('2016-03-31 9:30:20', 'YYYY-MM-DD HH:MI:SS');
-- select to_date with group by having (result)
--Testcase 1059:
SELECT count(value1), to_date(str4, 'YYYY-MM-DD HH:MI:SS') FROM numeric_tbl GROUP BY to_date(str4, 'YYYY-MM-DD HH:MI:SS'), str1 HAVING to_date(str4, 'YYYY-MM-DD HH:MI:SS') > to_date('2016-03-31 9:30:20', 'YYYY-MM-DD HH:MI:SS');

-- select to_date with group by index having (result)
--Testcase 1060:
SELECT value1, to_date(str4, 'YYYY-MM-DD HH:MI:SS') FROM numeric_tbl GROUP BY 1,2 HAVING value1 > 1;

-- ===============================================================================
-- test to_number()
-- ===============================================================================
-- select to_number (stub function, EXPLAIN (COSTS OFF))
--Testcase 1061:
EXPLAIN (COSTS OFF)
SELECT to_number(str5, '99G999D9S'), to_number(str5, '99G999D9S'), to_number(str5, '99G999D9S') FROM numeric_tbl;
-- select to_number (stub function, result)
--Testcase 1062:
SELECT to_number(str5, '99G999D9S'), to_number(str5, '99G999D9S'), to_number(str5, '99G999D9S') FROM numeric_tbl;

-- select to_number (stub function, not pushdown constraints, EXPLAIN (COSTS OFF))
--Testcase 1063:
EXPLAIN (COSTS OFF)
SELECT id, to_number(str5, '99G999D9S') FROM numeric_tbl WHERE to_hex(value2) = '64';
-- select to_number (stub function, not pushdown constraints, EXPLAIN (COSTS OFF))
--Testcase 1064:
SELECT id, to_number(str5, '99G999D9S') FROM numeric_tbl WHERE to_hex(value2) = '64';

-- select to_number (stub function, to_number in constraints, EXPLAIN (COSTS OFF))
--Testcase 1065:
EXPLAIN (COSTS OFF)
SELECT id, to_number(str5, '99G999D9S') FROM numeric_tbl WHERE to_number(str5, '99G999D9S') < 0;
-- select to_number (stub function, to_number in constraints, result)
--Testcase 1066:
SELECT id, to_number(str5, '99G999D9S') FROM numeric_tbl WHERE to_number(str5, '99G999D9S') < 0;

-- select to_number with non pushdown func and explicit constant (EXPLAIN (COSTS OFF))
--Testcase 1067:
EXPLAIN (COSTS OFF)
SELECT to_number(str5, '99G999D9S'), pi(), 4.1 FROM numeric_tbl;
-- select to_number with non pushdown func and explicit constant (result)
--Testcase 1068:
SELECT to_number(str5, '99G999D9S'), pi(), 4.1 FROM numeric_tbl;

-- select to_number with order by (EXPLAIN (COSTS OFF))
--Testcase 1069:
EXPLAIN (COSTS OFF)
SELECT value1, to_number(str5, '99G999D9S') FROM numeric_tbl ORDER BY to_number(str5, '99G999D9S'), 1 DESC;
-- select to_number with order by (result)
--Testcase 1070:
SELECT value1, to_number(str5, '99G999D9S') FROM numeric_tbl ORDER BY to_number(str5, '99G999D9S'), 1 DESC;

-- select to_number with group by (EXPLAIN (COSTS OFF))
--Testcase 1071:
EXPLAIN (COSTS OFF)
SELECT count(value1), to_number(str5, '99G999D9S') FROM numeric_tbl GROUP BY to_number(str5, '99G999D9S');
-- select to_number with group by (result)
--Testcase 1072:
SELECT count(value1), to_number(str5, '99G999D9S') FROM numeric_tbl GROUP BY to_number(str5, '99G999D9S');

-- select to_number with group by index (result)
--Testcase 1073:
SELECT value1, to_number(str5, '99G999D9S') FROM numeric_tbl GROUP BY 2,1;

-- select to_number with group by having (EXPLAIN (COSTS OFF))
--Testcase 1074:
EXPLAIN (COSTS OFF)
SELECT count(value1), to_number(str5, '99G999D9S') FROM numeric_tbl GROUP BY to_number(str5, '99G999D9S'), str1 HAVING to_number(str5, '99G999D9S') < 0;
-- select to_number with group by having (result)
--Testcase 1075:
SELECT count(value1), to_number(str5, '99G999D9S') FROM numeric_tbl GROUP BY to_number(str5, '99G999D9S'), str1 HAVING to_number(str5, '99G999D9S') < 0;

-- select to_number with group by index having (explain)
--Testcase 1076:
EXPLAIN (COSTS OFF)
SELECT value1, to_number(str5, '99G999D9S') FROM numeric_tbl GROUP BY 1,2 HAVING value1 > 1;;
-- select to_number with group by index having (result)
--Testcase 1077:
SELECT value1, to_number(str5, '99G999D9S') FROM numeric_tbl GROUP BY 1,2 HAVING value1 > 1;;

-- End test for Numeric


--
-- test for date/time function
--

-- ADD_MONTHS()
-- select add_months (stub function, explain)
--Testcase 1078:
EXPLAIN (COSTS OFF)
SELECT add_months(c2, 10), add_months(c2, '10') FROM time_tbl;

-- select add_months (stub function, result)
--Testcase 1079:
SELECT add_months(c2, 10), add_months(c2, '10') FROM time_tbl;

-- select add_months (stub function, explain)
--Testcase 1080:
EXPLAIN (COSTS OFF)
SELECT add_months(c2, -10), add_months('01-Aug-03', '10') FROM time_tbl;

-- select add_months (stub function, result)
--Testcase 1081:
SELECT add_months(c2, -10), add_months('01-Aug-03', '10') FROM time_tbl;

-- CURRENT_DATE()
-- select oracle_current_date (stub function, explain)
--Testcase 1082:
EXPLAIN (COSTS OFF)
SELECT oracle_current_date() FROM time_tbl;

-- select oracle_current_date with months_between to make stable result (stub function, result)
--Testcase 1083:
SELECT months_between(oracle_current_date(), oracle_current_date()) FROM time_tbl GROUP BY 1;

-- select oracle_current_date (stub function, not pushdown constraints, explain)
--Testcase 1084:
EXPLAIN (COSTS OFF)
SELECT oracle_current_date() FROM time_tbl WHERE to_hex(id) > '0';

-- select oracle_current_date with months_between to make stable result (stub function, result)
--Testcase 1085:
SELECT months_between(oracle_current_date(), oracle_current_date()) FROM time_tbl WHERE to_hex(id) > '0' GROUP BY 1;

-- select oracle_current_date (stub function, pushdown constraints, explain)
--Testcase 1086:
EXPLAIN (COSTS OFF)
SELECT oracle_current_date() FROM time_tbl WHERE id = 1;

-- select oracle_current_date with months_between to make stable result (stub function, result)
--Testcase 1087:
SELECT months_between(oracle_current_date(), oracle_current_date()) FROM time_tbl WHERE id = 1 GROUP BY 1;

-- select oracle_current_date (stub function, oracle_current_date in constraints, explain)
--Testcase 1088:
EXPLAIN (COSTS OFF)
SELECT oracle_current_date() FROM time_tbl WHERE oracle_current_date() > '2000-01-01';

-- select oracle_current_date with months_between to make stable result (stub function, result)
--Testcase 1089:
SELECT months_between(oracle_current_date(), oracle_current_date()) FROM time_tbl WHERE oracle_current_date() > '2000-01-01' GROUP BY 1;

-- oracle_current_date in constrains (stub function, explain)
--Testcase 1090:
EXPLAIN (COSTS OFF)
SELECT c1 FROM time_tbl WHERE oracle_current_date() > '2000-01-01';

-- oracle_current_date in constrains (stub function, result)
--Testcase 1091:
SELECT c1 FROM time_tbl WHERE oracle_current_date() > '2000-01-01';

-- oracle_current_date as parameter of add_moths(stub function, explain)
--Testcase 1092:
EXPLAIN (COSTS OFF)
SELECT c1 FROM time_tbl WHERE add_months(oracle_current_date(), 31) > '2000-01-01';

-- oracle_current_date as parameter of add_months(stub function, result)
--Testcase 1093:
SELECT c1 FROM time_tbl WHERE add_months(oracle_current_date(), 31) > '2000-01-01';

-- select oracle_current_date and agg (pushdown, explain)
--Testcase 1094:
EXPLAIN (COSTS OFF)
SELECT oracle_current_date(), sum(id) FROM time_tbl;

-- select oracle_current_date with months_between to make stable result (stub function, result)
--Testcase 1095:
SELECT months_between(oracle_current_date(), oracle_current_date()), sum(id) FROM time_tbl GROUP BY 1;

-- select oracle_current_date with order by (explain)
--Testcase 1096:
EXPLAIN (COSTS OFF)
SELECT oracle_current_date(), c1 FROM time_tbl ORDER BY c1;

-- select oracle_current_date with months_between to make stable result (stub function, result)
--Testcase 1097:
SELECT months_between(oracle_current_date(), oracle_current_date()), c1 FROM time_tbl ORDER BY c1;

-- select oracle_current_date with order by index (explain)
--Testcase 1098:
EXPLAIN (COSTS OFF)
SELECT oracle_current_date(), c1 FROM time_tbl ORDER BY 2;

-- select oracle_current_date with months_between to make stable result (stub function, result)
--Testcase 1099:
SELECT months_between(oracle_current_date(), oracle_current_date()), c1 FROM time_tbl ORDER BY 2;

-- oracle_current_date constraints with order by (explain)
--Testcase 1100:
EXPLAIN (COSTS OFF)
SELECT c1 FROM time_tbl WHERE add_months(oracle_current_date(), 31) > '2000-01-01' ORDER BY c1;

-- oracle_current_date constraints with order by (result)
--Testcase 1101:
SELECT c1 FROM time_tbl WHERE add_months(oracle_current_date(), 31) > '2000-01-01' ORDER BY c1;

-- select oracle_current_date with group by (explain)
--Testcase 1102:
EXPLAIN (COSTS OFF)
SELECT oracle_current_date(), c1 FROM time_tbl GROUP BY c1;

-- select oracle_current_date with months_between to make stable result (stub function, result)
--Testcase 1103:
SELECT months_between(oracle_current_date(), oracle_current_date()), c1 FROM time_tbl GROUP BY c1;

-- select oracle_current_date with group by index (explain)
--Testcase 1104:
EXPLAIN (COSTS OFF)
SELECT oracle_current_date(), c1 FROM time_tbl GROUP BY 2;

-- select oracle_current_date with months_between to make stable result (stub function, result)
--Testcase 1105:
SELECT months_between(oracle_current_date(), oracle_current_date()), c1 FROM time_tbl GROUP BY 2;

-- select oracle_current_date with group by having (explain)
--Testcase 1106:
EXPLAIN (COSTS OFF)
SELECT oracle_current_date(), c1 FROM time_tbl GROUP BY oracle_current_date(), c1 HAVING oracle_current_date() > '2000-01-01';

-- select oracle_current_date with months_between to make stable result (stub function, result)
--Testcase 1107:
SELECT months_between(oracle_current_date(), oracle_current_date()), c1 FROM time_tbl GROUP BY oracle_current_date(), c1 HAVING oracle_current_date() > '2000-01-01';

-- select oracle_current_date with group by index having (explain)
--Testcase 1108:
EXPLAIN (COSTS OFF)
SELECT oracle_current_date(), c1 FROM time_tbl GROUP BY 2,1 HAVING oracle_current_date() > '2000-01-01';

-- select oracle_current_date with months_between to make stable result (stub function, result)
--Testcase 1109:
SELECT months_between(oracle_current_date(), oracle_current_date()), c1 FROM time_tbl GROUP BY 2,1 HAVING oracle_current_date() > '2000-01-01';

-- oracle_current_date constraints with group by (explain)
--Testcase 1110:
EXPLAIN (COSTS OFF)
SELECT c1 FROM time_tbl WHERE add_months(oracle_current_date(), 31) > '2000-01-01' GROUP BY c1;

-- oracle_current_date constraints with group by (result)
--Testcase 1111:
SELECT c1 FROM time_tbl WHERE add_months(oracle_current_date(), 31) > '2000-01-01' GROUP BY c1;

-- select oracle_current_date with alias (explain)
--Testcase 1112:
EXPLAIN (COSTS OFF)
SELECT oracle_current_date() as oracle_current_date1 FROM time_tbl;

-- CURRENT_TIMESTAMP
-- oracle_current_timestamp constraints (explain)
--Testcase 1113:
EXPLAIN (COSTS OFF)
SELECT oracle_current_timestamp() FROM time_tbl;

-- oracle_current_timestamp constraints (result)
--Testcase 1114:
SELECT oracle_current_timestamp() - oracle_current_timestamp() FROM time_tbl;

-- select oracle_current_timestamp (stub function, not pushdown constraints, explain)
--Testcase 1115:
EXPLAIN (COSTS OFF)
SELECT oracle_current_timestamp() - oracle_current_timestamp() FROM time_tbl WHERE to_hex(id) > '0';

-- select oracle_current_timestamp (stub function, not pushdown constraints, result)
--Testcase 1116:
SELECT oracle_current_timestamp() - oracle_current_timestamp() FROM time_tbl WHERE to_hex(id) > '0';

-- select oracle_current_timestamp (stub function, pushdown constraints, explain)
--Testcase 1117:
EXPLAIN (COSTS OFF)
SELECT oracle_current_timestamp() FROM time_tbl WHERE id = 1;

-- select oracle_current_timestamp (stub function, pushdown constraints, result)
--Testcase 1118:
SELECT oracle_current_timestamp() - oracle_current_timestamp() FROM time_tbl WHERE id = 1;

-- select oracle_current_timestamp (stub function, oracle_current_timestamp in constraints, explain)
--Testcase 1119:
EXPLAIN (COSTS OFF)
SELECT oracle_current_timestamp() FROM time_tbl WHERE oracle_current_timestamp() > '2000-01-01 00:00:00'::timestamp;

-- select oracle_current_timestamp (stub function, oracle_current_timestamp in constraints, result)
--Testcase 1120:
SELECT oracle_current_timestamp() - oracle_current_timestamp() FROM time_tbl WHERE oracle_current_timestamp() > '2000-01-01 00:00:00'::timestamp;

-- oracle_current_timestamp in constrains (stub function, explain)
--Testcase 1121:
EXPLAIN (COSTS OFF)
SELECT c1 FROM time_tbl WHERE oracle_current_timestamp() > '2000-01-01 00:00:00'::timestamp;

-- oracle_current_timestamp in constrains (stub function, result)
--Testcase 1122:
SELECT c1 FROM time_tbl WHERE oracle_current_timestamp() > '2000-01-01 00:00:00'::timestamp;

-- select oracle_current_timestamp and agg (pushdown, explain)
--Testcase 1123:
EXPLAIN (COSTS OFF)
SELECT oracle_current_timestamp(), sum(id) FROM time_tbl;

-- select oracle_current_timestamp and agg (pushdown, result)
--Testcase 1124:
SELECT oracle_current_timestamp() - oracle_current_timestamp(), sum(id) FROM time_tbl;

-- select oracle_current_timestamp with order by (explain)
--Testcase 1125:
EXPLAIN (COSTS OFF)
SELECT oracle_current_timestamp(), c1 FROM time_tbl ORDER BY oracle_current_timestamp();

-- select oracle_current_timestamp with order by (result)
--Testcase 1126:
SELECT oracle_current_timestamp() - oracle_current_timestamp(), c1 FROM time_tbl ORDER BY oracle_current_timestamp();

-- select oracle_current_timestamp with order by index (explain)
--Testcase 1127:
EXPLAIN (COSTS OFF)
SELECT oracle_current_timestamp(), c1 FROM time_tbl ORDER BY 1;

-- select oracle_current_timestamp with order by index (result)
--Testcase 1128:
SELECT oracle_current_timestamp() - oracle_current_timestamp(), c1 FROM time_tbl ORDER BY 1;

-- select oracle_current_timestamp with group by (explain)
--Testcase 1129:
EXPLAIN (COSTS OFF)
SELECT oracle_current_timestamp(), c1 FROM time_tbl GROUP BY c1;

-- select oracle_current_timestamp with group by (result)
--Testcase 1130:
SELECT oracle_current_timestamp() - oracle_current_timestamp(), c1 FROM time_tbl GROUP BY c1;

-- select oracle_current_timestamp with group by index (explain)
--Testcase 1131:
EXPLAIN (COSTS OFF)
SELECT oracle_current_timestamp(), c1 FROM time_tbl GROUP BY 2;

-- select oracle_current_timestamp with group by index (result)
--Testcase 1132:
SELECT oracle_current_timestamp() - oracle_current_timestamp(), c1 FROM time_tbl GROUP BY 2;

-- select oracle_current_timestamp with group by having (explain)
--Testcase 1133:
EXPLAIN (COSTS OFF)
SELECT oracle_current_timestamp(), c1 FROM time_tbl GROUP BY oracle_current_timestamp(),c1 HAVING oracle_current_timestamp() > '2000-01-01 00:00:00'::timestamp;

-- select oracle_current_timestamp with group by having (result)
--Testcase 1134:
SELECT oracle_current_timestamp() - oracle_current_timestamp(), c1 FROM time_tbl GROUP BY oracle_current_timestamp(),c1 HAVING oracle_current_timestamp() > '2000-01-01 00:00:00'::timestamp;

-- select oracle_current_timestamp with group by index having (explain)
--Testcase 1135:
EXPLAIN (COSTS OFF)
SELECT oracle_current_timestamp(), c1 FROM time_tbl GROUP BY 2,1 HAVING oracle_current_timestamp() > '2000-01-01 00:00:00'::timestamp;

-- select oracle_current_timestamp with group by index having (result)
--Testcase 1136:
SELECT oracle_current_timestamp() - oracle_current_timestamp(), c1 FROM time_tbl GROUP BY 2,1 HAVING oracle_current_timestamp() > '2000-01-01 00:00:00'::timestamp;

-- select oracle_current_timestamp with alias (explain)
--Testcase 1137:
EXPLAIN (COSTS OFF)
SELECT oracle_current_timestamp() as oracle_current_timestamp1 FROM time_tbl;

-- select oracle_current_timestamp with alias (result)
--Testcase 1138:
SELECT (oracle_current_timestamp() - oracle_current_timestamp()) as oracle_current_timestamp_diff FROM time_tbl;

-- LOCALTIMESTAMP, LOCALTIMESTAMP()
-- select oracle_localtimestamp (stub function, explain)
--Testcase 1139:
EXPLAIN (COSTS OFF)
SELECT oracle_localtimestamp() FROM time_tbl;

-- select oracle_localtimestamp (stub function, result)
-- result is different from expected one
--Testcase 1140:
SELECT oracle_localtimestamp() - oracle_localtimestamp() FROM time_tbl;

-- select oracle_localtimestamp (stub function, not pushdown constraints, explain)
--Testcase 1141:
EXPLAIN (COSTS OFF)
SELECT oracle_localtimestamp() FROM time_tbl WHERE to_hex(id) > '0';

-- select oracle_localtimestamp (stub function, not pushdown constraints, result)
--Testcase 1142:
SELECT oracle_localtimestamp() - oracle_localtimestamp() FROM time_tbl WHERE to_hex(id) > '0';

-- select oracle_localtimestamp (stub function, pushdown constraints, explain)
--Testcase 1143:
EXPLAIN (COSTS OFF)
SELECT oracle_localtimestamp() FROM time_tbl WHERE id = 1;

-- select oracle_localtimestamp (stub function, pushdown constraints, result)
--Testcase 1144:
SELECT oracle_localtimestamp() - oracle_localtimestamp() FROM time_tbl WHERE id = 1;

-- select oracle_localtimestamp (stub function, oracle_localtimestamp in constraints, explain)
--Testcase 1145:
EXPLAIN (COSTS OFF)
SELECT oracle_localtimestamp() FROM time_tbl WHERE oracle_localtimestamp() > '2000-01-01 00:00:00'::timestamp;

-- select oracle_localtimestamp (stub function, oracle_localtimestamp in constraints, result)
--Testcase 1146:
SELECT oracle_localtimestamp() - oracle_localtimestamp() FROM time_tbl WHERE oracle_localtimestamp() > '2000-01-01 00:00:00'::timestamp;

-- oracle_localtimestamp in constrains (stub function, explain)
--Testcase 1147:
EXPLAIN (COSTS OFF)
SELECT c1 FROM time_tbl WHERE oracle_localtimestamp() > '2000-01-01 00:00:00'::timestamp;

-- oracle_localtimestamp in constrains (stub function, result)
--Testcase 1148:
SELECT c1 FROM time_tbl WHERE oracle_localtimestamp() > '2000-01-01 00:00:00'::timestamp;

-- select oracle_localtimestamp and agg (pushdown, explain)
--Testcase 1149:
EXPLAIN (COSTS OFF)
SELECT oracle_localtimestamp(), sum(id) FROM time_tbl;

-- select oracle_localtimestamp and agg (pushdown, result)
-- result is different from expected one
--Testcase 1150:
SELECT oracle_localtimestamp() - oracle_localtimestamp(), sum(id) FROM time_tbl;

-- select oracle_localtimestamp with order by (explain)
--Testcase 1151:
EXPLAIN (COSTS OFF)
SELECT oracle_localtimestamp(), c1 FROM time_tbl ORDER BY oracle_localtimestamp();

-- select oracle_localtimestamp with order by (result)
--Testcase 1152:
SELECT oracle_localtimestamp() - oracle_localtimestamp(), c1 FROM time_tbl ORDER BY oracle_localtimestamp();

-- select oracle_localtimestamp with order by index (explain)
--Testcase 1153:
EXPLAIN (COSTS OFF)
SELECT oracle_localtimestamp(), c1 FROM time_tbl ORDER BY 1;

-- select oracle_localtimestamp with order by index (result)
--Testcase 1154:
SELECT oracle_localtimestamp() - oracle_localtimestamp(), c1 FROM time_tbl ORDER BY 1;

-- select oracle_localtimestamp with group by (explain)
--Testcase 1155:
EXPLAIN (COSTS OFF)
SELECT oracle_localtimestamp(), c1 FROM time_tbl GROUP BY c1;

-- select oracle_localtimestamp with group by (result)
--Testcase 1156:
SELECT oracle_localtimestamp() - oracle_localtimestamp(), c1 FROM time_tbl GROUP BY c1;

-- select oracle_localtimestamp with group by index (explain)
--Testcase 1157:
EXPLAIN (COSTS OFF)
SELECT oracle_localtimestamp(), c1 FROM time_tbl GROUP BY 2;

-- select oracle_localtimestamp with group by index (result)
--Testcase 1158:
SELECT oracle_localtimestamp() - oracle_localtimestamp(), c1 FROM time_tbl GROUP BY 2;

-- select oracle_localtimestamp with group by having (explain)
--Testcase 1159:
EXPLAIN (COSTS OFF)
SELECT oracle_localtimestamp(), c1 FROM time_tbl GROUP BY oracle_localtimestamp(),c1 HAVING oracle_localtimestamp() > '2000-01-01 00:00:00'::timestamp;

-- select oracle_localtimestamp with group by having (result)
--Testcase 1160:
SELECT oracle_localtimestamp() - oracle_localtimestamp(), c1 FROM time_tbl GROUP BY oracle_localtimestamp(),c1 HAVING oracle_localtimestamp() > '2000-01-01 00:00:00'::timestamp;

-- select oracle_localtimestamp with group by index having (explain)
--Testcase 1161:
EXPLAIN (COSTS OFF)
SELECT oracle_localtimestamp(), c1 FROM time_tbl GROUP BY 2,1 HAVING oracle_localtimestamp() > '2000-01-01 00:00:00'::timestamp;

-- select oracle_localtimestamp with group by index having (result)
--Testcase 1162:
SELECT oracle_localtimestamp() - oracle_localtimestamp(), c1 FROM time_tbl GROUP BY 2,1 HAVING oracle_localtimestamp() > '2000-01-01 00:00:00'::timestamp;

-- select oracle_localtimestamp with alias (explain)
--Testcase 1163:
EXPLAIN (COSTS OFF)
SELECT oracle_localtimestamp() as oracle_localtimestamp1 FROM time_tbl;

-- select oracle_localtimestamp with alias (result)
--Testcase 1164:
SELECT (oracle_localtimestamp() - oracle_localtimestamp()) as oracle_localtimestamp_diff FROM time_tbl;

-- LAST_DAY()
-- select last_day (stub function, explain)
--Testcase 1165:
EXPLAIN (COSTS OFF)
SELECT last_day(c2) FROM time_tbl;

-- select last_day (stub function, result)
--Testcase 1166:
SELECT last_day(c2) FROM time_tbl;

-- select last_day (stub function, not pushdown constraints, explain)
--Testcase 1167:
EXPLAIN (COSTS OFF)
SELECT last_day(c2) FROM time_tbl WHERE to_hex(id) = '1';

-- select last_day (stub function, not pushdown constraints, result)
--Testcase 1168:
SELECT last_day(c2) FROM time_tbl WHERE to_hex(id) = '1';

-- select last_day (stub function, pushdown constraints, explain)
--Testcase 1169:
EXPLAIN (COSTS OFF)
SELECT last_day(c2) FROM time_tbl WHERE id != 200;

-- select last_day (stub function, pushdown constraints, result)
--Testcase 1170:
SELECT last_day(c2) FROM time_tbl WHERE id != 200;

-- select last_day with agg (pushdown, explain)
--Testcase 1171:
EXPLAIN (COSTS OFF)
SELECT max(c2), last_day(max(c2)) FROM time_tbl;

-- select last_day as nest function with agg (pushdown, result)
--Testcase 1172:
SELECT max(c2), last_day(max(c2)) FROM time_tbl;

-- select last_day with order by (explain)
--Testcase 1173:
EXPLAIN (COSTS OFF)
SELECT id, last_day(c2) FROM time_tbl order by last_day(c2);

-- select last_day with order by (result)
--Testcase 1174:
SELECT id, last_day(c2) FROM time_tbl order by last_day(c2);

-- select last_day with order by index (explain)
--Testcase 1175:
EXPLAIN (COSTS OFF)
SELECT id, last_day(c2) FROM time_tbl order by 2,1;

-- select last_day with order by index (result)
--Testcase 1176:
SELECT id, last_day(c2) FROM time_tbl order by 2,1;

-- select last_day with order by index (explain)
--Testcase 1177:
EXPLAIN (COSTS OFF)
SELECT id, last_day(c2) FROM time_tbl order by 1,2;

-- select last_day with order by index (result)
--Testcase 1178:
SELECT id, last_day(c2) FROM time_tbl order by 1,2;

-- select last_day with group by (explain)
--Testcase 1179:
EXPLAIN (COSTS OFF)
SELECT max(c2), last_day(c2) FROM time_tbl group by 2;

-- select last_day with group by (result)
--Testcase 1180:
SELECT max(c2), last_day(c2) FROM time_tbl group by 2;

-- select last_day with group by index (explain)
--Testcase 1181:
EXPLAIN (COSTS OFF)
SELECT id, last_day(c2) FROM time_tbl group by 2, 1;

-- select last_day with group by index (result)
--Testcase 1182:
SELECT id, last_day(c2) FROM time_tbl group by 2, 1;

-- select last_day with group by index (explain)
--Testcase 1183:
EXPLAIN (COSTS OFF)
SELECT id, last_day(c2) FROM time_tbl group by 1, 2;

-- select last_day with group by index (result)
--Testcase 1184:
SELECT id, last_day(c2) FROM time_tbl group by 1, 2;

-- select last_day with group by having (explain)
--Testcase 1185:
EXPLAIN (COSTS OFF)
SELECT max(c2), last_day(c2) FROM time_tbl group by last_day(c2), c2 HAVING last_day(c2) > '2001-01-31'::date;

-- select last_day with group by having (result)
--Testcase 1186:
SELECT max(c2), last_day(c2) FROM time_tbl group by last_day(c2), c2 HAVING last_day(c2) > '2001-01-31'::date;

-- select last_day with group by index having (explain)
--Testcase 1187:
EXPLAIN (COSTS OFF)
SELECT id, last_day(c2), c2 FROM time_tbl group by id, last_day(c2), c2 HAVING last_day(c2) > '2001-01-31'::date;

-- select last_day with group by index having (result)
--Testcase 1188:
SELECT id, last_day(c2), c2 FROM time_tbl group by id, last_day(c2), c2 HAVING last_day(c2) > '2001-01-31'::date;


-- EXTRACT()
-- select oracle_extract (stub function, explain)
--Testcase 1189:
EXPLAIN (COSTS OFF)
SELECT oracle_extract('year', c1), oracle_extract('year', c2), oracle_extract('year', c3) FROM time_tbl;

-- select oracle_extract (stub function, result)
--Testcase 1190:
SELECT oracle_extract('year', c1), oracle_extract('year', c2), oracle_extract('year', c3) FROM time_tbl;

-- select oracle_extract (stub function, not pushdown constraints, explain)
--Testcase 1191:
EXPLAIN (COSTS OFF)
SELECT oracle_extract('year', c1), oracle_extract('year', c2), oracle_extract('year', c3) FROM time_tbl WHERE to_hex(id) = '1';

-- select oracle_extract (stub function, not pushdown constraints, result)
--Testcase 1192:
SELECT oracle_extract('year', c1), oracle_extract('year', c2), oracle_extract('year', c3) FROM time_tbl WHERE to_hex(id) = '1';

-- select oracle_extract (stub function, pushdown constraints, explain)
--Testcase 1193:
EXPLAIN (COSTS OFF)
SELECT oracle_extract('year', c1), oracle_extract('year', c2), oracle_extract('year', c3) FROM time_tbl WHERE id != 200;

-- select oracle_extract (stub function, pushdown constraints, result)
--Testcase 1194:
SELECT oracle_extract('year', c1), oracle_extract('year', c2), oracle_extract('year', c3) FROM time_tbl WHERE id != 200;

-- select oracle_extract (stub function, oracle_extract in constraints, explain)
--Testcase 1195:
EXPLAIN (COSTS OFF)
SELECT oracle_extract('year', c1), oracle_extract('year', c2), oracle_extract('year', c3) FROM time_tbl WHERE oracle_extract('year', c3 ) != oracle_extract('year', '2000-01-01'::timestamp);

-- select oracle_extract (stub function, oracle_extract in constraints, result)
--Testcase 1196:
SELECT oracle_extract('year', c1), oracle_extract('year', c2), oracle_extract('year', c3) FROM time_tbl WHERE oracle_extract('year', c3 ) != oracle_extract('year', '2000-01-01'::timestamp);

-- select oracle_extract (stub function, oracle_extract in constraints, explain)
--Testcase 1197:
EXPLAIN (COSTS OFF)
SELECT oracle_extract('year', c2), oracle_extract('second', '2021-01-03 12:10:30.123456'::timestamp), oracle_extract('day', c3) FROM time_tbl WHERE oracle_extract('year', c3 ) > '1';

-- select oracle_extract (stub function, oracle_extract in constraints, result)
--Testcase 1198:
SELECT oracle_extract('year', c2), oracle_extract('second', '2021-01-03 12:10:30.123456'::timestamp), oracle_extract('day', c3) FROM time_tbl WHERE oracle_extract('year', c3 ) > '1';

-- select oracle_extract with agg (pushdown, explain)
--Testcase 1199:
EXPLAIN (COSTS OFF)
SELECT max(c3), oracle_extract('year', max(c3)) FROM time_tbl;

-- select oracle_extract as nest function with agg (pushdown, result)
--Testcase 1200:
SELECT max(c3), oracle_extract('year', max(c3)) FROM time_tbl;

-- select oracle_extract with order by (explain)
--Testcase 1201:
EXPLAIN (COSTS OFF)
SELECT id, oracle_extract('year', c2), oracle_extract('second', '2021-01-03 12:10:30.123456'::timestamp), oracle_extract('day', c3) FROM time_tbl order by oracle_extract('year', c2), oracle_extract('second', '2021-01-03 12:10:30.123456'::timestamp), oracle_extract('day', c3);

-- select oracle_extract with order by (result)
--Testcase 1202:
SELECT id, oracle_extract('year', c2), oracle_extract('second', '2021-01-03 12:10:30.123456'::timestamp), oracle_extract('day', c3) FROM time_tbl order by oracle_extract('year', c2), oracle_extract('second', '2021-01-03 12:10:30.123456'::timestamp), oracle_extract('day', c3);

-- select oracle_extract with order by index (explain)
--Testcase 1203:
EXPLAIN (COSTS OFF)
SELECT id, oracle_extract('year', c2), oracle_extract('second', '2021-01-03 12:10:30.123456'::timestamp), oracle_extract('minute', c3) FROM time_tbl order by 4,3,2,1;

-- select oracle_extract with order by index (result)
--Testcase 1204:
SELECT id, oracle_extract('year', c2), oracle_extract('second', '2021-01-03 12:10:30.123456'::timestamp), oracle_extract('minute', c3) FROM time_tbl order by 4,3,2,1;

-- select oracle_extract with group by (explain)
--Testcase 1205:
EXPLAIN (COSTS OFF)
SELECT max(c3), oracle_extract('year', c2), oracle_extract('second', '2021-01-03 12:10:30.123456'::timestamp), oracle_extract('minute', c3) FROM time_tbl group by oracle_extract('minute', c3),c2;

-- select oracle_extract with group by (result)
--Testcase 1206:
SELECT max(c3), oracle_extract('year', c2), oracle_extract('second', '2021-01-03 12:10:30.123456'::timestamp), oracle_extract('minute', c3) FROM time_tbl group by oracle_extract('minute', c3),c2;

-- select oracle_extract with group by index (explain)
--Testcase 1207:
EXPLAIN (COSTS OFF)
SELECT id, oracle_extract('year', c2), oracle_extract('second', '2021-01-03 12:10:30.123456'::timestamp), oracle_extract('minute', c3) FROM time_tbl group by 4,3,2,1;

-- select oracle_extract with group by index (result)
--Testcase 1208:
SELECT id, oracle_extract('year', c2), oracle_extract('second', '2021-01-03 12:10:30.123456'::timestamp), oracle_extract('minute', c3) FROM time_tbl group by 4,3,2,1;

-- select oracle_extract with group by index (explain)
--Testcase 1209:
EXPLAIN (COSTS OFF)
SELECT id, oracle_extract('year', c2), oracle_extract('second', '2021-01-03 12:10:30.123456'::timestamp), oracle_extract('minute', c3) FROM time_tbl group by 1,2,3,4;

-- select oracle_extract with group by index (result)
--Testcase 1210:
SELECT id, oracle_extract('year', c2), oracle_extract('second', '2021-01-03 12:10:30.123456'::timestamp), oracle_extract('minute', c3) FROM time_tbl group by 1,2,3,4;

-- select oracle_extract with group by index having (explain)
--Testcase 1211:
EXPLAIN (COSTS OFF)
SELECT id, oracle_extract('year', c2), oracle_extract('second', '2021-01-03 12:10:30.123456'::timestamp), oracle_extract('minute', c3), c2 FROM time_tbl group by 5, 4, 3, 2, 1 HAVING oracle_extract('year', c2) > 2000;

-- select oracle_extract with group by index having (result)
--Testcase 1212:
SELECT id, oracle_extract('year', c2), oracle_extract('second', '2021-01-03 12:10:30.123456'::timestamp), oracle_extract('minute', c3), c2 FROM time_tbl group by 5, 4, 3, 2, 1 HAVING oracle_extract('year', c2) > 2000;

-- select oracle_extract with group by index having (explain)
--Testcase 1213:
EXPLAIN (COSTS OFF)
SELECT id, oracle_extract('year', c2), oracle_extract('second', '2021-01-03 12:10:30.123456'::timestamp), oracle_extract('minute', c3), c2 FROM time_tbl group by 1, 2, 3, 4, 5 HAVING oracle_extract('year', c2) > 2000;

-- select oracle_extract with group by index having (result)
--Testcase 1214:
SELECT id, oracle_extract('year', c2), oracle_extract('second', '2021-01-03 12:10:30.123456'::timestamp), oracle_extract('minute', c3), c2 FROM time_tbl group by 1, 2, 3, 4, 5 HAVING oracle_extract('year', c2) > 2000;

-- select oracle_extract and as
--Testcase 1215:
SELECT oracle_extract('year', c2) as oracle_extract1, oracle_extract('second', '2021-01-03 12:10:30.123456'::timestamp) as oracle_extract2, oracle_extract('minute', c3) as oracle_extract3 FROM time_tbl;

-- select oracle_extract with date type (explain)
--Testcase 1216:
EXPLAIN (COSTS OFF)
SELECT oracle_extract('year', c2), oracle_extract('month', c2), oracle_extract('day', c2) FROM time_tbl; 

-- select oracle_extract with date type (result)
--Testcase 1217:
SELECT oracle_extract('year', c2), oracle_extract('month', c2), oracle_extract('day', c2) FROM time_tbl; 

-- select oracle_extract with timestamp with time zone type (explain)
--Testcase 1218:
EXPLAIN (COSTS OFF)
SELECT oracle_extract('year', c1), oracle_extract('month', c1), oracle_extract('day', c1), oracle_extract('hour', c1), oracle_extract('minute', c1), oracle_extract('second', c1),  oracle_extract('timezone_hour', c1),  oracle_extract('timezone_minute', c1) FROM time_tbl;

-- select oracle_extract with timestamp with time zone type (result)
--Testcase 1219:
SELECT oracle_extract('year', c1), oracle_extract('month', c1), oracle_extract('day', c1), oracle_extract('hour', c1), oracle_extract('minute', c1), oracle_extract('second', c1),  oracle_extract('timezone_hour', c1),  oracle_extract('timezone_minute', c1) FROM time_tbl;

-- select oracle_extract with timestamp without time zone type (explain)
--Testcase 1220:
EXPLAIN (COSTS OFF)
SELECT oracle_extract('year', c1), oracle_extract('month', c1), oracle_extract('day', c1), oracle_extract('hour', c1), oracle_extract('minute', c1), oracle_extract('second', c1) FROM time_tbl;

-- select oracle_extract with timestamp without time zone type (result)
--Testcase 1221:
SELECT oracle_extract('year', c1), oracle_extract('month', c1), oracle_extract('day', c1), oracle_extract('hour', c1), oracle_extract('minute', c1), oracle_extract('second', c1) FROM time_tbl;

-- select oracle_extract with interval day to second type (explain)
--Testcase 1222:
EXPLAIN (COSTS OFF)
SELECT oracle_extract('day', interval '5 04:30:20.11' day to second), oracle_extract('hour', interval '5 04:30:20.11' day to second), oracle_extract('minute', interval '5 04:30:20.11' day to second), oracle_extract('second', interval '5 04:30:20.11' day to second) FROM time_tbl;

-- select oracle_extract with interval day to second type (result)
--Testcase 1223:
SELECT oracle_extract('day', interval '5 04:30:20.11' day to second), oracle_extract('hour', interval '5 04:30:20.11' day to second), oracle_extract('minute', interval '5 04:30:20.11' day to second), oracle_extract('second', interval '5 04:30:20.11' day to second) FROM time_tbl;

-- select oracle_extract with interval day to second type (explain)
--Testcase 1224:
EXPLAIN (COSTS OFF)
SELECT oracle_extract('day', interval '40 days 2 hours 1 minute 1 second'), oracle_extract('hour', interval '40 days 2 hours 1 minute 1 second'), oracle_extract('minute', interval '40 days 2 hours 1 minute 1 second'), oracle_extract('second', interval '40 days 2 hours 1 minute 1 second') FROM time_tbl;

-- select oracle_extract with interval day to second type (result)
--Testcase 1225:
SELECT oracle_extract('day', interval '40 days 2 hours 1 minute 1 second'), oracle_extract('hour', interval '40 days 2 hours 1 minute 1 second'), oracle_extract('minute', interval '40 days 2 hours 1 minute 1 second'), oracle_extract('second', interval '40 days 2 hours 1 minute 1 second') FROM time_tbl;


-- DBTIMEZONE()
-- select dbtimezone (explain)
--Testcase 1226:
EXPLAIN (COSTS OFF)
SELECT dbtimezone() FROM time_tbl LIMIT 1;

-- select dbtimezone (result)
--Testcase 1227:
SELECT dbtimezone() FROM time_tbl LIMIT 1;

-- FROM_TZ(timestamp, timezone)
-- select from_tz (explain)
--Testcase 1228:
EXPLAIN (COSTS OFF)
SELECT c3, from_tz(c3, '3:00') FROM time_tbl;

-- select from_tz (result)
--Testcase 1229:
SELECT c3, from_tz(c3, '3:00') FROM time_tbl;


-- MONTHS_BETWEEN(date, date)
-- select months_between, negative result (explain)
--Testcase 1230:
EXPLAIN (COSTS OFF)
SELECT c2, months_between(c2, '2025-01-01') FROM time_tbl;

-- select months_between, negative result (result)
--Testcase 1231:
SELECT c2, months_between(c2, '2025-01-01') FROM time_tbl;

-- select months_between, positive result (explain)
--Testcase 1232:
EXPLAIN (COSTS OFF)
SELECT c2, months_between('2025-01-01', c2) FROM time_tbl;

-- select months_between, positive result (result)
--Testcase 1233:
SELECT c2, months_between('2025-01-01', c2) FROM time_tbl;


-- NEW_TIME(date, timezone1, timezone2)
-- set 24 hour format
--Testcase 1234:
SELECT oracle_execute('oracle_srv', 'ALTER SESSION SET NLS_DATE_FORMAT = ''DD-MON-YYYY HH24:MI:SS''');

-- select new_time, ast, bst (explain)
--Testcase 1235:
EXPLAIN (COSTS OFF)
SELECT c2, new_time(c2, 'AST', 'BST') FROM time_tbl;

-- select new_time, ast, bst (result)
--Testcase 1236:
SELECT c2, new_time(c2, 'AST', 'BST') FROM time_tbl;

-- select new_time, ast, cst (explain)
--Testcase 1237:
EXPLAIN (COSTS OFF)
SELECT c2, new_time(c2, 'AST', 'CST') FROM time_tbl;

-- select new_time, ast, cst (result)
--Testcase 1238:
SELECT c2, new_time(c2, 'AST', 'CST') FROM time_tbl;

-- select new_time, ast, est (explain)
--Testcase 1239:
EXPLAIN (COSTS OFF)
SELECT c2, new_time(c2, 'AST', 'EST') FROM time_tbl;

-- select new_time, ast, est (result)
--Testcase 1240:
SELECT c2, new_time(c2, 'AST', 'EST') FROM time_tbl;

-- select new_time, ast, gmt (explain)
--Testcase 1241:
EXPLAIN (COSTS OFF)
SELECT c2, new_time(c2, 'AST', 'GMT') FROM time_tbl;

-- select new_time, ast, gmt (result)
--Testcase 1242:
SELECT c2, new_time(c2, 'AST', 'GMT') FROM time_tbl;

-- select new_time, ast, hst (explain)
--Testcase 1243:
EXPLAIN (COSTS OFF)
SELECT c2, new_time(c2, 'AST', 'HST') FROM time_tbl;

-- select new_time, ast, hst (result)
--Testcase 1244:
SELECT c2, new_time(c2, 'AST', 'HST') FROM time_tbl;

-- select new_time, ast, mst (explain)
--Testcase 1245:
EXPLAIN (COSTS OFF)
SELECT c2, new_time(c2, 'AST', 'MST') FROM time_tbl;

-- select new_time, ast, mst (result)
--Testcase 1246:
SELECT c2, new_time(c2, 'AST', 'MST') FROM time_tbl;

-- select new_time, ast, nst (explain)
--Testcase 1247:
EXPLAIN (COSTS OFF)
SELECT c2, new_time(c2, 'AST', 'NST') FROM time_tbl;

-- select new_time, ast, nst (result)
--Testcase 1248:
SELECT c2, new_time(c2, 'AST', 'NST') FROM time_tbl;

-- select new_time, ast, pst (explain)
--Testcase 1249:
EXPLAIN (COSTS OFF)
SELECT c2, new_time(c2, 'AST', 'PST') FROM time_tbl;

-- select new_time, ast, pst (result)
--Testcase 1250:
SELECT c2, new_time(c2, 'AST', 'PST') FROM time_tbl;

-- select new_time, ast, yst (explain)
--Testcase 1251:
EXPLAIN (COSTS OFF)
SELECT c2, new_time(c2, 'AST', 'YST') FROM time_tbl;

-- select new_time, ast, yst (result)
--Testcase 1252:
SELECT c2, new_time(c2, 'AST', 'YST') FROM time_tbl;

-- NEXT_DAY(date, day_of_week)
-- select next_day (explain)
--Testcase 1253:
EXPLAIN (COSTS OFF)
SELECT c2, next_day(c2, 'MON') FROM time_tbl;

-- select next_day (result)
--Testcase 1254:
SELECT c2, next_day(c2, 'MON') FROM time_tbl;

-- select next_day (explain)
--Testcase 1255:
EXPLAIN (COSTS OFF)
SELECT c2, next_day(c2, 'TUE') FROM time_tbl;

-- select next_day (result)
--Testcase 1256:
SELECT c2, next_day(c2, 'TUE') FROM time_tbl;

-- select next_day (explain)
--Testcase 1257:
EXPLAIN (COSTS OFF)
SELECT c2, next_day(c2, 'WED') FROM time_tbl;

-- select next_day (result)
--Testcase 1258:
SELECT c2, next_day(c2, 'WED') FROM time_tbl;

-- select next_day (explain)
--Testcase 1259:
EXPLAIN (COSTS OFF)
SELECT c2, next_day(c2, 'THU') FROM time_tbl;

-- select next_day (result)
--Testcase 1260:
SELECT c2, next_day(c2, 'THU') FROM time_tbl;

-- select next_day (explain)
--Testcase 1261:
EXPLAIN (COSTS OFF)
SELECT c2, next_day(c2, 'FRI') FROM time_tbl;

-- select next_day (result)
--Testcase 1262:
SELECT c2, next_day(c2, 'FRI') FROM time_tbl;

-- select next_day (explain)
--Testcase 1263:
EXPLAIN (COSTS OFF)
SELECT c2, next_day(c2, 'SAT') FROM time_tbl;

-- select next_day (result)
--Testcase 1264:
SELECT c2, next_day(c2, 'SAT') FROM time_tbl;

-- select next_day (explain)
--Testcase 1265:
EXPLAIN (COSTS OFF)
SELECT c2, next_day(c2, 'MONDAY') FROM time_tbl;

-- select next_day (result)
--Testcase 1266:
SELECT c2, next_day(c2, 'MONDAY') FROM time_tbl;

-- select next_day (explain)
--Testcase 1267:
EXPLAIN (COSTS OFF)
SELECT c2, next_day(c2, 'TUESDAY') FROM time_tbl;

-- select next_day (result)
--Testcase 1268:
SELECT c2, next_day(c2, 'TUESDAY') FROM time_tbl;

-- select next_day (explain)
--Testcase 1269:
EXPLAIN (COSTS OFF)
SELECT c2, next_day(c2, 'WEDNESDAY') FROM time_tbl;

-- select next_day (result)
--Testcase 1270:
SELECT c2, next_day(c2, 'WEDNESDAY') FROM time_tbl;

-- select next_day (explain)
--Testcase 1271:
EXPLAIN (COSTS OFF)
SELECT c2, next_day(c2, 'THURSDAY') FROM time_tbl;

-- select next_day (result)
--Testcase 1272:
SELECT c2, next_day(c2, 'THURSDAY') FROM time_tbl;

-- select next_day (explain)
--Testcase 1273:
EXPLAIN (COSTS OFF)
SELECT c2, next_day(c2, 'FRIDAY') FROM time_tbl;

-- select next_day (result)
--Testcase 1274:
SELECT c2, next_day(c2, 'FRIDAY') FROM time_tbl;

-- select next_day (explain)
--Testcase 1275:
EXPLAIN (COSTS OFF)
SELECT c2, next_day(c2, 'SATURDAY') FROM time_tbl;

-- select next_day (result)
--Testcase 1276:
SELECT c2, next_day(c2, 'SATURDAY') FROM time_tbl;

-- NUMTODSINTERVAL(number, unit)
-- select numtodsinterval with day (explain)
--Testcase 1277:
EXPLAIN (COSTS OFF)
SELECT numtodsinterval(100, 'DAY') as DAY_TO_SECOND FROM time_tbl LIMIT 1;

-- select numtodsinterval with day (result)
--Testcase 1278:
SELECT numtodsinterval(100, 'DAY') as DAY_TO_SECOND FROM time_tbl LIMIT 1;

-- select numtodsinterval with hour (explain)
--Testcase 1279:
EXPLAIN (COSTS OFF)
SELECT numtodsinterval(100, 'HOUR') as DAY_TO_SECOND FROM time_tbl LIMIT 1;

-- select numtodsinterval with hour (result)
--Testcase 1280:
SELECT numtodsinterval(100, 'HOUR') as DAY_TO_SECOND FROM time_tbl LIMIT 1;

-- select numtodsinterval with minute (explain)
--Testcase 1281:
EXPLAIN (COSTS OFF)
SELECT numtodsinterval(100, 'MINUTE') as DAY_TO_SECOND FROM time_tbl LIMIT 1;

-- select numtodsinterval with minute (result)
--Testcase 1282:
SELECT numtodsinterval(100, 'MINUTE') as DAY_TO_SECOND FROM time_tbl LIMIT 1;

-- select numtodsinterval with second (result)
--Testcase 1283:
EXPLAIN (COSTS OFF)
SELECT numtodsinterval(100, 'SECOND') as DAY_TO_SECOND FROM time_tbl LIMIT 1;

-- select numtodsinterval with second (result)
--Testcase 1284:
SELECT numtodsinterval(100, 'SECOND') as DAY_TO_SECOND FROM time_tbl LIMIT 1;

-- NUMTOYMINTERVAL(number, unit)
-- select numtoyminterval with year (explain)
--Testcase 1285:
EXPLAIN (COSTS OFF)
SELECT numtoyminterval(100, 'YEAR') as YEAR_TO_MONTH FROM time_tbl LIMIT 1;

-- select numtoyminterval with year (result)
--Testcase 1286:
SELECT numtoyminterval(100, 'YEAR') as YEAR_TO_MONTH FROM time_tbl LIMIT 1;

-- select numtoyminterval with month (explain)
--Testcase 1287:
EXPLAIN (COSTS OFF)
SELECT numtoyminterval(100, 'MONTH') as YEAR_TO_MONTH FROM time_tbl LIMIT 1;

-- select numtoyminterval with month (result)
--Testcase 1288:
SELECT numtoyminterval(100, 'MONTH') as YEAR_TO_MONTH FROM time_tbl LIMIT 1;


-- ORACLE_ROUND(date/timestamp)
-- select round with date (explain)
--Testcase 1289:
EXPLAIN (COSTS OFF)
select c2, oracle_round(c2) from time_tbl;

-- select round with date (result)
--Testcase 1290:
select c2, oracle_round(c2) from time_tbl;

-- select round with date and format (explain)
-- One greater than the first two digits of a four-digit year
--Testcase 1291:
EXPLAIN (COSTS OFF)
select c2, oracle_round(c2, 'CC') from time_tbl;

-- select round with date and format (result)
-- One greater than the first two digits of a four-digit year
--Testcase 1292:
select c2, oracle_round(c2, 'CC') from time_tbl;

-- select round with date and format (explain)
-- One greater than the first two digits of a four-digit year
--Testcase 1293:
EXPLAIN (COSTS OFF)
select c2, oracle_round(c2, 'SCC') from time_tbl;

-- select round with date and format (result)
-- One greater than the first two digits of a four-digit year
--Testcase 1294:
select c2, oracle_round(c2, 'SCC') from time_tbl;

-- select round with date and format (explain)
-- Year (rounds up on July 1)
--Testcase 1295:
EXPLAIN (COSTS OFF)
select c2, oracle_round(c2, 'SYYYY') from time_tbl;

-- select round with date and format (result)
-- Year (rounds up on July 1)
--Testcase 1296:
select c2, oracle_round(c2, 'SYYYY') from time_tbl;

-- select round with date and format (explain)
-- Year (rounds up on July 1)
--Testcase 1297:
EXPLAIN (COSTS OFF)
select c2, oracle_round(c2, 'YYYY') from time_tbl;

-- select round with date and format (result)
-- Year (rounds up on July 1)
--Testcase 1298:
select c2, oracle_round(c2, 'YYYY') from time_tbl;

-- select round with date and format (explain)
-- Year (rounds up on July 1)
--Testcase 1299:
EXPLAIN (COSTS OFF)
select c2, oracle_round(c2, 'YEAR') from time_tbl;

-- select round with date and format (result)
-- Year (rounds up on July 1)
--Testcase 1300:
select c2, oracle_round(c2, 'YEAR') from time_tbl;

-- select round with date and format (explain)
-- Year (rounds up on July 1)
--Testcase 1301:
EXPLAIN (COSTS OFF)
select c2, oracle_round(c2, 'SYEAR') from time_tbl;

-- select round with date and format (result)
-- Year (rounds up on July 1)
--Testcase 1302:
select c2, oracle_round(c2, 'SYEAR') from time_tbl;

-- select round with date and format (explain)
-- Year (rounds up on July 1)
--Testcase 1303:
EXPLAIN (COSTS OFF)
select c2, oracle_round(c2, 'YYY') from time_tbl;

-- select round with date and format (result)
-- Year (rounds up on July 1)
--Testcase 1304:
select c2, oracle_round(c2, 'YYY') from time_tbl;

-- select round with date and format (explain)
-- Year (rounds up on July 1)
--Testcase 1305:
EXPLAIN (COSTS OFF)
select c2, oracle_round(c2, 'YY') from time_tbl;

-- select round with date and format (result)
-- Year (rounds up on July 1)
--Testcase 1306:
select c2, oracle_round(c2, 'YY') from time_tbl;

-- select round with date and format (explain)
-- Year (rounds up on July 1)
--Testcase 1307:
EXPLAIN (COSTS OFF)
select c2, oracle_round(c2, 'Y') from time_tbl;

-- select round with date and format (result)
-- Year (rounds up on July 1)
--Testcase 1308:
select c2, oracle_round(c2, 'Y') from time_tbl;

-- select round with date and format (explain)
-- ISO Year
--Testcase 1309:
EXPLAIN (COSTS OFF)
select c2, oracle_round(c2, 'IYYY') from time_tbl;

-- select round with date and format (result)
-- ISO Year
--Testcase 1310:
select c2, oracle_round(c2, 'IYYY') from time_tbl;

-- select round with date and format (explain)
-- ISO Year
--Testcase 1311:
EXPLAIN (COSTS OFF)
select c2, oracle_round(c2, 'IYY') from time_tbl;

-- select round with date and format (result)
-- ISO Year
--Testcase 1312:
select c2, oracle_round(c2, 'IYY') from time_tbl;

-- select round with date and format (explain)
-- ISO Year
--Testcase 1313:
EXPLAIN (COSTS OFF)
select c2, oracle_round(c2, 'IY') from time_tbl;

-- select round with date and format (result)
-- ISO Year
--Testcase 1314:
select c2, oracle_round(c2, 'IY') from time_tbl;

-- select round with date and format (explain)
-- ISO Year
--Testcase 1315:
EXPLAIN (COSTS OFF)
select c2, oracle_round(c2, 'I') from time_tbl;

-- select round with date and format (result)
-- ISO Year
--Testcase 1316:
select c2, oracle_round(c2, 'I') from time_tbl;

-- select round with date and format (explain)
-- Quarter (rounds up on the sixteenth day of the second month of the quarter)
--Testcase 1317:
EXPLAIN (COSTS OFF)
select c2, oracle_round(c2, 'Q') from time_tbl;

-- select round with date and format (result)
-- Quarter (rounds up on the sixteenth day of the second month of the quarter)
--Testcase 1318:
select c2, oracle_round(c2, 'Q') from time_tbl;

-- select round with date and format (explain)
-- Month (rounds up on the sixteenth day)
--Testcase 1319:
EXPLAIN (COSTS OFF)
select c2, oracle_round(c2, 'MONTH') from time_tbl;

-- select round with date and format (result)
-- Month (rounds up on the sixteenth day)
--Testcase 1320:
select c2, oracle_round(c2, 'MONTH') from time_tbl;

-- select round with date and format (explain)
-- Month (rounds up on the sixteenth day)
--Testcase 1321:
EXPLAIN (COSTS OFF)
select c2, oracle_round(c2, 'MON') from time_tbl;

-- select round with date and format (result)
-- Month (rounds up on the sixteenth day)
--Testcase 1322:
select c2, oracle_round(c2, 'MON') from time_tbl;

-- select round with date and format (explain)
-- Month (rounds up on the sixteenth day)
--Testcase 1323:
EXPLAIN (COSTS OFF)
select c2, oracle_round(c2, 'MM') from time_tbl;

-- select round with date and format (result)
-- Month (rounds up on the sixteenth day)
--Testcase 1324:
select c2, oracle_round(c2, 'MM') from time_tbl;

-- select round with date and format (explain)
-- Month (rounds up on the sixteenth day)
--Testcase 1325:
EXPLAIN (COSTS OFF)
select c2, oracle_round(c2, 'RM') from time_tbl;

-- select round with date and format (result)
-- Month (rounds up on the sixteenth day)
--Testcase 1326:
select c2, oracle_round(c2, 'RM') from time_tbl;

-- select round with date and format (explain)
-- Same day of the week as the first day of the year
--Testcase 1327:
EXPLAIN (COSTS OFF)
select c2, oracle_round(c2, 'WW') from time_tbl;

-- select round with date and format (result)
-- Same day of the week as the first day of the year
--Testcase 1328:
select c2, oracle_round(c2, 'WW') from time_tbl;

-- select round with date and format (explain)
-- Same day of the week as the first day of the ISO year
--Testcase 1329:
EXPLAIN (COSTS OFF)
select c2, oracle_round(c2, 'IW') from time_tbl;

-- select round with date and format (result)
-- Same day of the week as the first day of the ISO year
--Testcase 1330:
select c2, oracle_round(c2, 'IW') from time_tbl;

-- select round with date and format (explain)
-- Same day of the week as the first day of the month
--Testcase 1331:
EXPLAIN (COSTS OFF)
select c2, oracle_round(c2, 'W') from time_tbl;

-- select round with date and format (result)
-- Same day of the week as the first day of the month
--Testcase 1332:
select c2, oracle_round(c2, 'W') from time_tbl;

-- select round with date and format (explain)
-- Day
--Testcase 1333:
EXPLAIN (COSTS OFF)
select c2, oracle_round(c2, 'DDD') from time_tbl;

-- select round with date and format (result)
-- Day
--Testcase 1334:
select c2, oracle_round(c2, 'DDD') from time_tbl;

-- select round with date and format (result)
-- Day
--Testcase 1335:
EXPLAIN (COSTS OFF)
select c2, oracle_round(c2, 'DD') from time_tbl;

-- select round with date and format (result)
-- Day
--Testcase 1336:
select c2, oracle_round(c2, 'DD') from time_tbl;

-- select round with date and format (result)
-- Day
--Testcase 1337:
EXPLAIN (COSTS OFF)
select c2, oracle_round(c2, 'J') from time_tbl;

-- select round with date and format (result)
-- Day
--Testcase 1338:
select c2, oracle_round(c2, 'J') from time_tbl;

-- select round with date and format (explain)
-- Starting day of the week
--Testcase 1339:
EXPLAIN (COSTS OFF)
select c2, oracle_round(c2, 'DAY') from time_tbl;

-- select round with date and format (result)
-- Starting day of the week
--Testcase 1340:
select c2, oracle_round(c2, 'DAY') from time_tbl;

-- select round with date and format (explain)
-- Starting day of the week
--Testcase 1341:
EXPLAIN (COSTS OFF)
select c2, oracle_round(c2, 'DY') from time_tbl;

-- select round with date and format (result)
-- Starting day of the week
--Testcase 1342:
select c2, oracle_round(c2, 'DY') from time_tbl;

-- select round with date and format (explain)
-- Starting day of the week
--Testcase 1343:
EXPLAIN (COSTS OFF)
select c2, oracle_round(c2, 'D') from time_tbl;

-- select round with date and format (result)
-- Starting day of the week
--Testcase 1344:
select c2, oracle_round(c2, 'D') from time_tbl;

-- select round with date and format (explain)
-- Hour
--Testcase 1345:
EXPLAIN (COSTS OFF)
select c2, oracle_round(c2, 'HH') from time_tbl;

-- select round with date and format (result)
-- Hour
--Testcase 1346:
select c2, oracle_round(c2, 'HH') from time_tbl;

-- select round with date and format (explain)
-- Hour
--Testcase 1347:
EXPLAIN (COSTS OFF)
select c2, oracle_round(c2, 'HH12') from time_tbl;

-- select round with date and format (result)
-- Hour
--Testcase 1348:
select c2, oracle_round(c2, 'HH12') from time_tbl;

-- select round with date and format (explain)
-- Hour
--Testcase 1349:
EXPLAIN (COSTS OFF)
select c2, oracle_round(c2, 'HH24') from time_tbl;

-- select round with date and format (result)
-- Hour
--Testcase 1350:
select c2, oracle_round(c2, 'HH24') from time_tbl;

-- select round with date and format (explain)
-- Minute
--Testcase 1351:
EXPLAIN (COSTS OFF)
select c2, oracle_round(c2, 'MI') from time_tbl;

-- select round with date and format (result)
-- Minute
--Testcase 1352:
select c2, oracle_round(c2, 'MI') from time_tbl;

-- select round with timestamp (explain)
--Testcase 1353:
EXPLAIN (COSTS OFF)
select c3, oracle_round(c3) from time_tbl;

-- select round with timestamp (result)
--Testcase 1354:
select c3, oracle_round(c3) from time_tbl;

-- select round with timestamp and format (explain)
-- One greater than the first two digits of a four-digit year
--Testcase 1355:
EXPLAIN (COSTS OFF)
select c3, oracle_round(c3, 'CC') from time_tbl;

-- select round with timestamp and format (result)
-- One greater than the first two digits of a four-digit year
--Testcase 1356:
select c3, oracle_round(c3, 'CC') from time_tbl;

-- select round with timestamp and format (explain)
-- One greater than the first two digits of a four-digit year
--Testcase 1357:
EXPLAIN (COSTS OFF)
select c3, oracle_round(c3, 'SCC') from time_tbl;

-- select round with timestamp and format (result)
-- One greater than the first two digits of a four-digit year
--Testcase 1358:
select c3, oracle_round(c3, 'SCC') from time_tbl;

-- select round with timestamp and format (explain)
-- Year (rounds up on July 1)
--Testcase 1359:
EXPLAIN (COSTS OFF)
select c3, oracle_round(c3, 'SYYYY') from time_tbl;

-- select round with timestamp and format (result)
-- Year (rounds up on July 1)
--Testcase 1360:
select c3, oracle_round(c3, 'SYYYY') from time_tbl;

-- select round with timestamp and format (explain)
-- Year (rounds up on July 1)
--Testcase 1361:
EXPLAIN (COSTS OFF)
select c3, oracle_round(c3, 'YYYY') from time_tbl;

-- select round with timestamp and format (result)
-- Year (rounds up on July 1)
--Testcase 1362:
select c3, oracle_round(c3, 'YYYY') from time_tbl;

-- select round with timestamp and format (explain)
-- Year (rounds up on July 1)
--Testcase 1363:
EXPLAIN (COSTS OFF)
select c3, oracle_round(c3, 'YEAR') from time_tbl;

-- select round with timestamp and format (result)
-- Year (rounds up on July 1)
--Testcase 1364:
select c3, oracle_round(c3, 'YEAR') from time_tbl;

-- select round with timestamp and format (explain)
-- Year (rounds up on July 1)
--Testcase 1365:
EXPLAIN (COSTS OFF)
select c3, oracle_round(c3, 'SYEAR') from time_tbl;

-- select round with timestamp and format (result)
-- Year (rounds up on July 1)
--Testcase 1366:
select c3, oracle_round(c3, 'SYEAR') from time_tbl;

-- select round with timestamp and format (explain)
-- Year (rounds up on July 1)
--Testcase 1367:
EXPLAIN (COSTS OFF)
select c3, oracle_round(c3, 'YYY') from time_tbl;

-- select round with timestamp and format (result)
-- Year (rounds up on July 1)
--Testcase 1368:
select c3, oracle_round(c3, 'YYY') from time_tbl;

-- select round with timestamp and format (explain)
-- Year (rounds up on July 1)
--Testcase 1369:
EXPLAIN (COSTS OFF)
select c3, oracle_round(c3, 'YY') from time_tbl;

-- select round with timestamp and format (result)
-- Year (rounds up on July 1)
--Testcase 1370:
select c3, oracle_round(c3, 'YY') from time_tbl;

-- select round with timestamp and format (explain)
-- Year (rounds up on July 1)
--Testcase 1371:
EXPLAIN (COSTS OFF)
select c3, oracle_round(c3, 'Y') from time_tbl;

-- select round with timestamp and format (result)
-- Year (rounds up on July 1)
--Testcase 1372:
select c3, oracle_round(c3, 'Y') from time_tbl;

-- select round with timestamp and format (explain)
-- ISO Year
--Testcase 1373:
EXPLAIN (COSTS OFF)
select c3, oracle_round(c3, 'IYYY') from time_tbl;

-- select round with timestamp and format (result)
-- ISO Year
--Testcase 1374:
select c3, oracle_round(c3, 'IYYY') from time_tbl;

-- select round with timestamp and format (explain)
-- ISO Year
--Testcase 1375:
EXPLAIN (COSTS OFF)
select c3, oracle_round(c3, 'IYY') from time_tbl;

-- select round with timestamp and format (result)
-- ISO Year
--Testcase 1376:
select c3, oracle_round(c3, 'IYY') from time_tbl;

-- select round with timestamp and format (explain)
-- ISO Year
--Testcase 1377:
EXPLAIN (COSTS OFF)
select c3, oracle_round(c3, 'IY') from time_tbl;

-- select round with timestamp and format (result)
-- ISO Year
--Testcase 1378:
select c3, oracle_round(c3, 'IY') from time_tbl;

-- select round with timestamp and format (explain)
-- ISO Year
--Testcase 1379:
EXPLAIN (COSTS OFF)
select c3, oracle_round(c3, 'I') from time_tbl;

-- select round with timestamp and format (result)
-- ISO Year
--Testcase 1380:
select c3, oracle_round(c3, 'I') from time_tbl;

-- select round with timestamp and format (explain)
-- Quarter (rounds up on the sixteenth day of the second month of the quarter)
--Testcase 1381:
EXPLAIN (COSTS OFF)
select c3, oracle_round(c3, 'Q') from time_tbl;

-- select round with timestamp and format (result)
-- Quarter (rounds up on the sixteenth day of the second month of the quarter)
--Testcase 1382:
select c3, oracle_round(c3, 'Q') from time_tbl;

-- select round with timestamp and format (explain)
-- Month (rounds up on the sixteenth day)
--Testcase 1383:
EXPLAIN (COSTS OFF)
select c3, oracle_round(c3, 'MONTH') from time_tbl;

-- select round with timestamp and format (result)
-- Month (rounds up on the sixteenth day)
--Testcase 1384:
select c3, oracle_round(c3, 'MONTH') from time_tbl;

-- select round with timestamp and format (explain)
-- Month (rounds up on the sixteenth day)
--Testcase 1385:
EXPLAIN (COSTS OFF)
select c3, oracle_round(c3, 'MON') from time_tbl;

-- select round with timestamp and format (result)
-- Month (rounds up on the sixteenth day)
--Testcase 1386:
select c3, oracle_round(c3, 'MON') from time_tbl;

-- select round with timestamp and format (explain)
-- Month (rounds up on the sixteenth day)
--Testcase 1387:
EXPLAIN (COSTS OFF)
select c3, oracle_round(c3, 'MM') from time_tbl;

-- select round with timestamp and format (result)
-- Month (rounds up on the sixteenth day)
--Testcase 1388:
select c3, oracle_round(c3, 'MM') from time_tbl;

-- select round with timestamp and format (explain)
-- Month (rounds up on the sixteenth day)
--Testcase 1389:
EXPLAIN (COSTS OFF)
select c3, oracle_round(c3, 'RM') from time_tbl;

-- select round with timestamp and format (result)
-- Month (rounds up on the sixteenth day)
--Testcase 1390:
select c3, oracle_round(c3, 'RM') from time_tbl;

-- select round with timestamp and format (explain)
-- Same day of the week as the first day of the year
--Testcase 1391:
EXPLAIN (COSTS OFF)
select c3, oracle_round(c3, 'WW') from time_tbl;

-- select round with timestamp and format (result)
-- Same day of the week as the first day of the year
--Testcase 1392:
select c3, oracle_round(c3, 'WW') from time_tbl;

-- select round with timestamp and format (explain)
-- Same day of the week as the first day of the ISO year
--Testcase 1393:
EXPLAIN (COSTS OFF)
select c3, oracle_round(c3, 'IW') from time_tbl;

-- select round with timestamp and format (result)
-- Same day of the week as the first day of the ISO year
--Testcase 1394:
select c3, oracle_round(c3, 'IW') from time_tbl;

-- select round with timestamp and format (explain)
-- Same day of the week as the first day of the month
--Testcase 1395:
EXPLAIN (COSTS OFF)
select c3, oracle_round(c3, 'W') from time_tbl;

-- select round with timestamp and format (result)
-- Same day of the week as the first day of the month
--Testcase 1396:
select c3, oracle_round(c3, 'W') from time_tbl;

-- select round with timestamp and format (explain)
-- Day
--Testcase 1397:
EXPLAIN (COSTS OFF)
select c3, oracle_round(c3, 'DDD') from time_tbl;

-- select round with timestamp and format (result)
-- Day
--Testcase 1398:
select c3, oracle_round(c3, 'DDD') from time_tbl;

-- select round with timestamp and format (result)
-- Day
--Testcase 1399:
EXPLAIN (COSTS OFF)
select c3, oracle_round(c3, 'DD') from time_tbl;

-- select round with timestamp and format (result)
-- Day
--Testcase 1400:
select c3, oracle_round(c3, 'DD') from time_tbl;

-- select round with timestamp and format (explain)
-- Day
--Testcase 1401:
EXPLAIN (COSTS OFF)
select c3, oracle_round(c3, 'J') from time_tbl;

-- select round with timestamp and format (result)
-- Day
--Testcase 1402:
select c3, oracle_round(c3, 'J') from time_tbl;

-- select round with timestamp and format (explain)
-- Starting day of the week
--Testcase 1403:
EXPLAIN (COSTS OFF)
select c3, oracle_round(c3, 'DAY') from time_tbl;

-- select round with timestamp and format (result)
-- Starting day of the week
--Testcase 1404:
select c3, oracle_round(c3, 'DAY') from time_tbl;

-- select round with timestamp and format (explain)
-- Starting day of the week
--Testcase 1405:
EXPLAIN (COSTS OFF)
select c3, oracle_round(c3, 'DY') from time_tbl;

-- select round with timestamp and format (result)
-- Starting day of the week
--Testcase 1406:
select c3, oracle_round(c3, 'DY') from time_tbl;

-- select round with timestamp and format (explain)
-- Starting day of the week
--Testcase 1407:
EXPLAIN (COSTS OFF)
select c3, oracle_round(c3, 'D') from time_tbl;

-- select round with timestamp and format (result)
-- Starting day of the week
--Testcase 1408:
select c3, oracle_round(c3, 'D') from time_tbl;

-- select round with timestamp and format (explain)
-- Hour
--Testcase 1409:
EXPLAIN (COSTS OFF)
select c3, oracle_round(c3, 'HH') from time_tbl;

-- select round with timestamp and format (result)
-- Hour
--Testcase 1410:
select c3, oracle_round(c3, 'HH') from time_tbl;

-- select round with timestamp and format (explain)
-- Hour
--Testcase 1411:
EXPLAIN (COSTS OFF)
select c3, oracle_round(c3, 'HH12') from time_tbl;

-- select round with timestamp and format (result)
-- Hour
--Testcase 1412:
select c3, oracle_round(c3, 'HH12') from time_tbl;

-- select round with timestamp and format (explain)
-- Hour
--Testcase 1413:
EXPLAIN (COSTS OFF)
select c3, oracle_round(c3, 'HH24') from time_tbl;

-- select round with timestamp and format (result)
-- Hour
--Testcase 1414:
select c3, oracle_round(c3, 'HH24') from time_tbl;

-- select round with timestamp and format (explain)
-- Minute
--Testcase 1415:
EXPLAIN (COSTS OFF)
select c3, oracle_round(c3, 'MI') from time_tbl;

-- select round with timestamp and format (result)
-- Minute
--Testcase 1416:
select c3, oracle_round(c3, 'MI') from time_tbl;

--
-- End test for date/time function
--

--
-- Test for character function
--

-- CHR function
--Testcase 1417:
EXPLAIN (COSTS OFF)
SELECT id, CHR(id) FROM character_tbl;
--Testcase 1418:
SELECT id, CHR(id) FROM character_tbl;

--Testcase 1419:
EXPLAIN (COSTS OFF)
SELECT n, CHR(n) FROM character_tbl;
--Testcase 1420:
SELECT n, CHR(n) FROM character_tbl;

-- CHR fail if the input is not int
--Testcase 1421:
EXPLAIN (COSTS OFF)
SELECT fl, CHR(fl) FROM character_tbl;


-- REGEXP_REPLACE function
--Testcase 1422:
EXPLAIN (COSTS OFF)
SELECT vc, regexp_replace(vc, 'a') FROM character_tbl;
--Testcase 1423:
SELECT vc, regexp_replace(vc, 'a') FROM character_tbl;

--Testcase 1424:
EXPLAIN (COSTS OFF)
SELECT c, regexp_replace(c, 'e', 'Y') FROM character_tbl;
--Testcase 1425:
SELECT c, regexp_replace(c, 'e', 'Y') FROM character_tbl;

-- Oracle replaces all, however postgres only replace the first character.
-- To replace all on postgres, use 'g' argument.
--Testcase 1426:
EXPLAIN (COSTS OFF)
SELECT nc, REGEXP_REPLACE(nc, '(.)', '\1 ') FROM character_tbl;
--Testcase 1427:
SELECT nc, REGEXP_REPLACE(nc, '(.)', '\1 ') FROM character_tbl;

--Testcase 1428:
EXPLAIN (COSTS OFF)
SELECT lc, REGEXP_REPLACE (lc, '^(\S*)', 'FirstWord') FROM character_tbl;
--Testcase 1429:
SELECT lc, REGEXP_REPLACE (lc, '^(\S*)', 'FirstWord') FROM character_tbl;

--Testcase 1430:
EXPLAIN (COSTS OFF)
SELECT nvc, REGEXP_REPLACE (nvc, 'a|b|o|u', 'G') FROM character_tbl;
--Testcase 1431:
SELECT nvc, REGEXP_REPLACE (nvc, 'a|b|o|u', 'G') FROM character_tbl;

--Testcase 1432:
EXPLAIN (COSTS OFF)
SELECT nvc, REGEXP_REPLACE (nvc, 'a|b|u', 'G'), REGEXP_REPLACE (nvc, 'a|b|u', 'G', 1, 0, 'i') FROM character_tbl;
--Testcase 1433:
SELECT nvc, REGEXP_REPLACE (nvc, 'a|b|u', 'G'), REGEXP_REPLACE (nvc, 'a|b|u', 'G', 1, 0, 'i') FROM character_tbl;

--Testcase 1434:
EXPLAIN (COSTS OFF)
SELECT lc, REGEXP_REPLACE (lc, 'a|e', 'O', 4), REGEXP_REPLACE (lc, 'a|e', 'O', 8) FROM character_tbl;
--Testcase 1435:
SELECT lc, REGEXP_REPLACE (lc, 'a|e', 'O', 4), REGEXP_REPLACE (lc, 'a|e', 'O', 8) FROM character_tbl;

--Testcase 1436:
EXPLAIN (COSTS OFF)
SELECT lc, REGEXP_REPLACE (lc, 'a|e', 'O', 4, 1), REGEXP_REPLACE (lc, 'a|e', 'O', 8, 0) FROM character_tbl;
--Testcase 1437:
SELECT lc, REGEXP_REPLACE (lc, 'a|e', 'O', 4, 1), REGEXP_REPLACE (lc, 'a|e', 'O', 8, 0) FROM character_tbl;

-- Oracle does not support this argument, however postgresql does.
--Testcase 1438:
EXPLAIN (COSTS OFF)
SELECT vc, regexp_replace(vc, 'r(..)', 'X\1Y', 'g') FROM character_tbl;
--Testcase 1439:
SELECT vc, regexp_replace(vc, 'r(..)', 'X\1Y', 'g') FROM character_tbl;


-- TRIM function
--Testcase 1440:
EXPLAIN (COSTS OFF)
SELECT vc, TRIM(vc) FROM character_tbl;
--Testcase 1441:
SELECT vc, TRIM(vc) FROM character_tbl;

--Testcase 1442:
EXPLAIN (COSTS OFF)
SELECT c, TRIM(LEADING 'sf' FROM c), TRIM(TRAILING 'r' FROM c), TRIM(BOTH 'r' FROM c) FROM character_tbl;
--Testcase 1443:
SELECT c, TRIM(LEADING 'sf' FROM c), TRIM(TRAILING 'r' FROM c), TRIM(BOTH 'r' FROM c) FROM character_tbl;

--Testcase 1444:
EXPLAIN (COSTS OFF)
SELECT lc, TRIM('t' FROM lc) FROM character_tbl;
--Testcase 1445:
SELECT lc, TRIM('t' FROM lc) FROM character_tbl;

--Testcase 1446:
EXPLAIN (COSTS OFF)
SELECT lc, TRIM('' FROM lc) FROM character_tbl;
--Testcase 1447:
SELECT lc, TRIM('' FROM lc) FROM character_tbl;


-- ASCII function
--Testcase 1448:
EXPLAIN (COSTS OFF)
SELECT lc, ASCII(lc), ASCII(SUBSTR(lc, 1, 1)), ASCII(SUBSTR(lc, 3, 1)) FROM character_tbl;
--Testcase 1449:
SELECT lc, ASCII(lc), ASCII(SUBSTR(lc, 1, 1)), ASCII(SUBSTR(lc, 3, 1)) FROM character_tbl;

--Testcase 1450:
EXPLAIN (COSTS OFF)
SELECT nvc, ASCII(nvc) FROM character_tbl WHERE ASCII(c) > 100;
--Testcase 1451:
SELECT nvc, ASCII(nvc) FROM character_tbl WHERE ASCII(c) > 100;


-- GREATEST function
--Testcase 1452:
EXPLAIN (COSTS OFF)
SELECT GREATEST(id, 5, 500), GREATEST(id, 5, 85, NULL) FROM character_tbl WHERE GREATEST(id, 5, 85) != 0;
--Testcase 1453:
SELECT GREATEST(id, 5, 500), GREATEST(id, 5, 85, NULL) FROM character_tbl WHERE GREATEST(id, 5, 85) != 0;

--Testcase 1454:
EXPLAIN (COSTS OFF)
SELECT GREATEST(c, 'electronic', 'niko') FROM character_tbl;
--Testcase 1455:
SELECT GREATEST(c, 'electronic', 'niko') FROM character_tbl;


-- LEAST function
--Testcase 1456:
EXPLAIN (COSTS OFF)
SELECT LEAST(n, 5, 500, NULL), LEAST(n, 111, 185) FROM character_tbl WHERE LEAST(n, 5, 85) != 0;
--Testcase 1457:
SELECT LEAST(n, 5, 500, NULL), LEAST(n, 111, 185) FROM character_tbl WHERE LEAST(n, 5, 85) != 0;

--Testcase 1458:
EXPLAIN (COSTS OFF)
SELECT LEAST(nvc, 'Liquid', 'Johnny') FROM character_tbl;
--Testcase 1459:
SELECT LEAST(nvc, 'Liquid', 'Johnny') FROM character_tbl;


-- COALESCE function
--Testcase 1460:
EXPLAIN (COSTS OFF)
SELECT COALESCE(n, 5, 500, NULL), COALESCE(111, n) FROM character_tbl WHERE COALESCE(n, 5, 85) != 0;
--Testcase 1461:
SELECT COALESCE(n, 5, 500, NULL), COALESCE(111, n) FROM character_tbl WHERE COALESCE(n, 5, 85) != 0;

--Testcase 1462:
EXPLAIN (COSTS OFF)
SELECT COALESCE(1.2*id, n, 19) FROM character_tbl;
--Testcase 1463:
SELECT COALESCE(1.2*id, n, 19) FROM character_tbl;


-- NULLIF function
--Testcase 1464:
EXPLAIN (COSTS OFF)
SELECT NULLIF(n, 5), NULLIF(n, 111) FROM character_tbl WHERE NULLIF(n, 85) != 0;
--Testcase 1465:
SELECT NULLIF(n, 5), NULLIF(n, 111) FROM character_tbl WHERE NULLIF(n, 85) != 0;

--Testcase 1466:
EXPLAIN (COSTS OFF)
SELECT NULLIF(c, 'Fansipan') FROM character_tbl;
--Testcase 1467:
SELECT NULLIF(c, 'Fansipan') FROM character_tbl;


-- TO_CHAR (character) function
--Testcase 1468:
EXPLAIN (COSTS OFF)
SELECT TO_CHAR(nc), TO_CHAR(lc), TO_CHAR('113') FROM character_tbl;
--Testcase 1469:
SELECT TO_CHAR(nc), TO_CHAR(lc), TO_CHAR('113') FROM character_tbl;

--Testcase 1470:
EXPLAIN (COSTS OFF)
SELECT TO_CHAR('01110') FROM character_tbl;
--Testcase 1471:
SELECT TO_CHAR('01110') FROM character_tbl;

-- TO_CHAR (datetime) function
--Testcase 1472:
EXPLAIN (COSTS OFF)
SELECT TO_CHAR(itv, 'DD-MON-YYYY') FROM character_tbl;
--Testcase 1473:
SELECT TO_CHAR(itv, 'DD-MON-YYYY') FROM character_tbl;

--Testcase 1474:
EXPLAIN (COSTS OFF)
SELECT TO_CHAR(timetz, 'DD-MON-YYYY HH24:MI:SSxFF TZH:TZM') FROM character_tbl;
--Testcase 1475:
SELECT TO_CHAR(timetz, 'DD-MON-YYYY HH24:MI:SSxFF TZH:TZM') FROM character_tbl;

--Testcase 1476:
EXPLAIN (COSTS OFF)
SELECT TO_CHAR(dt, 'DD-MON-YYYY HH24:MI:SS') FROM character_tbl;
--Testcase 1477:
SELECT TO_CHAR(dt, 'DD-MON-YYYY HH24:MI:SS') FROM character_tbl;

--Testcase 1478:
EXPLAIN (COSTS OFF)
SELECT TO_CHAR(dt) FROM character_tbl;
--Testcase 1479:
SELECT TO_CHAR(dt) FROM character_tbl;

--Testcase 1480:
EXPLAIN (COSTS OFF)
SELECT TO_CHAR(TIMESTAMP'1999-12-01 10:00:00') FROM character_tbl;
--Testcase 1481:
SELECT TO_CHAR(TIMESTAMP'1999-12-01 10:00:00') FROM character_tbl;

--Testcase 1482:
EXPLAIN (COSTS OFF)
SELECT TO_CHAR(INTERVAL '10 days 21 hours') FROM character_tbl;
--Testcase 1483:
SELECT TO_CHAR(INTERVAL '10 days 21 hours') FROM character_tbl;

-- TO_TIMESTAMP function
--Testcase 1484:
EXPLAIN (COSTS OFF)
SELECT TO_TIMESTAMP(dt_text, 'DD-Mon-RR HH24:MI:SS.FF') FROM character_tbl;
--Testcase 1485:
SELECT TO_TIMESTAMP(dt_text, 'DD-Mon-RR HH24:MI:SS.FF') FROM character_tbl;

--Testcase 1486:
EXPLAIN (COSTS OFF)
SELECT TO_TIMESTAMP('05 Dec 2000', 'DD Mon YYYY') FROM character_tbl;
--Testcase 1487:
SELECT TO_TIMESTAMP('05 Dec 2000', 'DD Mon YYYY') FROM character_tbl;

--
-- End test for character function
--

--Testcase 1488:
DROP FOREIGN TABLE numeric_tbl;
--Testcase 1489:
DROP FOREIGN TABLE time_tbl;
--Testcase 1490:
DROP FOREIGN TABLE character_tbl;
--Testcase 1491:
DROP USER MAPPING FOR CURRENT_USER SERVER oracle_srv;
--Testcase 1492:
DROP SERVER oracle_srv;
--Testcase 1493:
DROP EXTENSION oracle_fdw CASCADE;
