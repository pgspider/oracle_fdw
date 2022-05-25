CREATE FUNCTION oracle_fdw_handler() RETURNS fdw_handler
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT;

COMMENT ON FUNCTION oracle_fdw_handler()
IS 'Oracle foreign data wrapper handler';

CREATE FUNCTION oracle_fdw_validator(text[], oid) RETURNS void
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT;

COMMENT ON FUNCTION oracle_fdw_validator(text[], oid)
IS 'Oracle foreign data wrapper options validator';

CREATE FUNCTION oracle_close_connections() RETURNS void
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT;

COMMENT ON FUNCTION oracle_close_connections()
IS 'closes all open Oracle connections';

CREATE FUNCTION oracle_diag(name DEFAULT NULL) RETURNS text
AS 'MODULE_PATHNAME'
LANGUAGE C STABLE CALLED ON NULL INPUT;

COMMENT ON FUNCTION oracle_diag(name)
IS 'shows the version of oracle_fdw, PostgreSQL, Oracle client and Oracle server';

CREATE FUNCTION oracle_execute(server name, statement text) RETURNS void
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT;

COMMENT ON FUNCTION oracle_execute(name, text)
IS 'executes an arbitrary SQL statement with no results on the Oracle server';

CREATE FOREIGN DATA WRAPPER oracle_fdw
  HANDLER oracle_fdw_handler
  VALIDATOR oracle_fdw_validator;

COMMENT ON FOREIGN DATA WRAPPER oracle_fdw
IS 'Oracle foreign data wrapper';

CREATE PROCEDURE oracle_create_or_replace_stub(func_type text, name_arg text, return_type regtype) AS $$
DECLARE
  proname_raw text := split_part(name_arg, '(', 1);
  proname text := ltrim(rtrim(proname_raw));
BEGIN
  IF lower(func_type) = 'aggregation' OR lower(func_type) = 'aggregate' OR lower(func_type) = 'agg' OR lower(func_type) = 'a' THEN
    DECLARE
      proargs_raw text := right(name_arg, length(name_arg) - length(proname_raw));
      proargs text := ltrim(rtrim(proargs_raw));
      proargs_types text := right(left(proargs, length(proargs) - 1), length(proargs) - 2);
      aggproargs text := format('(%s, %s)', return_type, proargs_types);
    BEGIN
      BEGIN
        EXECUTE format('
          CREATE FUNCTION %s_sfunc%s RETURNS %s IMMUTABLE AS $inner$
          BEGIN
            RAISE EXCEPTION ''stub %s_sfunc%s is called'';
            RETURN NULL;
          END $inner$ LANGUAGE plpgsql;',
	  proname, aggproargs, return_type, proname, aggproargs);
      EXCEPTION
        WHEN duplicate_function THEN
          RAISE DEBUG 'stub function for aggregation already exists (ignored)';
      END;
      BEGIN
        IF lower(proargs_types) = '*' THEN
          name_arg := format('%s(*)', proname);
        END IF;
        EXECUTE format('
          CREATE AGGREGATE %s
          (
            sfunc = %s_sfunc,
            stype = %s
          );', name_arg, proname, return_type);
      EXCEPTION
        WHEN duplicate_function THEN
          RAISE DEBUG 'stub aggregation already exists (ignored)';
        WHEN others THEN
          RAISE EXCEPTION 'stub aggregation exception';
      END;
    END;
  ELSIF lower(func_type) = 'function' OR lower(func_type) = 'func' OR lower(func_type) = 'f' THEN
    BEGIN
      EXECUTE format('
        CREATE FUNCTION %s RETURNS %s IMMUTABLE AS $inner$
        BEGIN
          RAISE EXCEPTION ''stub %s is called'';
          RETURN NULL;
        END $inner$ LANGUAGE plpgsql COST 1;',
        name_arg, return_type, name_arg);
    EXCEPTION
      WHEN duplicate_function THEN
        RAISE DEBUG 'stub already exists (ignored)';
    END;
  ELSEIF lower(func_type) = 'stable function' OR lower(func_type) = 'sfunc' OR lower(func_type) = 'sf' THEN
    BEGIN
      EXECUTE format('
        CREATE FUNCTION %s RETURNS %s STABLE AS $inner$
        BEGIN
          RAISE EXCEPTION ''stub %s is called'';
          RETURN NULL;
        END $inner$ LANGUAGE plpgsql COST 1;',
        name_arg, return_type, name_arg);
    EXCEPTION
      WHEN duplicate_function THEN
        RAISE DEBUG 'stub already exists (ignored)';
    END;
  ELSEIF lower(func_type) = 'volatile function' OR lower(func_type) = 'vfunc' OR lower(func_type) = 'vf' THEN
    BEGIN
      EXECUTE format('
        CREATE FUNCTION %s RETURNS %s VOLATILE AS $inner$
        BEGIN
          RAISE EXCEPTION ''stub %s is called'';
          RETURN NULL;
        END $inner$ LANGUAGE plpgsql COST 1;',
        name_arg, return_type, name_arg);
    EXCEPTION
      WHEN duplicate_function THEN
        RAISE DEBUG 'stub already exists (ignored)';
    END;
  ELSE
    RAISE EXCEPTION 'not supported function type %', func_type;
    BEGIN
      EXECUTE format('
        CREATE FUNCTION %s_sfunc RETURNS %s AS $inner$
        BEGIN
          RAISE EXCEPTION ''stub %s is called'';
          RETURN NULL;
       END $inner$ LANGUAGE plpgsql COST 1;',
        name_arg, return_type, name_arg);
    EXCEPTION
      WHEN duplicate_function THEN
        RAISE DEBUG 'stub already exists (ignored)';
    END;
  END IF;
END
$$ LANGUAGE plpgsql;

-- Character Funtions
CALL oracle_create_or_replace_stub('vf', 'regexp_replace(text, text)', 'text');
CALL oracle_create_or_replace_stub('vf', 'regexp_replace(text, text, text, int)', 'text');
CALL oracle_create_or_replace_stub('vf', 'regexp_replace(text, text, text, int, int)', 'text');
CALL oracle_create_or_replace_stub('vf', 'regexp_replace(text, text, text, int, int, text)', 'text');

CALL oracle_create_or_replace_stub('vf', 'to_char(text)', 'text');
CALL oracle_create_or_replace_stub('vf', 'to_char(timestamp)', 'text');
CALL oracle_create_or_replace_stub('vf', 'to_char(interval)', 'text');

-- Date and Time Functions
CALL oracle_create_or_replace_stub('vf', 'oracle_current_date()', 'date');
CALL oracle_create_or_replace_stub('vf', 'oracle_current_timestamp()', 'timestamp');
CALL oracle_create_or_replace_stub('vf', 'oracle_localtimestamp()', 'timestamp');
CALL oracle_create_or_replace_stub('vf', 'oracle_extract(text, date)', 'integer');
-- todo: oracle fdw does not support interval year to month yet
--CALL oracle_create_or_replace_stub('vf', 'oracle_extract(text, interval year to month)', 'integer');
CALL oracle_create_or_replace_stub('vf', 'oracle_extract(text, interval day to second(6))', 'double precision');
CALL oracle_create_or_replace_stub('vf', 'oracle_extract(text, timestamp)', 'double precision');
CALL oracle_create_or_replace_stub('vf', 'oracle_extract(text, timestamp with time zone)', 'double precision');
CALL oracle_create_or_replace_stub('vf', 'add_months(date, integer)', 'date');
CALL oracle_create_or_replace_stub('vf', 'last_day(date)', 'date');
CALL oracle_create_or_replace_stub('vf', 'dbtimezone()', 'text');
CALL oracle_create_or_replace_stub('vf', 'from_tz(timestamp, text)', 'timestamp with time zone');
CALL oracle_create_or_replace_stub('vf', 'months_between(date, date)', 'double precision');
CALL oracle_create_or_replace_stub('vf', 'new_time(date, text, text)', 'date');
CALL oracle_create_or_replace_stub('vf', 'next_day(date, text)', 'date');
CALL oracle_create_or_replace_stub('vf', 'numtodsinterval(integer, text)', 'interval');
CALL oracle_create_or_replace_stub('vf', 'numtoyminterval(integer, text)', 'interval');
CALL oracle_create_or_replace_stub('vf', 'oracle_round(date)', 'date');
CALL oracle_create_or_replace_stub('vf', 'oracle_round(timestamp)', 'date');
CALL oracle_create_or_replace_stub('vf', 'oracle_round(date, text)', 'date');
CALL oracle_create_or_replace_stub('vf', 'oracle_round(timestamp, text)', 'date');

-- aggregate functions
CALL oracle_create_or_replace_stub('a', 'approx_count_distinct(anyelement)', 'integer');
