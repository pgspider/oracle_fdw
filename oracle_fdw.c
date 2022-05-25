/*-------------------------------------------------------------------------
 *
 * oracle_fdw.c
 * 		PostgreSQL-related functions for Oracle foreign data wrapper.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "fmgr.h"
#include "access/htup_details.h"
#include "access/reloptions.h"
#include "access/sysattr.h"
#include "access/xact.h"
#include "catalog/pg_aggregate.h"
#include "catalog/indexing.h"
#include "catalog/pg_attribute.h"
#include "catalog/pg_cast.h"
#include "catalog/pg_collation.h"
#include "catalog/pg_foreign_data_wrapper.h"
#include "catalog/pg_foreign_server.h"
#include "catalog/pg_foreign_table.h"
#include "catalog/pg_namespace.h"
#include "catalog/pg_operator.h"
#include "catalog/pg_proc.h"
#include "catalog/pg_user_mapping.h"
#include "catalog/pg_type.h"
#include "commands/defrem.h"
#include "commands/explain.h"
#include "commands/vacuum.h"
#include "foreign/fdwapi.h"
#include "foreign/foreign.h"
#include "funcapi.h"
#if PG_VERSION_NUM < 100000
#include "libpq/md5.h"
#else
#include "common/md5.h"
#endif  /* PG_VERSION_NUM */
#include "libpq/pqsignal.h"
#include "mb/pg_wchar.h"
#include "miscadmin.h"
#include "nodes/makefuncs.h"
#include "nodes/nodeFuncs.h"
#include "nodes/pg_list.h"
#if PG_VERSION_NUM <= 1340000
#include "optimizer/clauses.h"
#endif
#include "optimizer/cost.h"
#if PG_VERSION_NUM >= 140000
#include "optimizer/appendinfo.h"
#endif  /* PG_VERSION_NUM */
#include "optimizer/pathnode.h"
#if PG_VERSION_NUM >= 130000
#include "optimizer/paths.h"
#endif  /* PG_VERSION_NUM */
#include "optimizer/planmain.h"
#include "optimizer/prep.h"
#include "optimizer/restrictinfo.h"
#include "optimizer/tlist.h"
#include "parser/parse_relation.h"
#include "parser/parsetree.h"
#include "port.h"
#include "storage/ipc.h"
#include "storage/lock.h"
#include "tcop/tcopprot.h"
#include "utils/array.h"
#include "utils/builtins.h"
#include "utils/catcache.h"
#include "utils/date.h"
#include "utils/datetime.h"
#include "utils/elog.h"
#include "utils/fmgroids.h"
#include "utils/formatting.h"
#include "utils/float.h"
#include "utils/guc.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/rel.h"
#include "utils/resowner.h"
#include "utils/timestamp.h"
#include "utils/snapmgr.h"
#include "utils/syscache.h"
#include "utils/selfuncs.h"
#include "utils/typcache.h"
#if PG_VERSION_NUM < 120000
#include "nodes/relation.h"
#include "optimizer/var.h"
#include "utils/tqual.h"
#else
#include "nodes/pathnodes.h"
#include "optimizer/optimizer.h"
#include "access/heapam.h"
#endif

#include <string.h>
#include <stdlib.h>

#include "oracle_fdw.h"

/* defined in backend/commands/analyze.c */
#ifndef WIDTH_THRESHOLD
#define WIDTH_THRESHOLD 1024
#endif  /* WIDTH_THRESHOLD */

#if PG_VERSION_NUM >= 90500
#define IMPORT_API

/* array_create_iterator has a new signature from 9.5 on */
#define array_create_iterator(arr, slice_ndim) array_create_iterator(arr, slice_ndim, NULL)
#else
#undef IMPORT_API
#endif  /* PG_VERSION_NUM */

#if PG_VERSION_NUM >= 90600
#define JOIN_API

/* the useful macro IS_SIMPLE_REL is defined in v10, backport */
#ifndef IS_SIMPLE_REL
#define IS_SIMPLE_REL(rel) \
	((rel)->reloptkind == RELOPT_BASEREL || \
	(rel)->reloptkind == RELOPT_OTHER_MEMBER_REL)
#endif

/* GetConfigOptionByName has a new signature from 9.6 on */
#define GetConfigOptionByName(name, varname) GetConfigOptionByName(name, varname, false)
#else
#undef JOIN_API
#endif  /* PG_VERSION_NUM */

#if PG_VERSION_NUM < 110000
/* backport macro from V11 */
#define TupleDescAttr(tupdesc, i) ((tupdesc)->attrs[(i)])
#endif  /* PG_VERSION_NUM */

/* list API has changed in v13 */
#if PG_VERSION_NUM < 130000
#define list_next(l, e) lnext((e))
#define do_each_cell(cell, list, element) for_each_cell(cell, (element))
#else
#define list_next(l, e) lnext((l), (e))
#define do_each_cell(cell, list, element) for_each_cell(cell, (list), (element))
#endif  /* PG_VERSION_NUM */

/* "table_open" was "heap_open" before v12 */
#if PG_VERSION_NUM < 120000
#define table_open(x, y) heap_open(x, y)
#define table_close(x, y) heap_close(x, y)
#endif  /* PG_VERSION_NUM */

#if PG_VERSION_NUM <= 134000
/* source-code-compatibility hacks for pull_varnos() API change */
#define make_restrictinfo(a,b,c,d,e,f,g,h,i) make_restrictinfo_new(a,b,c,d,e,f,g,h,i)
#endif

PG_MODULE_MAGIC;

/* Default CPU cost to start up a foreign query. */
#define DEFAULT_FDW_STARTUP_COST	100.0

/* Default CPU cost to process 1 row (above and beyond cpu_tuple_cost). */
#define DEFAULT_FDW_TUPLE_COST		0.01

/* If no remote estimates, assume a sort costs 20% extra */
#define DEFAULT_FDW_SORT_MULTIPLIER 1.2

/*
 * "true" if Oracle data have been modified in the current transaction.
 */
static bool dml_in_transaction = false;

/*
 * PostGIS geometry type, set upon library initialization.
 */
static Oid GEOMETRYOID = InvalidOid;
static bool geometry_is_setup = false;

/*
 * OracleSupportedBuiltinAggFunction
 * List of supported builtin aggregate functions for Oracle
 */
static const char *OracleSupportedBuiltinAggFunction[] = {
	"sum",
	"avg",
	"max",
	"min",
	"stddev",
	"count",
	"variance",
	"corr",
	"covar_pop",
	"covar_samp",
	"cume_dist",
	"dense_rank",
	"percent_rank",
	"stddev_pop",
	"stddev_samp",
	"var_pop",
	"var_samp",
	"percentile_cont",
	"percentile_disc",
	NULL};

/*
 * OracleSupportedUniqueAggFunction
 * List of supported unique aggregate functions for Oracle
 */
static const char *OracleUniqueAggFunction[] = {
	"approx_count_distinct",
	NULL};

/*
 * OracleSupportedBuiltinNumericFunction
 * List of supported builtin numeric functions for Oracle
 */
static const char *OracleSupportedBuiltinNumericFunction[] = {
	"abs",
	"acos",
	"asin",
	"atan",
	"atan2",
	"ceil",
	"ceiling",
	"cos",
	"cosh",
	"exp",
	"floor",
	"ln",
	"log",
	"mod",
	"pow",
	"power",
	"round",
	"sign",
	"sin",
	"sinh",
	"sqrt",
	"tan",
	"tanh",
NULL};

/*
 * OracleSupportedUniqueNumericFunction
 * List of supported unique numeric functions for Oracle
 */
static const char *OracleUniqueNumericFunction[] = {
	"oracle_round",
	NULL};

/*
 * OracleSupportedBuiltinStringFunction
 * List of supported builtin string functions for Oracle
 */
static const char *OracleSupportedBuiltinStringFunction[] = {
	"ascii",
	"char_length",
	"character_length",
	"chr",
	"initcap",
	"length",
	"lower",
	"lpad",
	"ltrim",
	"octet_length",
	"position",
	"replace",
	"rpad",
	"rtrim",
	"regexp_replace",
	"strpos",
	"substr",
	"substring",
	"translate",
	"trunc",
	"upper",
	"width_bucket",
	"to_char",
	"to_date",
	"to_number",
	"to_timestamp",
NULL};

/*
 * OracleUniqueDateTimeFunction
 * List of unique Date/Time function for Oracle
 */
static const char *OracleUniqueDateTimeFunction[] = {
	"add_months",
	"last_day",
	"oracle_current_date",
	"oracle_current_timestamp",
	"oracle_localtimestamp",
	"oracle_extract",
	"dbtimezone",
	"from_tz",
	"months_between",
	"new_time",
	"next_day",
	"numtodsinterval",
	"numtoyminterval",
NULL};

/*
 * Describes the valid options for objects that use this wrapper.
 */
struct OracleFdwOption
{
	const char *optname;
	Oid			optcontext;  /* Oid of catalog in which option may appear */
	bool		optrequired;
};

typedef struct pull_func_clause_context
{
	List	   *funclist;
}			pull_func_clause_context;

/*
 * Context for deparseExpr
 */
typedef struct deparse_expr_cxt
{
	PlannerInfo *root;			/* global planner state */
	RelOptInfo *foreignrel;		/* the foreign relation we are planning for */
	RelOptInfo *scanrel;		/* the underlying scan relation. Same as
								 * foreignrel, when that represents a join or
								 * a base relation. */
	StringInfo buf;				/* output buffer to append to */
	List **params_list;			/* exprs that will become remote Params */
	oracleSession *session;		/* encapsulates the active Oracle session */
	struct oraTable *oraTable;	/* description of the remote Oracle table */
	bool use_alias;				/* mark alias use */
	Index ignore_rel;			/* is either zero or the RT index of a target relation.
								 * Use for deparsing join relation */
	List **ignore_conds;		/* List of join clause. Use for deparsing join relation */
	bool string_comparison;		/* mark if handling string comparison */
	bool handle_length_func;	/* mark if handling length function */
	bool can_pushdown_function; /* true if query contains function
								 * which can pushed down to remote server */
	bool handle_aggref;			/* mark if handling aggregation */
} deparse_expr_cxt;

#define OPT_NLS_LANG "nls_lang"
#define OPT_DBSERVER "dbserver"
#define OPT_ISOLATION_LEVEL "isolation_level"
#define OPT_NCHAR "nchar"
#define OPT_USER "user"
#define OPT_PASSWORD "password"
#define OPT_DBLINK "dblink"
#define OPT_SCHEMA "schema"
#define OPT_TABLE "table"
#define OPT_MAX_LONG "max_long"
#define OPT_READONLY "readonly"
#define OPT_KEY "key"
#define OPT_STRIP_ZEROS "strip_zeros"
#define OPT_SAMPLE "sample_percent"
#define OPT_PREFETCH "prefetch"
#define OPT_COLUMN_NAME "column_name"

#define DEFAULT_ISOLATION_LEVEL ORA_TRANS_SERIALIZABLE
#define DEFAULT_MAX_LONG 32767
#define DEFAULT_PREFETCH 200

/*
 * Options for case folding for names in IMPORT FOREIGN TABLE.
 */
typedef enum { CASE_KEEP, CASE_LOWER, CASE_SMART } fold_t;

/*
 * Valid options for oracle_fdw.
 */
static struct OracleFdwOption valid_options[] = {
	{OPT_NLS_LANG, ForeignDataWrapperRelationId, false},
	{OPT_DBSERVER, ForeignServerRelationId, true},
	{OPT_ISOLATION_LEVEL, ForeignServerRelationId, false},
	{OPT_NCHAR, ForeignServerRelationId, false},
	{OPT_USER, UserMappingRelationId, true},
	{OPT_PASSWORD, UserMappingRelationId, true},
	{OPT_DBLINK, ForeignTableRelationId, false},
	{OPT_SCHEMA, ForeignTableRelationId, false},
	{OPT_TABLE, ForeignTableRelationId, true},
	{OPT_MAX_LONG, ForeignTableRelationId, false},
	{OPT_READONLY, ForeignTableRelationId, false},
	{OPT_SAMPLE, ForeignTableRelationId, false},
	{OPT_PREFETCH, ForeignTableRelationId, false},
	{OPT_COLUMN_NAME, AttributeRelationId, false},
	{OPT_KEY, AttributeRelationId, false},
	{OPT_STRIP_ZEROS, AttributeRelationId, false}
};

#define option_count (sizeof(valid_options)/sizeof(struct OracleFdwOption))

/*
 * Array to hold the type output functions during table modification.
 * It is ok to hold this cache in a static variable because there cannot
 * be more than one foreign table modified at the same time.
 */

static regproc *output_funcs;

/*
 * FDW-specific information for RelOptInfo.fdw_private and ForeignScanState.fdw_state.
 * The same structure is used to hold information for query planning and execution.
 * The structure is initialized during query planning and passed on to the execution
 * step serialized as a List (see serializePlanData and deserializePlanData).
 * For DML statements, the scan stage and the modify stage both hold an
 * OracleFdwState, and the latter is initialized by copying the former (see copyPlanData).
 */
struct OracleFdwState {
	char *dbserver;                /* Oracle connect string */
	oraIsoLevel isolation_level;   /* Transaction Isolation Level */
	char *user;                    /* Oracle username */
	char *password;                /* Oracle password */
	char *nls_lang;                /* Oracle locale information */
	bool have_nchar;               /* needs support for national character conversion */
	oracleSession *session;        /* encapsulates the active Oracle session */
	char *query;                   /* query we issue against Oracle */
	List *params;                  /* list of parameters needed for the query */
	struct paramDesc *paramList;   /* description of parameters needed for the query */
	struct oraTable *oraTable;     /* description of the remote Oracle table */
	Cost startup_cost;             /* cost estimate, only needed for planning */
	Cost total_cost;               /* cost estimate, only needed for planning */
	unsigned long rowcount;        /* rows already read from Oracle */
	int columnindex;               /* currently processed column for error context */
	MemoryContext temp_cxt;        /* short-lived memory for data modification */
	unsigned int prefetch;         /* number of rows to prefetch */
	char *order_clause;            /* for ORDER BY pushdown */
	List *usable_pathkeys;         /* for ORDER BY pushdown */
	char *where_clause;            /* deparsed where clause */
	char *limit_clause;            /* deparsed limit clause */

	/* FOR FOREIGN SCAN and FOREIGN MODIFICATION */
	/*
	 * Restriction clauses, divided into safe and unsafe to pushdown subsets.
	 *
	 * For a base foreign relation this is a list of clauses along-with
	 * RestrictInfo wrapper. Keeping RestrictInfo wrapper helps while dividing
	 * scan_clauses in oracleGetForeignPlan into safe and unsafe subsets.
	 * Also it helps in estimating costs since RestrictInfo caches the
	 * selectivity and qual cost for the clause in it.
	 *
	 * For a join relation, however, they are part of otherclause list
	 * obtained from extract_actual_join_clauses, which strips RestrictInfo
	 * construct. So, for a join relation they are list of bare clauses.
	 */
	List       *remote_conds;  /* can be pushed down to remote server */
	List       *local_conds;   /* cannot be pushed down to remote server */

	/* Join information */
	RelOptInfo *outerrel;
	RelOptInfo *innerrel;
	JoinType    jointype;
	List       *joinclauses;

	long max_long;				/* use this for re-build oraTable */

	List	    *retrieved_attrs;	/* attr numbers retrieved by RETURNING */

	/* RELATION INFO */

	/*
	 * True means that the relation can be pushed down. Always true for simple
	 * foreign scan.
	 */
	bool		pushdown_safe;

	/* Actual remote restriction clauses for scan (sans RestrictInfos) */
	List	   *final_remote_exprs;

	/* Bitmap of attr numbers we need to fetch from the remote server. */
	Bitmapset  *attrs_used;

	/* True means that the query_pathkeys is safe to push down */
	bool		qp_is_pushdown_safe;

	/* Cost and selectivity of local_conds. */
	QualCost	local_conds_cost;
	Selectivity local_conds_sel;

	/* Estimated size and cost for a scan, join, or grouping/aggregation. */
	double		rows;
	int			width;

	/*
	 * Estimated number of rows fetched from the foreign server, and costs
	 * excluding costs for transferring those rows from the foreign server.
	 * These are only used by estimate_path_cost_size().
	 */
	double		retrieved_rows;
	Cost		rel_startup_cost;
	Cost		rel_total_cost;

	/* Options extracted from catalogs. */
	Cost		fdw_startup_cost;
	Cost		fdw_tuple_cost;

	/* Cached catalog information. */
	ForeignTable *table;
	ForeignServer *server;

	/*
	 * Name of the relation, for use while EXPLAINing ForeignScan.  It is used
	 * for join and upper relations but is set for all relations.  For a base
	 * relation, this is really just the RT index as a string; we convert that
	 * while producing EXPLAIN output.  For join and upper relations, the name
	 * indicates which base foreign tables are included and the join type or
	 * aggregation type used.
	 */
	char	   *relation_name;

	/* Upper relation information */
	UpperRelationKind stage;

	/* Grouping information */
	List	   *grouped_tlist;

	/* Subquery information */
	bool		make_outerrel_subquery; /* do we deparse outerrel as a
										 * subquery? */
	bool		make_innerrel_subquery; /* do we deparse innerrel as a
										 * subquery? */
	Relids		lower_subquery_rels;	/* all relids appearing in lower
										 * subqueries */

	/*
	 * Index of the relation.  It is used to create an alias to a subquery
	 * representing the relation.
	 */
	int			relation_index;

	/* Function pushdown support in target list */
	bool            is_tlist_func_pushdown;

	/* scan tlist */
	List		*fdw_scan_tlist;

	/* FOR DIRECT MODIFICATION */
	Relation	rel;			/* relcache entry for the foreign table */

								/* extracted fdw_private data */
	bool		has_returning;	/* is there a RETURNING clause? */
	bool		set_processed;	/* do we set the command es_processed? */

	int		numParams;		/* number of parameters passed to query */

							/* for storing result tuples */
	int		next_tuple;		/* index of next one to return */
	Relation	resultRel;		/* relcache entry for the target relation */
	AttrNumber	*attnoMap;		/* array of attnums of input user columns */
};


/*
 * This enum describes what's kept in the fdw_private list for a ForeignPath.
 * We store:
 *
 * 1) Boolean flag showing if the remote query has the final sort
 * 2) Boolean flag showing if the remote query has the LIMIT clause
 */
enum FdwPathPrivateIndex
{
	/* has-final-sort flag (as an integer Value node) */
	FdwPathPrivateHasFinalSort,
	/* has-limit flag (as an integer Value node) */
	FdwPathPrivateHasLimit
};

/* Struct for extra information passed to estimate_path_cost_size() */
typedef struct
{
	PathTarget *target;
	bool		has_final_sort;
	bool		has_limit;
	double		limit_tuples;
	int64		count_est;
	int64		offset_est;
} OracleFdwPathExtraData;

/*
 * SQL functions
 */
extern PGDLLEXPORT Datum oracle_fdw_handler(PG_FUNCTION_ARGS);
extern PGDLLEXPORT Datum oracle_fdw_validator(PG_FUNCTION_ARGS);
extern PGDLLEXPORT Datum oracle_close_connections(PG_FUNCTION_ARGS);
extern PGDLLEXPORT Datum oracle_diag(PG_FUNCTION_ARGS);
extern PGDLLEXPORT Datum oracle_execute(PG_FUNCTION_ARGS);

PG_FUNCTION_INFO_V1(oracle_fdw_handler);
PG_FUNCTION_INFO_V1(oracle_fdw_validator);
PG_FUNCTION_INFO_V1(oracle_close_connections);
PG_FUNCTION_INFO_V1(oracle_diag);
PG_FUNCTION_INFO_V1(oracle_execute);

/*
 * on-load initializer
 */
extern PGDLLEXPORT void _PG_init(void);

/*
 * FDW callback routines
 */
static void oracleGetForeignRelSize(PlannerInfo *root, RelOptInfo *baserel, Oid foreigntableid);
static void oracleGetForeignPaths(PlannerInfo *root, RelOptInfo *baserel, Oid foreigntableid);
#ifdef JOIN_API
static void oracleGetForeignJoinPaths(PlannerInfo *root, RelOptInfo *joinrel, RelOptInfo *outerrel, RelOptInfo *innerrel, JoinType jointype, JoinPathExtraData *extra);
#endif  /* JOIN_API */
static ForeignScan *oracleGetForeignPlan(PlannerInfo *root, RelOptInfo *foreignrel, Oid foreigntableid, ForeignPath *best_path, List *tlist, List *scan_clauses
#if PG_VERSION_NUM >= 90500
, Plan *outer_plan
#endif  /* PG_VERSION_NUM */
);
static bool oracleAnalyzeForeignTable(Relation relation, AcquireSampleRowsFunc *func, BlockNumber *totalpages);
static void oracleExplainForeignScan(ForeignScanState *node, ExplainState *es);
static void oracleBeginForeignScan(ForeignScanState *node, int eflags);
static TupleTableSlot *oracleIterateForeignScan(ForeignScanState *node);
static void oracleEndForeignScan(ForeignScanState *node);
static void oracleReScanForeignScan(ForeignScanState *node);
#if PG_VERSION_NUM < 140000
static void oracleAddForeignUpdateTargets(Query *parsetree, RangeTblEntry *target_rte, Relation target_relation);
#else
static void oracleAddForeignUpdateTargets(PlannerInfo *root, Index rtindex, RangeTblEntry *target_rte, Relation target_relation);
#endif
static List *oraclePlanForeignModify(PlannerInfo *root, ModifyTable *plan, Index resultRelation, int subplan_index);
static void oracleBeginForeignModify(ModifyTableState *mtstate, ResultRelInfo *rinfo, List *fdw_private, int subplan_index, int eflags);
#if PG_VERSION_NUM >= 110000
static void oracleBeginForeignInsert(ModifyTableState *mtstate, ResultRelInfo *rinfo);
static void oracleEndForeignInsert(EState *estate, ResultRelInfo *rinfo);
#endif  /*PG_VERSION_NUM */
static TupleTableSlot *oracleExecForeignInsert(EState *estate, ResultRelInfo *rinfo, TupleTableSlot *slot, TupleTableSlot *planSlot);
static TupleTableSlot *oracleExecForeignUpdate(EState *estate, ResultRelInfo *rinfo, TupleTableSlot *slot, TupleTableSlot *planSlot);
static TupleTableSlot *oracleExecForeignDelete(EState *estate, ResultRelInfo *rinfo, TupleTableSlot *slot, TupleTableSlot *planSlot);
static void oracleEndForeignModify(EState *estate, ResultRelInfo *rinfo);
static void oracleExplainForeignModify(ModifyTableState *mtstate, ResultRelInfo *rinfo, List *fdw_private, int subplan_index, struct ExplainState *es);
static int oracleIsForeignRelUpdatable(Relation rel);
#ifdef IMPORT_API
static List *oracleImportForeignSchema(ImportForeignSchemaStmt *stmt, Oid serverOid);
#endif  /* IMPORT_API */
static void oracleGetForeignUpperPaths(PlannerInfo *root,
										 UpperRelationKind stage,
										 RelOptInfo *input_rel,
										 RelOptInfo *output_rel,
										 void *extra);

static bool oraclePlanDirectModify(PlannerInfo *root,
									 ModifyTable *plan,
									 Index resultRelation,
									 int subplan_index);

static void oracleBeginDirectModify(ForeignScanState *node, int eflags);
static TupleTableSlot *oracleIterateDirectModify(ForeignScanState *node);
static void oracleEndDirectModify(ForeignScanState *node);
static void oracleExplainDirectModify(ForeignScanState *node,
										ExplainState *es);

/*
 * Helper functions
 */
static struct OracleFdwState *getFdwState(Oid foreigntableid, double *sample_percent, Oid userid);
static void oracleGetOptions(Oid foreigntableid, Oid userid, List **options);
static void deparseFromExprForRel(StringInfo buf, RelOptInfo *joinrel, List **params_list, deparse_expr_cxt *context);
#ifdef JOIN_API
static void appendConditions(List *exprs, deparse_expr_cxt *context);
static bool foreign_join_ok(PlannerInfo *root, RelOptInfo *joinrel, JoinType jointype, RelOptInfo *outerrel, RelOptInfo *innerrel, JoinPathExtraData *extra);
static const char *get_jointype_name(JoinType jointype);
static List *build_tlist_to_deparse(RelOptInfo *foreignrel);
#endif  /* JOIN_API */
static void getColumnData(Oid foreigntableid, struct oraTable *oraTable);
static void getColumnDataByTupdesc(Relation rel, TupleDesc tupdesc, List *retrieved_attrs, struct oraTable *oraTable);
static int acquireSampleRowsFunc (Relation relation, int elevel, HeapTuple *rows, int targrows, double *totalrows, double *totaldeadrows);
static void appendAsType(StringInfoData *dest, const char *s, Oid type);
static void castNullAsType(StringInfoData *dest, Oid type);
static char *deparseExpr(Expr *expr, deparse_expr_cxt *context);
static char *datumToString(Datum datum, Oid type);
static void getUsedColumns(Expr *expr, struct oraTable *oraTable, int foreignrelid);
static void checkDataType(oraType oratype, int scale, Oid pgtype, const char *tablename, const char *colname);
static char *deparseWhereConditions(struct OracleFdwState *fdwState, PlannerInfo *root, RelOptInfo *baserel, List **local_conds, List **remote_conds);
static char *guessNlsLang(char *nls_lang);
static oracleSession *oracleConnectServer(Name srvname);
static List *serializePlanData(struct OracleFdwState *fdwState);
static Const *serializeString(const char *s);
static Const *serializeLong(long i);
static struct OracleFdwState *deserializePlanData(List *list);
static char *deserializeString(Const *constant);
static long deserializeLong(Const *constant);
static bool optionIsTrue(const char *value);
#if PG_VERSION_NUM < 130000
/* this function is not exported before v13 */
static Expr *find_em_expr_for_rel(EquivalenceClass *ec, RelOptInfo *rel);
#endif  /* PG_VERSION_NUM */
static char *deparseDate(Datum datum);
static char *deparseTimestamp(Datum datum, bool hasTimezone);
static char *deparseInterval(Datum datum);
static char *convertUUID(char *uuid);
static void subtransactionCallback(SubXactEvent event, SubTransactionId mySubid, SubTransactionId parentSubid, void *arg);
static void addParam(struct paramDesc **paramList, char *name, Oid pgtype, oraType oratype, int colnum);
static void setModifyParameters(struct paramDesc *paramList, TupleTableSlot *newslot, TupleTableSlot *oldslot, struct oraTable *oraTable, oracleSession *session);
static void transactionCallback(XactEvent event, void *arg);
static void exitHook(int code, Datum arg);
static void oracleDie(SIGNAL_ARGS);
static char *setSelectParameters(struct paramDesc *paramList, ExprContext *econtext);
static void convertTuple(struct OracleFdwState *fdw_state, Datum *values, bool *nulls, bool trunc_lob);
static void errorContextCallback(void *arg);
static bool hasTrigger(Relation rel, CmdType cmdtype);
static void buildInsertQuery(StringInfo sql, struct OracleFdwState *fdwState);
static void buildUpdateQuery(StringInfo sql, struct OracleFdwState *fdwState, List *targetAttrs);
static void appendReturningClause(StringInfo sql, struct OracleFdwState *fdwState);
#ifdef IMPORT_API
static char *fold_case(char *name, fold_t foldcase, int collation);
#endif  /* IMPORT_API */
static oraIsoLevel getIsolationLevel(const char *isolation_level);
static char *deparseLimit(PlannerInfo *root, struct OracleFdwState *fdwState);

static void initializeContext(struct OracleFdwState *fdwState,
									 PlannerInfo *root,
									 RelOptInfo *foreignrel,
									 RelOptInfo *scanrel,
									 deparse_expr_cxt *context);

static Expr *find_em_expr_for_input_target(PlannerInfo *root,
										   EquivalenceClass *ec,
										   PathTarget *target);

static List *get_useful_pathkeys_for_relation(PlannerInfo *root,
											  RelOptInfo *rel);
static void add_paths_with_pathkeys_for_rel(PlannerInfo *root, RelOptInfo *rel,
											Path *epq_path);
static TupleDesc
get_tupdesc_for_join_scan_tuples(ForeignScanState *node);

static void add_foreign_grouping_paths(PlannerInfo *root,
									   RelOptInfo *input_rel,
									   RelOptInfo *grouped_rel,
									   GroupPathExtraData *extra);

static void add_foreign_ordered_paths(PlannerInfo *root,
									  RelOptInfo *input_rel,
									  RelOptInfo *ordered_rel);

static void add_foreign_final_paths(PlannerInfo *root,
									RelOptInfo *input_rel,
									RelOptInfo *final_rel,
									FinalPathExtraData *extra);



static void merge_fdw_state(struct OracleFdwState * fpinfo,
							  const struct OracleFdwState * fpinfo_o,
							  const struct OracleFdwState * fpinfo_i);

static bool foreign_grouping_ok(PlannerInfo *root, RelOptInfo *grouped_rel,
								Node *havingQual);
static bool is_foreign_param(PlannerInfo *root, RelOptInfo *baserel, Expr *expr);

static void adjust_foreign_grouping_path_cost(PlannerInfo *root,
											  List *pathkeys,
											  double retrieved_rows,
											  double width,
											  double limit_tuples,
											  Cost *p_startup_cost,
											  Cost *p_run_cost);

static void estimate_path_cost_size(PlannerInfo *root,
									RelOptInfo *foreignrel,
									List *param_join_conds,
									List *pathkeys,
									OracleFdwPathExtraData *fpextra,
									double *p_rows, int *p_width,
									Cost *p_startup_cost, Cost *p_total_cost);

static bool exist_in_function_list(char *funcname, const char **funclist);

static void
oracleDeparseSelectStmtForRel(StringInfo buf, PlannerInfo *root, RelOptInfo *rel,
						List *tlist, List *remote_conds, bool for_update, List *pathkeys,
						bool has_final_sort, bool has_limit, bool is_subquery,
						List **retrieved_attrs, List **params_list);


static void
oracleDeparseSelectSql(List *tlist, bool is_subquery, List **retrieved_attrs,
				 deparse_expr_cxt *context);

static void
oracleDeparseSubqueryTargetList(deparse_expr_cxt *context);

static void
oracleDeparseExplicitTargetList(List *tlist,
						  bool is_returning,
						  List **retrieved_attrs,
						  deparse_expr_cxt *context);

static
void oracleDeparseReturningList(struct oraTable *oraTable, StringInfo buf, RangeTblEntry *rte,
								 Index rtindex, Relation rel,
								 bool trig_after_row,
								 List *withCheckOptionList,
								 List *returningList,
								 List **retrieved_attrs);

static void
oracleDeparseTargetList(struct oraTable *oraTable, StringInfo buf,
				  RangeTblEntry *rte,
				  Index rtindex,
				  Relation rel,
				  bool is_returning,
				  Bitmapset *attrs_used,
				  bool qualify_col,
				  List **retrieved_attrs);

static void
oracleDeparseColumnRef(struct oraTable *oraTable, StringInfo buf, int varno, int varattno, bool qualify_col);

static void
oracleDeparseFromExpr(struct OracleFdwState *fdwState, List *quals, deparse_expr_cxt *context);

static void
oracleDeparseRangeTblRef(StringInfo buf, RelOptInfo *foreignrel,
				   bool make_subquery, List **params_list, deparse_expr_cxt *context);
static void
oracleAppendGroupByClause(List *tlist, deparse_expr_cxt *context);

static char *
oracleAppendAggOrderBy(List *orderList, List *targetList,
							 deparse_expr_cxt *context);
static Node *
oracleDeparseSortGroupClause(Index ref, List *tlist,
					   deparse_expr_cxt *context);

static char *
oracleDeparseAggref(Aggref *node, deparse_expr_cxt *context);

static void
oracleAppendOrderByClause(List *pathkeys, bool has_final_sort,
					deparse_expr_cxt *context);

static char *oracleCreateQuery(char *tablename);

static int	set_transmission_modes(void);
static void reset_transmission_modes(int nestlevel);
static struct oraTable *getOraTableFromJoinRel(Var *variable, RelOptInfo *foreignrel);

static char *
oracleDeparseConcat(List *args, deparse_expr_cxt *context);

static bool
oracle_contain_functions_walker(Node *node, void *context);
static bool oracle_is_foreign_function_tlist(PlannerInfo *root,
											RelOptInfo *baserel,
											List *tlist);

static int	set_transmission_modes(void);
static void reset_transmission_modes(int nestlevel);

static char *oracle_replace_function(char *in);
static bool starts_with(const char *pre, const char *str);

#if PG_VERSION_NUM >= 140000
static ForeignScan *find_modifytable_subplan(PlannerInfo *root,
						 ModifyTable *plan,
						 Index rtindex,
						 int subplan_index);
#endif

static List *build_remote_returning(Index rtindex, Relation rel, List *returningList);
static void rebuild_fdw_scan_tlist(ForeignScan *fscan, List *tlist);

static void oracleDeparseDirectUpdateSql(StringInfo buf, PlannerInfo *root,
								   Index rtindex, Relation rel,
								   RelOptInfo *foreignrel,
								   List *targetlist,
								   List *targetAttrs,
								   List *remote_conds,
								   List **params_list,
								   List *returningList,
								   List **retrieved_attrs);

static void oracleDeparseDirectDeleteSql(StringInfo buf, PlannerInfo *root,
								   Index rtindex, Relation rel,
								   RelOptInfo *foreignrel,
								   List *remote_conds,
								   List **params_list,
								   List *returningList,
								   List **retrieved_attrs);

static void init_returning_filter(struct OracleFdwState *dmstate,
								  List *fdw_scan_tlist,
								  Index rtindex);
static TupleTableSlot *apply_returning_filter(struct OracleFdwState *dmstate,
											  ResultRelInfo *resultRelInfo,
											  TupleTableSlot *slot,
											  EState *estate);

static void prepare_query_params(struct OracleFdwState *fdw_state,
								 PlanState *node,
								 List *fdw_exprs,
								 int numParams);

static void execute_dml_stmt(ForeignScanState *node);
static TupleTableSlot *get_returning_data(ForeignScanState *node);

#define REL_ALIAS_PREFIX    "r"
/* Handy macro to add relation name qualification */
#define ADD_REL_QUALIFIER(buf, varno)   \
		appendStringInfo((buf), "%s%d.", REL_ALIAS_PREFIX, (varno))
#define SUBQUERY_REL_ALIAS_PREFIX	"s"
#define SUBQUERY_COL_ALIAS_PREFIX	"c"

/*
 * Foreign-data wrapper handler function: return a struct with pointers
 * to callback routines.
 */
PGDLLEXPORT Datum
oracle_fdw_handler(PG_FUNCTION_ARGS)
{
	FdwRoutine *fdwroutine = makeNode(FdwRoutine);

	fdwroutine->GetForeignRelSize = oracleGetForeignRelSize;
	fdwroutine->GetForeignPaths = oracleGetForeignPaths;
#ifdef JOIN_API
	fdwroutine->GetForeignJoinPaths = oracleGetForeignJoinPaths;
#endif  /* JOIN_API */
	fdwroutine->GetForeignPlan = oracleGetForeignPlan;
	fdwroutine->AnalyzeForeignTable = oracleAnalyzeForeignTable;
	fdwroutine->ExplainForeignScan = oracleExplainForeignScan;
	fdwroutine->BeginForeignScan = oracleBeginForeignScan;
	fdwroutine->IterateForeignScan = oracleIterateForeignScan;
	fdwroutine->ReScanForeignScan = oracleReScanForeignScan;
	fdwroutine->EndForeignScan = oracleEndForeignScan;
	fdwroutine->AddForeignUpdateTargets = oracleAddForeignUpdateTargets;
	fdwroutine->PlanForeignModify = oraclePlanForeignModify;
	fdwroutine->BeginForeignModify = oracleBeginForeignModify;
#if PG_VERSION_NUM >= 110000
	fdwroutine->BeginForeignInsert = oracleBeginForeignInsert;
	fdwroutine->EndForeignInsert = oracleEndForeignInsert;
#endif  /*PG_VERSION_NUM */
	fdwroutine->ExecForeignInsert = oracleExecForeignInsert;
	fdwroutine->ExecForeignUpdate = oracleExecForeignUpdate;
	fdwroutine->ExecForeignDelete = oracleExecForeignDelete;
	fdwroutine->EndForeignModify = oracleEndForeignModify;
	fdwroutine->ExplainForeignModify = oracleExplainForeignModify;
	fdwroutine->IsForeignRelUpdatable = oracleIsForeignRelUpdatable;
#ifdef IMPORT_API
	fdwroutine->ImportForeignSchema = oracleImportForeignSchema;
#endif  /* IMPORT_API */

	/* Support functions for upper relation push-down */
	fdwroutine->GetForeignUpperPaths = oracleGetForeignUpperPaths;

	/* Support direct modification */
	fdwroutine->PlanDirectModify = oraclePlanDirectModify;
	fdwroutine->BeginDirectModify = oracleBeginDirectModify;
	fdwroutine->IterateDirectModify = oracleIterateDirectModify;
	fdwroutine->EndDirectModify = oracleEndDirectModify;
	fdwroutine->ExplainDirectModify = oracleExplainDirectModify;

	PG_RETURN_POINTER(fdwroutine);
}

/*
 * oracle_fdw_validator
 * 		Validate the generic options given to a FOREIGN DATA WRAPPER, SERVER,
 * 		USER MAPPING or FOREIGN TABLE that uses oracle_fdw.
 *
 * 		Raise an ERROR if the option or its value are considered invalid
 * 		or a required option is missing.
 */
PGDLLEXPORT Datum
oracle_fdw_validator(PG_FUNCTION_ARGS)
{
	List *options_list = untransformRelOptions(PG_GETARG_DATUM(0));
	Oid catalog = PG_GETARG_OID(1);
	ListCell *cell;
	bool option_given[option_count] = { false };
	int i;

	/*
	 * Check that only options supported by oracle_fdw, and allowed for the
	 * current object type, are given.
	 */

	foreach(cell, options_list)
	{
		DefElem *def = (DefElem *)lfirst(cell);
		bool opt_found = false;

		/* search for the option in the list of valid options */
		for (i=0; i<option_count; ++i)
		{
			if (catalog == valid_options[i].optcontext && strcmp(valid_options[i].optname, def->defname) == 0)
			{
				opt_found = true;
				option_given[i] = true;
				break;
			}
		}

		/* option not found, generate error message */
		if (!opt_found)
		{
			/* generate list of options */
			StringInfoData buf;
			initStringInfo(&buf);
			for (i=0; i<option_count; ++i)
			{
				if (catalog == valid_options[i].optcontext)
					appendStringInfo(&buf, "%s%s", (buf.len > 0) ? ", " : "",  valid_options[i].optname);
			}

			ereport(ERROR,
					(errcode(ERRCODE_FDW_INVALID_OPTION_NAME),
					errmsg("invalid option \"%s\"", def->defname),
					errhint("Valid options in this context are: %s", buf.data)));
		}

		/* check valid values for "isolation_level" */
		if (strcmp(def->defname, OPT_ISOLATION_LEVEL) == 0)
			(void)getIsolationLevel(strVal(def->arg));

		/* check valid values for "readonly", "key", "strip_zeros" and "nchar" */
		if (strcmp(def->defname, OPT_READONLY) == 0
				|| strcmp(def->defname, OPT_KEY) == 0
				|| strcmp(def->defname, OPT_STRIP_ZEROS) == 0
				|| strcmp(def->defname, OPT_NCHAR) == 0
			)
		{
			char *val = strVal(def->arg);
			if (pg_strcasecmp(val, "on") != 0
					&& pg_strcasecmp(val, "off") != 0
					&& pg_strcasecmp(val, "yes") != 0
					&& pg_strcasecmp(val, "no") != 0
					&& pg_strcasecmp(val, "true") != 0
					&& pg_strcasecmp(val, "false") != 0)
				ereport(ERROR,
						(errcode(ERRCODE_FDW_INVALID_ATTRIBUTE_VALUE),
						errmsg("invalid value for option \"%s\"", def->defname),
						errhint("Valid values in this context are: on/yes/true or off/no/false")));
		}

		/* check valid values for "dblink" */
		if (strcmp(def->defname, OPT_DBLINK) == 0)
		{
			char *val = strVal(def->arg);
			if (strchr(val, '"') != NULL)
				ereport(ERROR,
						(errcode(ERRCODE_FDW_INVALID_ATTRIBUTE_VALUE),
						errmsg("invalid value for option \"%s\"", def->defname),
						errhint("Double quotes are not allowed in the dblink name.")));
		}

		/* check valid values for "schema" */
		if (strcmp(def->defname, OPT_SCHEMA) == 0)
		{
			char *val = strVal(def->arg);
			if (strchr(val, '"') != NULL)
				ereport(ERROR,
						(errcode(ERRCODE_FDW_INVALID_ATTRIBUTE_VALUE),
						errmsg("invalid value for option \"%s\"", def->defname),
						errhint("Double quotes are not allowed in the schema name.")));
		}

		/* check valid values for max_long */
		if (strcmp(def->defname, OPT_MAX_LONG) == 0)
		{
			char *val = strVal(def->arg);
			char *endptr;
			unsigned long max_long;

			errno = 0;
			max_long = strtoul(val, &endptr, 0);
			if (val[0] == '\0' || *endptr != '\0' || errno != 0 || max_long < 1 || max_long > 1073741823ul)
				ereport(ERROR,
						(errcode(ERRCODE_FDW_INVALID_ATTRIBUTE_VALUE),
						errmsg("invalid value for option \"%s\"", def->defname),
						errhint("Valid values in this context are integers between 1 and 1073741823.")));
		}

		/* check valid values for "sample_percent" */
		if (strcmp(def->defname, OPT_SAMPLE) == 0)
		{
			char *val = strVal(def->arg);
			char *endptr;
			double sample_percent;

			errno = 0;
			sample_percent = strtod(val, &endptr);
			if (val[0] == '\0' || *endptr != '\0' || errno != 0 || sample_percent < 0.000001 || sample_percent > 100.0)
				ereport(ERROR,
						(errcode(ERRCODE_FDW_INVALID_ATTRIBUTE_VALUE),
						errmsg("invalid value for option \"%s\"", def->defname),
						errhint("Valid values in this context are numbers between 0.000001 and 100.")));
		}

		/* check valid values for "prefetch" */
		if (strcmp(def->defname, OPT_PREFETCH) == 0)
		{
			char *val = strVal(def->arg);
			char *endptr;
			long prefetch;

			errno = 0;
			prefetch = strtol(val, &endptr, 0);
			if (val[0] == '\0' || *endptr != '\0' || errno != 0 || prefetch < 0 || prefetch > 10240 )
				ereport(ERROR,
						(errcode(ERRCODE_FDW_INVALID_ATTRIBUTE_VALUE),
						errmsg("invalid value for option \"%s\"", def->defname),
						errhint("Valid values in this context are integers between 0 and 10240.")));
		}
	}

	/* check that all required options have been given */
	for (i=0; i<option_count; ++i)
	{
		if (catalog == valid_options[i].optcontext && valid_options[i].optrequired && !option_given[i])
		{
			ereport(ERROR,
					(errcode(ERRCODE_FDW_OPTION_NAME_NOT_FOUND),
					errmsg("missing required option \"%s\"", valid_options[i].optname)));
		}
	}

	PG_RETURN_VOID();
}

/*
 * oracle_close_connections
 * 		Close all open Oracle connections.
 */
PGDLLEXPORT Datum
oracle_close_connections(PG_FUNCTION_ARGS)
{
	if (dml_in_transaction)
		ereport(ERROR,
				(errcode(ERRCODE_ACTIVE_SQL_TRANSACTION),
				errmsg("connections with an active transaction cannot be closed"),
				errhint("The transaction that modified Oracle data must be closed first.")));

	elog(DEBUG1, "oracle_fdw: close all Oracle connections");
	oracleCloseConnections();

	PG_RETURN_VOID();
}

/*
 * oracle_diag
 * 		Get the Oracle client version.
 * 		If a non-NULL argument is supplied, it must be a foreign server name.
 * 		In this case, the remote server version is returned as well.
 */
PGDLLEXPORT Datum
oracle_diag(PG_FUNCTION_ARGS)
{
	char *pgversion;
	int major, minor, update, patch, port_patch;
	StringInfoData version;

	/*
	 * Get the PostgreSQL server version.
	 * We cannot use PG_VERSION because that would give the version against which
	 * oracle_fdw was compiled, not the version it is running with.
	 */
	pgversion = GetConfigOptionByName("server_version", NULL);

	/* get the Oracle client version */
	oracleClientVersion(&major, &minor, &update, &patch, &port_patch);

	initStringInfo(&version);
	appendStringInfo(&version, "oracle_fdw %s, PostgreSQL %s, Oracle client %d.%d.%d.%d.%d",
					ORACLE_FDW_VERSION,
					pgversion,
					major, minor, update, patch, port_patch);

	if (PG_ARGISNULL(0))
	{
		/* display some important Oracle environment variables */
		static const char * const oracle_env[] = {
			"ORACLE_HOME",
			"ORACLE_SID",
			"TNS_ADMIN",
			"TWO_TASK",
			"LDAP_ADMIN",
			NULL
		};
		int i;

		for (i=0; oracle_env[i] != NULL; ++i)
		{
			char *val = getenv(oracle_env[i]);

			if (val != NULL)
				appendStringInfo(&version, ", %s=%s", oracle_env[i], val);
		}
	}
	else
	{
		oracleSession *session;

		Name srvname = PG_GETARG_NAME(0);
		session = oracleConnectServer(srvname);

		/* get the server version */
		oracleServerVersion(session, &major, &minor, &update, &patch, &port_patch);
		appendStringInfo(&version, ", Oracle server %d.%d.%d.%d.%d",
						major, minor, update, patch, port_patch);

		/* free the session (connection will be cached) */
		pfree(session);
	}

	PG_RETURN_TEXT_P(cstring_to_text(version.data));
}

/*
 * oracle_execute
 * 		Execute a statement that returns no result values on a foreign server.
 */
PGDLLEXPORT Datum
oracle_execute(PG_FUNCTION_ARGS)
{
	Name srvname = PG_GETARG_NAME(0);
	char *stmt = text_to_cstring(PG_GETARG_TEXT_PP(1));
	oracleSession *session = oracleConnectServer(srvname);

	oracleExecuteCall(session, stmt);

	/* free the session (connection will be cached) */
	pfree(session);

	PG_RETURN_VOID();
}

/*
 * _PG_init
 * 		Library load-time initalization.
 * 		Sets exitHook() callback for backend shutdown.
 */
void
_PG_init(void)
{
	/* check for incompatible server versions */
	char *pgver_str = GetConfigOptionByName("server_version_num", NULL);
	long pgver = strtol(pgver_str, NULL, 10);

	pfree(pgver_str);

	if ((pgver >= 90600 && pgver <= 90608)
			|| (pgver >= 100000 && pgver <= 100003))
		ereport(ERROR,
				(errcode(ERRCODE_EXTERNAL_ROUTINE_INVOCATION_EXCEPTION),
				errmsg("PostgreSQL version \"%s\" not supported by oracle_fdw",
					   GetConfigOptionByName("server_version", NULL)),
				errhint("You'll have to update PostgreSQL to a later minor release.")));

	/* register an exit hook */
	on_proc_exit(&exitHook, PointerGetDatum(NULL));
}

/*
 * oracleGetForeignRelSize
 * 		Get an OracleFdwState for this foreign scan.
 * 		Construct the remote SQL query.
 * 		Provide estimates for the number of tuples, the average width and the cost.
 */
void
oracleGetForeignRelSize(PlannerInfo *root, RelOptInfo *baserel, Oid foreigntableid)
{
	struct OracleFdwState *fdwState;
	int i, major, minor, update, patch, port_patch;
	double ntuples = -1;
	RangeTblEntry *rte = planner_rt_fetch(baserel->relid, root);
	ListCell *lc;

	elog(DEBUG1, "oracle_fdw: plan foreign table scan");

	/*
	 * Get connection options, connect and get the remote table description.
	 * To match what ExecCheckRTEPerms does, pass the user whose user mapping
	 * should be used (if invalid, the current user is used).
	 */
	fdwState = getFdwState(foreigntableid, NULL, rte->checkAsUser);

	/*
	 * Store the table OID in each table column.
	 * This is redundant for base relations, but join relations will
	 * have columns from different tables, and we have to keep track of them.
	 */
	for (i=0; i<fdwState->oraTable->ncols; ++i){
		fdwState->oraTable->cols[i]->varno = baserel->relid;
	}

	/*
	 * Classify conditions into remote_conds or local_conds.
	 */
	deparseWhereConditions(fdwState, root, baserel,
						   &(fdwState->local_conds),
						   &(fdwState->remote_conds));

	/* try to push down LIMIT from Oracle 12.2 on */
	oracleServerVersion(fdwState->session, &major, &minor, &update, &patch, &port_patch);
	if (major > 12 || (major == 12 && minor > 1))
	{
		if ((list_length(root->canon_pathkeys) <= 1 && !root->cte_plan_ids)
				||  (list_length(root->parse->rtable) == 1))
		{
			fdwState->limit_clause = deparseLimit(root, fdwState);
		}
	}

	/* release Oracle session (will be cached) */
	pfree(fdwState->session);
	fdwState->session = NULL;

	/* use a random "high" value for cost */
	fdwState->startup_cost = 10000.0;

	/* if baserel->pages > 0, there was an ANALYZE; use the row count estimate */
#if PG_VERSION_NUM < 140000
	/* before v14, baserel->tuples == 0 for tables that have never been vacuumed */
	if (baserel->pages > 0)
#endif  /* PG_VERSION_NUM */
		ntuples = baserel->tuples;

	/* estimale selectivity locally for all conditions */

	/* apply statistics only if we have a reasonable row count estimate */
	if (ntuples != -1)
	{
		/* estimate how conditions will influence the row count */
		ntuples = ntuples * clauselist_selectivity(root, baserel->baserestrictinfo, 0, JOIN_INNER, NULL);
		/* make sure that the estimate is not less that 1 */
		ntuples = clamp_row_est(ntuples);
		baserel->rows = ntuples;
	}

	/* estimate total cost as startup cost + 10 * (returned rows) */
	fdwState->total_cost = fdwState->startup_cost + baserel->rows * 10.0;

	/* store the state so that the other planning functions can use it */
	baserel->fdw_private = (void *)fdwState;

	/* Base foreign tables need to be pushed down always. */
	fdwState->pushdown_safe = true;

	/* Look up foreign-table catalog info. */
	fdwState->table = GetForeignTable(foreigntableid);
	fdwState->server = GetForeignServer(fdwState->table->serverid);

	/*
	 * Extract user-settable option values.  Note that per-table settings of
	 * use_remote_estimate, fetch_size and async_capable override per-server
	 * settings of them, respectively.
	 */
	fdwState->fdw_startup_cost = DEFAULT_FDW_STARTUP_COST;
	fdwState->fdw_tuple_cost = DEFAULT_FDW_TUPLE_COST;

	/*
	 * Identify which attributes will need to be retrieved from the remote
	 * server.  These include all attrs needed for joins or final output, plus
	 * all attrs used in the local_conds.  (Note: if we end up using a
	 * parameterized scan, it's possible that some of the join clauses will be
	 * sent to the remote and thus we wouldn't really need to retrieve the
	 * columns used in them.  Doesn't seem worth detecting that case though.)
	 */
	fdwState->attrs_used = NULL;
	pull_varattnos((Node *) baserel->reltarget->exprs, baserel->relid,
				   &fdwState->attrs_used);
	foreach(lc, fdwState->local_conds)
	{
		RestrictInfo *rinfo = lfirst_node(RestrictInfo, lc);

		pull_varattnos((Node *) rinfo->clause, baserel->relid,
					   &fdwState->attrs_used);
	}

	/*
	 * Set # of retrieved rows and cached relation costs to some negative
	 * value, so that we can detect when they are set to some sensible values,
	 * during one (usually the first) of the calls to estimate_path_cost_size.
	 */
	fdwState->retrieved_rows = -1;
	fdwState->rel_startup_cost = -1;
	fdwState->rel_total_cost = -1;

	/*
	 * fdwState->relation_name gets the numeric rangetable index of the foreign
	 * table RTE.  (If this query gets EXPLAIN'd, we'll convert that to a
	 * human-readable string at that time.)
	 */
	fdwState->relation_name = psprintf("%u", baserel->relid);

	/* No outer and inner relations. */
	fdwState->make_outerrel_subquery = false;
	fdwState->make_innerrel_subquery = false;
	fdwState->lower_subquery_rels = NULL;
	/* Set the relation index. */
	fdwState->relation_index = baserel->relid;
}

/* oracleGetForeignPaths
 * 		Create a ForeignPath node and add it as only possible path.
 */
void
oracleGetForeignPaths(PlannerInfo *root, RelOptInfo *baserel, Oid foreigntableid)
{
	struct OracleFdwState *fdwState = (struct OracleFdwState *)baserel->fdw_private;

	/* add the only path */
	add_path(baserel,
		(Path *)create_foreignscan_path(
					root,
					baserel,
#if PG_VERSION_NUM >= 90600
					NULL,  /* default pathtarget */
#endif  /* PG_VERSION_NUM */
					baserel->rows,
					fdwState->startup_cost,
					fdwState->total_cost,
					NIL, /* no pathkeys */
					baserel->lateral_relids,
#if PG_VERSION_NUM >= 90500
					NULL,  /* no extra plan */
#endif  /* PG_VERSION_NUM */
					NIL
			)
	);

	/* Add paths with pathkeys */
	add_paths_with_pathkeys_for_rel(root, baserel, NULL);

}

#ifdef JOIN_API
/*
 * oracleGetForeignJoinPaths
 * 		Add possible ForeignPath to joinrel if the join is safe to push down.
 * 		For now, we can only push down 2-way joins for SELECT.
 */
static void
oracleGetForeignJoinPaths(PlannerInfo *root,
							RelOptInfo *joinrel,
							RelOptInfo *outerrel,
							RelOptInfo *innerrel,
							JoinType jointype,
							JoinPathExtraData *extra)
{
	struct OracleFdwState *fdwState;
	ForeignPath *joinpath;
	double      joinclauses_selectivity;
	double      rows;				/* estimated number of returned rows */
	Cost        startup_cost;
	Cost        total_cost;

	/*
	 * Currently we don't push-down joins in query for UPDATE/DELETE.
	 * This would require a path for EvalPlanQual.
	 * This restriction might be relaxed in a later release.
	 */
	if (root->parse->commandType != CMD_SELECT)
	{
		elog(DEBUG2, "oracle_fdw: don't push down join because it is no SELECT");
		return;
	}

	if (root->rowMarks)
	{
		elog(DEBUG2, "oracle_fdw: don't push down join with FOR UPDATE");
		return;
	}

	/*
	 * N-way join is not supported, due to the column definition infrastracture.
	 * If we can track relid mapping of join relations, we can support N-way join.
	 */
	if (! IS_SIMPLE_REL(outerrel) || ! IS_SIMPLE_REL(innerrel))
		return;

	/* skip if this join combination has been considered already */
	if (joinrel->fdw_private)
		return;

	/*
	 * This code does not work for joins with lateral references, since those
	 * must have parameterized paths, which we don't generate yet.
	 */
	if (!bms_is_empty(joinrel->lateral_relids))
		return;

	/*
	 * Create unfinished OracleFdwState which is used to indicate
	 * that the join relation has already been considered, so that we won't waste
	 * time considering it again and don't add the same path a second time.
	 * Once we know that this join can be pushed down, we fill the data structure.
	 */
	fdwState = (struct OracleFdwState *) palloc0(sizeof(struct OracleFdwState));

	joinrel->fdw_private = fdwState;

	fdwState->pushdown_safe = false;

	/* attrs_used is only for base relations. */
	fdwState->attrs_used = NULL;

	/* this performs further checks */
	if (!foreign_join_ok(root, joinrel, jointype, outerrel, innerrel, extra))
	{
		return;
	}

	/* estimate the number of result rows for the join */
#if PG_VERSION_NUM < 140000
	if (outerrel->pages > 0 && innerrel->pages > 0)
#else
	if (outerrel->tuples >= 0 && innerrel->tuples >= 0)
#endif  /* PG_VERSION_NUM */
	{
		/* both relations have been ANALYZEd, so there should be useful statistics */
		joinclauses_selectivity = clauselist_selectivity(root, fdwState->joinclauses, 0, JOIN_INNER, extra->sjinfo);
		rows = clamp_row_est(innerrel->tuples * outerrel->tuples * joinclauses_selectivity);
	}
	else
	{
		/* at least one table lacks statistics, so use a fixed estimate */
		rows = 1000.0;
	}

	/* use a random "high" value for startup cost */
	startup_cost = 10000.0;

	/* estimate total cost as startup cost + (returned rows) * 10.0 */
	total_cost = startup_cost + rows * 10.0;

	/* store cost estimation results */
	joinrel->rows = rows;
	fdwState->startup_cost = startup_cost;
	fdwState->total_cost = total_cost;

	/* create a new join path */
#if PG_VERSION_NUM < 120000
	joinpath = create_foreignscan_path(
#else
	joinpath = create_foreign_join_path(
#endif
									   root,
									   joinrel,
									   NULL,	/* default pathtarget */
									   rows,
									   startup_cost,
									   total_cost,
									   NIL, 	/* no pathkeys */
									   joinrel->lateral_relids,
									   NULL,	/* no epq_path */
									   NIL);	/* no fdw_private */

	/* add generated path to joinrel */
	add_path(joinrel, (Path *) joinpath);

	/* Consider pathkeys for the join relation */
	add_paths_with_pathkeys_for_rel(root, joinrel, NULL);

}
#endif  /* JOIN_API */

/*****************************************************************************
 *		Check clauses for immutable functions
 *****************************************************************************/

/*
 * oracle_contain_functions
 * Recursively search for immutable, stable and volatile functions within a clause.
 *
 * Returns true if any function (or operator implemented by a function) is found.
 *
 * We will recursively look into TargetEntry exprs.
 */
static bool
oracle_contain_functions(Node *clause)
{
	return oracle_contain_functions_walker(clause, NULL);
}

static bool
oracle_contain_functions_walker(Node *node, void *context)
{
	if (node == NULL)
		return false;
	/* Check for functions in node itself */
	if (nodeTag(node) == T_FuncExpr ||
		nodeTag(node) == T_MinMaxExpr ||
		nodeTag(node) == T_CoalesceExpr ||
		nodeTag(node) == T_NullIfExpr)
	{
		return true;
	}

	/*
	 * It should be safe to treat MinMaxExpr as immutable, because it will
	 * depend on a non-cross-type btree comparison function, and those should
	 * always be immutable.  Treating XmlExpr as immutable is more dubious,
	 * and treating CoerceToDomain as immutable is outright dangerous.  But we
	 * have done so historically, and changing this would probably cause more
	 * problems than it would fix.  In practice, if you have a non-immutable
	 * domain constraint you are in for pain anyhow.
	 */

	/* Recurse to check arguments */
	if (IsA(node, Query))
	{
		/* Recurse into subselects */
		return query_tree_walker((Query *) node,
								 oracle_contain_functions_walker,
								 context, 0);
	}
	return expression_tree_walker(node, oracle_contain_functions_walker,
								  context);
}

/*
 * Returns true if given tlist is safe to evaluate on the foreign server.
 */
bool
oracle_is_foreign_function_tlist(PlannerInfo *root,
								RelOptInfo *baserel,
								List *tlist)
{
	struct OracleFdwState *fdwState = (struct OracleFdwState *)(baserel->fdw_private);
	ListCell   *lc;
	bool		is_contain_function = false;

	/*
	 * Check that the expression consists of any function.
	 */
	foreach(lc, tlist)
	{
		TargetEntry *tle = lfirst_node(TargetEntry, lc);

		if (oracle_contain_functions((Node *) tle->expr))
		{
			is_contain_function = true;
			break;
		}
	}

	if (!is_contain_function)
		return false;

	/*
	 * Check that the expression consists of nodes that are safe to execute
	 * remotely.
	 */
	foreach(lc, tlist)
	{
		deparse_expr_cxt context;
		TargetEntry *tle = lfirst_node(TargetEntry, lc);

		/* Initialize context */
		initializeContext(fdwState, root, baserel, baserel, &context);

		if (!deparseExpr((Expr *) tle->expr, &context))
			return false;

		/*
		* An expression which includes any mutable functions can't be sent
		* over because its result is not stable.  For example, sending now()
		* remote side could cause confusion from clock offsets.  Future
		* versions might be able to make this choice with more granularity.
		* (We check this last because it requires a lot of expensive catalog
		* lookups.)
		*/
		if (context.can_pushdown_function == false &&
			contain_mutable_functions((Node *) tle->expr))
		{
			return false;
		}
	}

	/* OK for the target list with functions to evaluate on the remote server */
	return true;
}

/*
 * oracleGetForeignPlan
 * 		Construct a ForeignScan node containing the serialized OracleFdwState,
 * 		the RestrictInfo clauses not handled entirely by Oracle and the list
 * 		of parameters we need for execution.
 * 		For join relations, the oraTable is constructed from the target list.
 */
ForeignScan
*oracleGetForeignPlan(PlannerInfo *root, RelOptInfo *foreignrel, Oid foreigntableid, ForeignPath *best_path, List *tlist, List *scan_clauses
#if PG_VERSION_NUM >= 90500
, Plan *outer_plan
#endif  /* PG_VERSION_NUM */
)
{
	struct OracleFdwState *fdwState = (struct OracleFdwState *)foreignrel->fdw_private;
	List *fdw_private = NIL;
	int i;
	bool need_keys = false, for_update = false, has_trigger;
	Relation rel;
	Index scan_relid;  /* will be 0 for join relations */
	List *local_exprs = NIL;
#if PG_VERSION_NUM >= 90500
	List *fdw_scan_tlist = NIL;
#endif  /* PG_VERSION_NUM */
	List *remote_exprs = NIL;
	StringInfoData sql;
	bool has_final_sort = false;
	bool has_limit = false;
	ListCell *lc;

	elog(DEBUG1, "oracle_fdw: get foreign plan");
	
	/* Decide to execute function pushdown support in the target list. */
	fdwState->is_tlist_func_pushdown = oracle_is_foreign_function_tlist(root, foreignrel, tlist);

	/*
	 * Get FDW private data created by oracleGetForeignUpperPaths(), if any.
	 */
	if (best_path->fdw_private)
	{
		has_final_sort = intVal(list_nth(best_path->fdw_private,
										 FdwPathPrivateHasFinalSort));
		has_limit = intVal(list_nth(best_path->fdw_private,
									FdwPathPrivateHasLimit));
	}

#ifdef JOIN_API
	/* treat base relations and join relations differently */
	if (IS_SIMPLE_REL(foreignrel))
	{
#endif  /* JOIN_API */
		deparse_expr_cxt context;

		/* Init context */
		initializeContext(fdwState, root, foreignrel, foreignrel, &context);

		/* for base relations, set scan_relid as the relid of the relation */
		scan_relid = foreignrel->relid;

		/*
		 * In a base-relation scan, we must apply the given scan_clauses.
		 *
		 * Separate the scan_clauses into those that can be executed remotely
		 * and those that can't.  baserestrictinfo clauses that were
		 * previously determined to be safe or unsafe by classifyConditions
		 * are found in fpinfo->remote_conds and fpinfo->local_conds. Anything
		 * else in the scan_clauses list will be a join clause, which we have
		 * to check for remote-safety.
		 *
		 * Note: the join clauses we see here should be the exact same ones
		 * previously examined by oracleGetForeignPaths.  Possibly it'd be
		 * worth passing forward the classification work done then, rather
		 * than repeating it here.
		 *
		 * This code must match "extract_actual_clauses(scan_clauses, false)"
		 * except for the additional decision about remote versus local
		 * execution.
		 */
		foreach(lc, scan_clauses)
		{
			RestrictInfo *rinfo = lfirst_node(RestrictInfo, lc);

			/* Ignore any pseudoconstants, they're dealt with elsewhere */
			if (rinfo->pseudoconstant)
				continue;

			if (list_member_ptr(fdwState->remote_conds, rinfo))
				remote_exprs = lappend(remote_exprs, rinfo->clause);
			else if (list_member_ptr(fdwState->local_conds, rinfo))
				local_exprs = lappend(local_exprs, rinfo->clause);
			else if (deparseExpr(rinfo->clause, &context))
				remote_exprs = lappend(remote_exprs, rinfo->clause);
			else
				local_exprs = lappend(local_exprs, rinfo->clause);
		}

		if (fdwState->is_tlist_func_pushdown == true)
		{
			foreach(lc, tlist)
			{
				TargetEntry *tle = lfirst_node(TargetEntry, lc);

				fdw_scan_tlist = lappend(fdw_scan_tlist, tle);
			}

			foreach(lc, fdwState->local_conds)
			{
				RestrictInfo *rinfo = lfirst_node(RestrictInfo, lc);

				fdw_scan_tlist = add_to_flat_tlist(fdw_scan_tlist,
												   pull_var_clause((Node *) rinfo->clause,
																   PVC_RECURSE_PLACEHOLDERS));
			}
		}

		/* check if the foreign scan is for an UPDATE or DELETE */
#if PG_VERSION_NUM < 140000
		if (foreignrel->relid == root->parse->resultRelation &&
			(root->parse->commandType == CMD_UPDATE ||
			root->parse->commandType == CMD_DELETE))
#else
		if (bms_is_member(foreignrel->relid, root->all_result_relids) &&
			(root->parse->commandType == CMD_UPDATE ||
			root->parse->commandType == CMD_DELETE))
#endif  /* PG_VERSION_NUM */
		{
			/* we need the table's primary key columns */
			need_keys = true;
		}

		/* check if FOR [KEY] SHARE/UPDATE was specified */
		if (need_keys || get_parse_rowmark(root->parse, foreignrel->relid))
		{
			/* we should add FOR UPDATE */
			for_update = true;
		}

		if (need_keys)
		{
			/* we need to fetch all primary key columns */
			for (i=0; i<fdwState->oraTable->ncols; ++i)
				if (fdwState->oraTable->cols[i]->pkey)
					fdwState->oraTable->cols[i]->used = 1;
		}

		/*
		 * Core code already has some lock on each rel being planned, so we can
		 * use NoLock here.
		 */
		rel = table_open(foreigntableid, NoLock);

		/* is there an AFTER trigger FOR EACH ROW? */
		has_trigger = (foreignrel->relid == root->parse->resultRelation)
						&& hasTrigger(rel, root->parse->commandType);

		table_close(rel, NoLock);

		if (has_trigger)
		{
			/* we need to fetch and return all columns */
			for (i=0; i<fdwState->oraTable->ncols; ++i)
				if (fdwState->oraTable->cols[i]->pgname)
					fdwState->oraTable->cols[i]->used = 1;
		}
#ifdef JOIN_API
	}
	else
	{
		/* we have a join relation, so set scan_relid to 0 */
		scan_relid = 0;

		/*
		 * create_scan_plan() and create_foreignscan_plan() pass
		 * rel->baserestrictinfo + parameterization clauses through
		 * scan_clauses. For a join rel->baserestrictinfo is NIL and we are
		 * not considering parameterization right now, so there should be no
		 * scan_clauses for a joinrel.
		 */
		Assert(!scan_clauses);

		/*
		 * Instead we get the conditions to apply from the fdw_private
		 * structure.
		 */
		remote_exprs = extract_actual_clauses(fdwState->remote_conds, false);
		local_exprs = extract_actual_clauses(fdwState->local_conds, false);


		/* Build the list of columns to be fetched from the foreign server. */
		fdw_scan_tlist = build_tlist_to_deparse(foreignrel);

		/*
		 * Ensure that the outer plan produces a tuple whose descriptor
		 * matches our scan tuple slot. This is safe because all scans and
		 * joins support projection, so we never need to insert a Result node.
		 * Also, remove the local conditions from outer plan's quals, lest
		 * they will be evaluated twice, once by the local plan and once by
		 * the scan.
		 */
		if (outer_plan)
		{
			ListCell   *lc;

			/*
			 * Right now, we only consider grouping and aggregation beyond
			 * joins. Queries involving aggregates or grouping do not require
			 * EPQ mechanism, hence should not have an outer plan here.
			 */
			Assert(!IS_UPPER_REL(foreignrel));

			outer_plan->targetlist = fdw_scan_tlist;

			foreach(lc, local_exprs)
			{
				Join       *join_plan = (Join *) outer_plan;
				Node       *qual = lfirst(lc);

				outer_plan->qual = list_delete(outer_plan->qual, qual);

				/*
				 * For an inner join the local conditions of foreign scan plan
				 * can be part of the joinquals as well.
				 */
				if (join_plan->jointype == JOIN_INNER)
					join_plan->joinqual = list_delete(join_plan->joinqual,
													  qual);
			}
		}
	}
#endif  /* JOIN_API */

	/*
	 * Build the query string to be sent for execution, and identify
	 * expressions to be sent as parameters.
	 */
	initStringInfo(&sql);
	oracleDeparseSelectStmtForRel(&sql, root, foreignrel, fdw_scan_tlist,
								  remote_exprs, for_update, best_path->path.pathkeys,
								  has_final_sort, has_limit, false,
								  &fdwState->retrieved_attrs, &(fdwState->params));

	/* create remote query */
	fdwState->query = sql.data;
	elog(DEBUG1, "oracle_fdw: remote query is: %s", fdwState->query);

	/* Remember remote_exprs for possible use by oraclePlanDirectModify */
	fdwState->final_remote_exprs = remote_exprs;

	/* connect to Oracle database */
	fdwState->session = oracleGetSession(fdwState->dbserver,
										 fdwState->isolation_level,
										 fdwState->user,
										 fdwState->password,
										 fdwState->nls_lang,
										 (int)fdwState->have_nchar,
										 fdwState->oraTable->pgname,
										 GetCurrentTransactionNestLevel());

	/*
	* rebuild oraTable for the scanning table based on the remote query,
	* don't use oraTable which was created at GetForeignRelSize()
	* because it represents to the remote table.
	*/
	fdwState->oraTable = oracleDescribe(fdwState->session,
													 fdwState->query,
													 fdwState->oraTable->name,
													 fdwState->oraTable->pgname,
													 fdwState->max_long);

	/* 
	 * In case of function pushdown, join pushdown and aggregation pushdown,
	 * node type of each item in the scan target list is not only T_Var,
	 * so we need Update node type in such cases. We check node type to get
	 * column option in getColumnDataByTupdesc.
	 */
	for (i=0; i < fdwState->oraTable->ncols; ++i)
	{
		/* default is T_Var */
		fdwState->oraTable->cols[i]->node_type = T_Var;

		/* update node type */
		if (fdw_scan_tlist != NIL)
		{
			TargetEntry* tle = (TargetEntry*) list_nth(fdw_scan_tlist, i);
			Expr *expr = (Expr *)tle->expr;

			fdwState->oraTable->cols[i]->node_type = expr->type;
		}
	}

	/* release Oracle session (will be cached) */
	pfree(fdwState->session);
	fdwState->session = NULL;

	fdw_private = serializePlanData(fdwState);

	/*
	 * Create the ForeignScan node for the given relation.
	 *
	 * Note that the remote parameter expressions are stored in the fdw_exprs
	 * field of the finished plan node; we can't keep them in private state
	 * because then they wouldn't be subject to later planner processing.
	 */
	return make_foreignscan(tlist, local_exprs, scan_relid, fdwState->params, fdw_private
#if PG_VERSION_NUM >= 90500
								, fdw_scan_tlist,
								NIL,  /* no parameterized paths */
								outer_plan
#endif  /* PG_VERSION_NUM */
							);
}

bool
oracleAnalyzeForeignTable(Relation relation, AcquireSampleRowsFunc *func, BlockNumber *totalpages)
{
	*func = acquireSampleRowsFunc;
	/* use positive page count as a sign that the table has been ANALYZEd */
	*totalpages = 42;

	return true;
}

/*
 * oracleExplainForeignScan
 * 		Produce extra output for EXPLAIN:
 * 		the Oracle query and, if VERBOSE was given, the execution plan.
 */
void
oracleExplainForeignScan(ForeignScanState *node, ExplainState *es)
{
	struct OracleFdwState *fdw_state = (struct OracleFdwState *)node->fdw_state;
	char **plan;
	int nrows, i;

	elog(DEBUG1, "oracle_fdw: explain foreign table scan");

	/* show query */
	ExplainPropertyText("Oracle query", fdw_state->query, es);

	if (es->verbose)
	{
		/* get the EXPLAIN PLAN */
		oracleExplain(fdw_state->session, fdw_state->query, &nrows, &plan);

		/* add it to explain text */
		for (i=0; i<nrows; ++i)
		{
			ExplainPropertyText("Oracle plan", plan[i], es);
		}
	}
}

/*
 * oracleBeginForeignScan
 * 		Recover ("deserialize") connection information, remote query,
 * 		Oracle table description and parameter list from the plan's
 * 		"fdw_private" field.
 * 		Reestablish a connection to Oracle.
 */
void
oracleBeginForeignScan(ForeignScanState *node, int eflags)
{
	ForeignScan *fsplan = (ForeignScan *)node->ss.ps.plan;
	List *fdw_private = fsplan->fdw_private;
	List *exec_exprs;
	ListCell *cell;
	int index;
	struct paramDesc *paramDesc;
	struct OracleFdwState *fdw_state;
	int i;
	TupleDesc tupdesc;
	Relation rel = NULL;

	/* deserialize private plan data */
	fdw_state = deserializePlanData(fdw_private);
	node->fdw_state = (void *)fdw_state;

	/* create an ExprState tree for the parameter expressions */
#if PG_VERSION_NUM < 100000
	exec_exprs = (List *)ExecInitExpr((Expr *)fsplan->fdw_exprs, (PlanState *)node);
#else
	exec_exprs = (List *)ExecInitExprList(fsplan->fdw_exprs, (PlanState *)node);
#endif  /* PG_VERSION_NUM */

	/* create the list of parameters */
	index = 0;
	foreach(cell, exec_exprs)
	{
		ExprState *expr = (ExprState *)lfirst(cell);
		char parname[10];

		/* count, but skip deleted entries */
		++index;
		if (expr == NULL)
			continue;

		/* create a new entry in the parameter list */
		paramDesc = (struct paramDesc *)palloc(sizeof(struct paramDesc));
		snprintf(parname, 10, ":p%d", index);
		paramDesc->name = pstrdup(parname);
		paramDesc->type = exprType((Node *)(expr->expr));

		if (paramDesc->type == TEXTOID || paramDesc->type == VARCHAROID
				|| paramDesc->type == BPCHAROID || paramDesc->type == CHAROID
				|| paramDesc->type == DATEOID || paramDesc->type == TIMESTAMPOID
				|| paramDesc->type == TIMESTAMPTZOID)
			paramDesc->bindType = BIND_STRING;
		else
			paramDesc->bindType = BIND_NUMBER;

		paramDesc->value = NULL;
		paramDesc->node = expr;
		paramDesc->bindh = NULL;
		paramDesc->colnum = -1;
		paramDesc->next = fdw_state->paramList;
		fdw_state->paramList = paramDesc;
	}

	/* add a fake parameter ":now" if that string appears in the query */
	if (strstr(fdw_state->query, ":now") != NULL)
	{
		paramDesc = (struct paramDesc *)palloc(sizeof(struct paramDesc));
		paramDesc->name = pstrdup(":now");
		paramDesc->type = TIMESTAMPTZOID;
		paramDesc->bindType = BIND_STRING;
		paramDesc->value = NULL;
		paramDesc->node = NULL;
		paramDesc->bindh = NULL;
		paramDesc->colnum = -1;
		paramDesc->next = fdw_state->paramList;
		fdw_state->paramList = paramDesc;
	}

	if (node->ss.ss_currentRelation)
		elog(DEBUG1, "oracle_fdw: begin foreign table scan on %d", RelationGetRelid(node->ss.ss_currentRelation));
	else
		elog(DEBUG1, "oracle_fdw: begin foreign join");

	/* connect to Oracle database */
	fdw_state->session = oracleGetSession(
			fdw_state->dbserver,
			fdw_state->isolation_level,
			fdw_state->user,
			fdw_state->password,
			fdw_state->nls_lang,
			(int)fdw_state->have_nchar,
			fdw_state->oraTable->pgname,
			GetCurrentTransactionNestLevel()
		);

	/* initialize row count to zero */
	fdw_state->rowcount = 0;
	fdw_state->next_tuple = 0;		/* only use for Direct Modification */

	/*
	 * Get info we'll need for converting data fetched from the foreign server
	 * into local representation and error reporting during that process.
	 */
	if (fsplan->scan.scanrelid > 0)
	{
		rel = node->ss.ss_currentRelation;
		tupdesc = node->ss.ss_ScanTupleSlot->tts_tupleDescriptor;
	}else
	{
		tupdesc = get_tupdesc_for_join_scan_tuples(node);
	}

	getColumnDataByTupdesc(rel, tupdesc, fdw_state->retrieved_attrs, fdw_state->oraTable);

	for (i=0; i<fdw_state->oraTable->ncols; ++i)
		if (fdw_state->oraTable->cols[i]->used)
			checkDataType(
				fdw_state->oraTable->cols[i]->oratype,
				fdw_state->oraTable->cols[i]->scale,
				fdw_state->oraTable->cols[i]->pgtype,
				fdw_state->oraTable->pgname,
				fdw_state->oraTable->cols[i]->pgname
			);
}

/*
 * oracleIterateForeignScan
 * 		On first invocation (if there is no Oracle statement yet),
 * 		get the actual parameter values and run the remote query against
 * 		the Oracle database, retrieving the first result row.
 * 		Subsequent invocations will fetch more result rows until there
 * 		are no more.
 * 		The result is stored as a virtual tuple in the ScanState's
 * 		TupleSlot and returned.
 */
TupleTableSlot *
oracleIterateForeignScan(ForeignScanState *node)
{
	TupleTableSlot *slot = node->ss.ss_ScanTupleSlot;
	ExprContext *econtext = node->ss.ps.ps_ExprContext;
	int have_result;
	struct OracleFdwState *fdw_state = (struct OracleFdwState *)node->fdw_state;

	if (oracleIsStatementOpen(fdw_state->session))
	{
		elog(DEBUG3, "oracle_fdw: get next row in foreign table scan");

		/* fetch the next result row */
		have_result = oracleFetchNext(fdw_state->session);
	}
	else
	{
		/* fill the parameter list with the actual values */
		char *paramInfo = setSelectParameters(fdw_state->paramList, econtext);

		/* execute the Oracle statement and fetch the first row */
		elog(DEBUG1, "oracle_fdw: execute query in foreign table scan %s", paramInfo);

		oraclePrepareQuery(fdw_state->session, fdw_state->query, fdw_state->oraTable, fdw_state->prefetch);
		have_result = oracleExecuteQuery(fdw_state->session, fdw_state->oraTable, fdw_state->paramList);
	}

	/* initialize virtual tuple */
	ExecClearTuple(slot);

	if (have_result)
	{
		/* increase row count */
		++fdw_state->rowcount;

		/* convert result to arrays of values and null indicators */
		convertTuple(fdw_state, slot->tts_values, slot->tts_isnull, false);

		/* store the virtual tuple */
		ExecStoreVirtualTuple(slot);
	}
	else
	{
		/* close the statement */
		oracleCloseStatement(fdw_state->session);
	}

	return slot;
}

/*
 * oracleEndForeignScan
 * 		Close the currently active Oracle statement.
 */
void
oracleEndForeignScan(ForeignScanState *node)
{
	struct OracleFdwState *fdw_state = (struct OracleFdwState *)node->fdw_state;

	elog(DEBUG1, "oracle_fdw: end foreign table scan");

	/* release the Oracle session */
	oracleCloseStatement(fdw_state->session);
	pfree(fdw_state->session);
	fdw_state->session = NULL;
}

/*
 * oracleReScanForeignScan
 * 		Close the Oracle statement if there is any.
 * 		That causes the next oracleIterateForeignScan call to restart the scan.
 */
void
oracleReScanForeignScan(ForeignScanState *node)
{
	struct OracleFdwState *fdw_state = (struct OracleFdwState *)node->fdw_state;

	elog(DEBUG1, "oracle_fdw: restart foreign table scan");

	/* close open Oracle statement if there is one */
	oracleCloseStatement(fdw_state->session);

	/* reset row count to zero */
	fdw_state->rowcount = 0;
}

/*
 * oracleAddForeignUpdateTargets
 * 		Add the primary key columns as resjunk entries.
 */
void
oracleAddForeignUpdateTargets(
#if PG_VERSION_NUM < 140000
	Query *parsetree,
#else
	PlannerInfo *root,
	Index rtindex,
#endif
	RangeTblEntry *target_rte,
	Relation target_relation
)
{
	Oid relid = RelationGetRelid(target_relation);
	TupleDesc tupdesc = target_relation->rd_att;
	int i;
	bool has_key = false;

	elog(DEBUG1, "oracle_fdw: add target columns for update on %d", relid);

	/* loop through all columns of the foreign table */
	for (i=0; i<tupdesc->natts; ++i)
	{
		Form_pg_attribute att = TupleDescAttr(tupdesc, i);
		AttrNumber attrno = att->attnum;
		List *options;
		ListCell *option;

		/* look for the "key" option on this column */
		options = GetForeignColumnOptions(relid, attrno);
		foreach(option, options)
		{
			DefElem *def = (DefElem *)lfirst(option);

			/* if "key" is set, add a resjunk for this column */
			if (strcmp(def->defname, OPT_KEY) == 0)
			{
				if (optionIsTrue(strVal(def->arg)))
				{
					Var *var;
#if PG_VERSION_NUM < 140000
					TargetEntry *tle;

					/* Make a Var representing the desired value */
					var = makeVar(
							parsetree->resultRelation,
							attrno,
							att->atttypid,
							att->atttypmod,
							att->attcollation,
							0);

					/* Wrap it in a resjunk TLE with the right name ... */
					tle = makeTargetEntry((Expr *)var,
							list_length(parsetree->targetList) + 1,
							pstrdup(NameStr(att->attname)),
							true);

					/* ... and add it to the query's targetlist */
					parsetree->targetList = lappend(parsetree->targetList, tle);
#else
					/* Make a Var representing the desired value */
					var = makeVar(
							rtindex,
							attrno,
							att->atttypid,
							att->atttypmod,
							att->attcollation,
							0);

					add_row_identity_var(root, var, rtindex, NameStr(att->attname));
#endif  /* PG_VERSION_NUM */

					has_key = true;
				}
			}
			/* Do nothing if option is "column_name" */
			else if (strcmp(def->defname, OPT_COLUMN_NAME) == 0)
				continue;
			else
			{
				elog(ERROR, "impossible column option \"%s\"", def->defname);
			}
		}
	}

	if (! has_key)
		ereport(ERROR,
				(errcode(ERRCODE_FDW_UNABLE_TO_CREATE_EXECUTION),
				errmsg("no primary key column specified for foreign Oracle table"),
				errdetail("For UPDATE or DELETE, at least one foreign table column must be marked as primary key column."),
				errhint("Set the option \"%s\" on the columns that belong to the primary key.", OPT_KEY)));
}

/*
 * oraclePlanForeignModify
 * 		Construct an OracleFdwState or copy it from the foreign scan plan.
 * 		Construct the Oracle DML statement and a list of necessary parameters.
 * 		Return the serialized OracleFdwState.
 */
List *
oraclePlanForeignModify(PlannerInfo *root, ModifyTable *plan, Index resultRelation, int subplan_index)
{
	CmdType operation = plan->operation;
	RangeTblEntry *rte = planner_rt_fetch(resultRelation, root);
	Relation rel = NULL;
	StringInfoData sql;
	List *targetAttrs = NIL;
	List *returningList = NIL;
	struct OracleFdwState *fdwState;
	int attnum, i;
	bool has_trigger = false, firstcol;
	char paramName[10];
	TupleDesc tupdesc;
	Bitmapset *tmpset;
	AttrNumber col;

#if PG_VERSION_NUM >= 90500
	/* we don't support INSERT ... ON CONFLICT */
	if (plan->onConflictAction != ONCONFLICT_NONE)
		ereport(ERROR,
				(errcode(ERRCODE_FDW_UNABLE_TO_CREATE_EXECUTION),
				errmsg("INSERT with ON CONFLICT clause is not supported")));
#endif  /* PG_VERSION_NUM */

	/*
	 * We have to construct the foreign table data ourselves.
	 * To match what ExecCheckRTEPerms does, pass the user whose user mapping
	 * should be used (if invalid, the current user is used).
	 */
	fdwState = getFdwState(rte->relid, NULL, rte->checkAsUser);

	initStringInfo(&sql);

	/*
	 * Core code already has some lock on each rel being planned, so we can
	 * use NoLock here.
	 */
	rel = table_open(rte->relid, NoLock);

	/* figure out which attributes are affected and if there is a trigger */
	if (operation == CMD_INSERT ||
		(operation == CMD_UPDATE &&
		 rel->trigdesc &&
		 rel->trigdesc->trig_update_before_row))
	{
		/*
		 * In an INSERT, we transmit all columns that are defined in the foreign
		 * table.  In an UPDATE, we transmit only columns that were explicitly
		 * targets of the UPDATE, so as to avoid unnecessary data transmission.
		 * (We can't do that for INSERT since we would miss sending default values
		 * for columns not listed in the source statement.)
		 */
		tupdesc = RelationGetDescr(rel);

		for (attnum = 1; attnum <= tupdesc->natts; attnum++)
		{
			Form_pg_attribute attr = TupleDescAttr(tupdesc, attnum - 1);

			if (!attr->attisdropped)
				targetAttrs = lappend_int(targetAttrs, attnum);
		}

		/* is there a row level AFTER/BEFORE trigger? */
		has_trigger = hasTrigger(rel, operation);
	}
	else if (operation == CMD_UPDATE)
	{
#if PG_VERSION_NUM >= 120000
		tmpset = bms_union(rte->updatedCols, rte->extraUpdatedCols);
#elif PG_VERSION_NUM >= 90500
		tmpset = bms_copy(rte->updatedCols);
#else
		tmpset = bms_copy(rte->modifiedCols);
#endif /* PG_VERSION_NUM */

		while ((col = bms_first_member(tmpset)) >= 0)
		{
			col += FirstLowInvalidHeapAttributeNumber;
			if (col <= InvalidAttrNumber) /* shouldn't happen */
				elog(ERROR, "system-column update is not supported");
			targetAttrs = lappend_int(targetAttrs, col);
		}

		/* is there a row level AFTER trigger? */
		has_trigger = hasTrigger(rel, CMD_UPDATE);
	}
	else if (operation == CMD_DELETE)
	{
		/* is there a row level AFTER trigger? */
		has_trigger = hasTrigger(rel, CMD_DELETE);
	}
	else
	{
		elog(ERROR, "unexpected operation: %d", (int) operation);
	}

	table_close(rel, NoLock);

	/* mark all attributes for which we need to return column values */
	if (has_trigger)
	{
		/* all attributes are needed for the RETURNING clause */
		for (i=0; i<fdwState->oraTable->ncols; ++i)
			if (fdwState->oraTable->cols[i]->pgname != NULL)
			{
				/* throw an error if it is a LONG or LONG RAW column */
				if (fdwState->oraTable->cols[i]->oratype == ORA_TYPE_LONGRAW
						|| fdwState->oraTable->cols[i]->oratype == ORA_TYPE_LONG)
					ereport(ERROR,
							(errcode(ERRCODE_FDW_INVALID_DATA_TYPE),
							errmsg("columns with Oracle type LONG or LONG RAW cannot be used with triggers"),
							errdetail("Column \"%s\" of foreign table \"%s\" is of Oracle type LONG%s.",
								fdwState->oraTable->cols[i]->pgname,
								fdwState->oraTable->pgname,
								fdwState->oraTable->cols[i]->oratype == ORA_TYPE_LONG ? "" : " RAW")));

				fdwState->oraTable->cols[i]->used = 1;
			}
	}
	else
	{
		Bitmapset *attrs_used = NULL;

		/* extract the relevant RETURNING list if any */
		if (plan->returningLists)
			returningList = (List *) list_nth(plan->returningLists, subplan_index);

		if (returningList != NIL)
		{
			bool	have_wholerow = false;

			/* get all the attributes mentioned there */
			pull_varattnos((Node *) returningList, resultRelation, &attrs_used);

			/* If there's a whole-row reference, we'll need all the columns. */
			have_wholerow = bms_is_member(0 - FirstLowInvalidHeapAttributeNumber,
										  attrs_used);

			/* mark the corresponding columns as used */
			for (i=0; i<fdwState->oraTable->ncols; ++i)
			{
				/* ignore columns that are not in the PostgreSQL table */
				if (fdwState->oraTable->cols[i]->pgname == NULL)
					continue;

				if (have_wholerow ||
					bms_is_member(fdwState->oraTable->cols[i]->pgattnum - FirstLowInvalidHeapAttributeNumber, attrs_used))
				{
					/* throw an error if it is a LONG or LONG RAW column */
					if (fdwState->oraTable->cols[i]->oratype == ORA_TYPE_LONGRAW
							|| fdwState->oraTable->cols[i]->oratype == ORA_TYPE_LONG)
						ereport(ERROR,
								(errcode(ERRCODE_FDW_INVALID_DATA_TYPE),
								errmsg("columns with Oracle type LONG or LONG RAW cannot be used in RETURNING clause"),
								errdetail("Column \"%s\" of foreign table \"%s\" is of Oracle type LONG%s.",
									fdwState->oraTable->cols[i]->pgname,
									fdwState->oraTable->pgname,
									fdwState->oraTable->cols[i]->oratype == ORA_TYPE_LONG ? "" : " RAW")));

					fdwState->oraTable->cols[i]->used = 1;
				}
			}
		}
	}

	/* construct the SQL command string */
	switch (operation)
	{
		case CMD_INSERT:
			buildInsertQuery(&sql, fdwState);

			break;
		case CMD_UPDATE:
			buildUpdateQuery(&sql, fdwState, targetAttrs);

			break;
		case CMD_DELETE:
			appendStringInfo(&sql, "DELETE FROM %s", fdwState->oraTable->name);

			break;
		default:
			elog(ERROR, "unexpected operation: %d", (int) operation);
	}

	if (operation == CMD_UPDATE || operation == CMD_DELETE)
	{
		/* add WHERE clause with the primary key columns */

		firstcol = true;
		for (i=0; i<fdwState->oraTable->ncols; ++i)
		{
			if (fdwState->oraTable->cols[i]->pkey)
			{
				/* add a parameter description */
				snprintf(paramName, 9, ":k%d", fdwState->oraTable->cols[i]->pgattnum);
				addParam(&fdwState->paramList, paramName, fdwState->oraTable->cols[i]->pgtype,
					fdwState->oraTable->cols[i]->oratype, i);

				/* add column and parameter name to query */
				if (firstcol)
				{
					appendStringInfo(&sql, " WHERE");
					firstcol = false;
				}
				else
					appendStringInfo(&sql, " AND");

				appendStringInfo(&sql, " %s = ", fdwState->oraTable->cols[i]->name);
				appendAsType(&sql, paramName, fdwState->oraTable->cols[i]->pgtype);
			}
		}
	}

	appendReturningClause(&sql, fdwState);

	fdwState->query = sql.data;

	elog(DEBUG1, "oracle_fdw: remote statement is: %s", fdwState->query);

	/* return a serialized form of the plan state */
	return serializePlanData(fdwState);
}

/*
 * oracleBeginForeignModify
 * 		Prepare everything for the DML query:
 * 		The SQL statement is prepared, the type output functions for
 * 		the parameters are fetched, and the column numbers of the
 * 		resjunk attributes are stored in the "pkey" field.
 */
void
oracleBeginForeignModify(ModifyTableState *mtstate, ResultRelInfo *rinfo, List *fdw_private, int subplan_index, int eflags)
{
	struct OracleFdwState *fdw_state = deserializePlanData(fdw_private);
	EState *estate = mtstate->ps.state;
	struct paramDesc *param;
	HeapTuple tuple;
	int i;
#if PG_VERSION_NUM < 140000
	Plan *subplan = mtstate->mt_plans[subplan_index]->plan;
#else
	Plan *subplan = outerPlanState(mtstate)->plan;
#endif

	/* init row count */
	fdw_state->next_tuple = 0;		/* only use for Direct Modification */

	elog(DEBUG1, "oracle_fdw: begin foreign table modify on %d", RelationGetRelid(rinfo->ri_RelationDesc));

	rinfo->ri_FdwState = fdw_state;

	/* connect to Oracle database */
	fdw_state->session = oracleGetSession(
			fdw_state->dbserver,
			fdw_state->isolation_level,
			fdw_state->user,
			fdw_state->password,
			fdw_state->nls_lang,
			(int)fdw_state->have_nchar,
			fdw_state->oraTable->pgname,
			GetCurrentTransactionNestLevel()
		);

	oraclePrepareQuery(fdw_state->session, fdw_state->query, fdw_state->oraTable, 0);

	/* get the type output functions for the parameters */
	output_funcs = (regproc *)palloc0(fdw_state->oraTable->ncols * sizeof(regproc *));
	for (param=fdw_state->paramList; param!=NULL; param=param->next)
	{
		/* ignore output parameters */
		if (param->bindType == BIND_OUTPUT)
			continue;

		tuple = SearchSysCache1(TYPEOID, ObjectIdGetDatum(fdw_state->oraTable->cols[param->colnum]->pgtype));
		if (!HeapTupleIsValid(tuple))
			elog(ERROR, "cache lookup failed for type %u", fdw_state->oraTable->cols[param->colnum]->pgtype);
		output_funcs[param->colnum] = ((Form_pg_type)GETSTRUCT(tuple))->typoutput;
		ReleaseSysCache(tuple);
	}

	/* loop through table columns */
	for (i=0; i<fdw_state->oraTable->ncols; ++i)
	{
		if (! fdw_state->oraTable->cols[i]->pkey)
			continue;

		/* for primary key columns, get the resjunk attribute number and store it in "pkey" */
		fdw_state->oraTable->cols[i]->pkey =
			ExecFindJunkAttributeInTlist(subplan->targetlist,
				fdw_state->oraTable->cols[i]->pgname);
	}

	/* create a memory context for short-lived memory */
	fdw_state->temp_cxt = AllocSetContextCreate(estate->es_query_cxt,
							"oracle_fdw temporary data",
							ALLOCSET_SMALL_SIZES);
}

#if PG_VERSION_NUM >= 110000
/*
 * oracleBeginForeignInsert
 * 		Initialize the FDW state for COPY to a foreign table.
 */
void oracleBeginForeignInsert(ModifyTableState *mtstate, ResultRelInfo *rinfo)
{
	ModifyTable *plan = castNode(ModifyTable, mtstate->ps.plan);
	EState *estate = mtstate->ps.state;
	struct OracleFdwState *fdw_state;
	Index resultRelation;
	RangeTblEntry *rte;
	StringInfoData buf;
	struct paramDesc *param;
	HeapTuple tuple;
	int i;

	elog(DEBUG3, "oracle_fdw: execute foreign table COPY on %d", RelationGetRelid(rinfo->ri_RelationDesc));

	/* we don't support INSERT ... ON CONFLICT */
	if (plan && plan->onConflictAction != ONCONFLICT_NONE)
		ereport(ERROR,
				(errcode(ERRCODE_FDW_UNABLE_TO_CREATE_EXECUTION),
				 errmsg("INSERT with ON CONFLICT clause is not supported")));

	/*
	 * If the foreign table we are about to insert routed rows into is also an
	 * UPDATE subplan result rel that will be updated later, proceeding with
	 * the INSERT will result in the later UPDATE incorrectly modifying those
	 * routed rows, so prevent the INSERT --- it would be nice if we could
	 * handle this case; but for now, throw an error for safety.
	 */
	if (plan && plan->operation == CMD_UPDATE &&
		(rinfo->ri_usesFdwDirectModify ||
		 rinfo->ri_FdwState))
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("cannot route tuples into foreign table to be updated")));

	/*
	 * If the foreign table is a partition that doesn't have a corresponding
	 * RTE entry, we need to create a new RTE describing the foreign table for
	 * use by deparseInsertSql and create_foreign_modify() below, after first
	 * copying the parent's RTE and modifying some fields to describe the
	 * foreign partition to work on. However, if this is invoked by UPDATE,
	 * the existing RTE may already correspond to this partition if it is one
	 * of the UPDATE subplan target rels; in that case, we can just use the
	 * existing RTE as-is.
	 */
	if (rinfo->ri_RangeTableIndex == 0)
	{
		ResultRelInfo *rootResultRelInfo = rinfo->ri_RootResultRelInfo;
		Index rootRelation;

#if PG_VERSION_NUM < 120000
		rte = list_nth(estate->es_range_table, rootResultRelInfo->ri_RangeTableIndex - 1);
		if (plan != NULL)
			rootRelation = plan->nominalRelation;
#else
		rte = exec_rt_fetch(rootResultRelInfo->ri_RangeTableIndex, estate);
		if (plan != NULL)
			rootRelation = plan->rootRelation;
#endif  /* PG_VERSION_NUM */
		rte = copyObject(rte);
		rte->relid = RelationGetRelid(rinfo->ri_RelationDesc);
		rte->relkind = RELKIND_FOREIGN_TABLE;

		/*
		 * For UPDATE, we must use the RT index of the first subplan target
		 * rel's RTE, because the core code would have built expressions for
		 * the partition, such as RETURNING, using that RT index as varno of
		 * Vars contained in those expressions.
		 */
		if (plan && plan->operation == CMD_UPDATE &&
			rootResultRelInfo->ri_RangeTableIndex == rootRelation)
			resultRelation = mtstate->resultRelInfo[0].ri_RangeTableIndex;
		else
			resultRelation = rootResultRelInfo->ri_RangeTableIndex;
	}
	else
	{
		resultRelation = rinfo->ri_RangeTableIndex;
#if PG_VERSION_NUM < 120000
		rte = list_nth(estate->es_range_table, resultRelation - 1);
#else
		rte = exec_rt_fetch(resultRelation, estate);
#endif  /* PG_VERSION_NUM */
	}

	/*
	 * To match what ExecCheckRTEPerms does, pass the user whose user mapping
	 * should be used (if invalid, the current user is used).
	 */
	fdw_state = getFdwState(RelationGetRelid(rinfo->ri_RelationDesc), NULL, rte->checkAsUser);

	/* not using "deserializePlanData", we have to initialize these ourselves */
	for (i=0; i<fdw_state->oraTable->ncols; ++i)
	{
		fdw_state->oraTable->cols[i]->val = (char *)palloc(fdw_state->oraTable->cols[i]->val_size + 1);
		fdw_state->oraTable->cols[i]->val_len = (unsigned int *)palloc0(sizeof(unsigned int));
		fdw_state->oraTable->cols[i]->val_len4 = (unsigned int *)palloc0(sizeof(unsigned int));
		fdw_state->oraTable->cols[i]->val_null = (short *)palloc(sizeof(short));
		memset(fdw_state->oraTable->cols[i]->val_null, 1, sizeof(short));
	}

	/* init row count */
	fdw_state->rowcount = 0;
	fdw_state->next_tuple = 0;		/* only use for Direct Modification */

	fdw_state->session = oracleGetSession(
			fdw_state->dbserver,
			fdw_state->isolation_level,
			fdw_state->user,
			fdw_state->password,
			fdw_state->nls_lang,
			(int)fdw_state->have_nchar,
			fdw_state->oraTable->pgname,
			GetCurrentTransactionNestLevel()
		);

	/*
	 * We need to fetch all attributes if there is an AFTER INSERT trigger
	 * or if the foreign table is a partition, and the statement is
	 * INSERT ... RETURNING on the partitioned table.
	 * We could figure out what columns to return in the second case,
	 * but let's keep it simple for now.
	 */
	if (hasTrigger(rinfo->ri_RelationDesc, CMD_INSERT)
		|| (estate->es_plannedstmt != NULL && estate->es_plannedstmt->hasReturning))
	{
		/* mark all attributes for returning */
		for (i=0; i<fdw_state->oraTable->ncols; ++i)
			if (fdw_state->oraTable->cols[i]->pgname != NULL)
			{
				/* throw an error if it is a LONG or LONG RAW column */
				if (fdw_state->oraTable->cols[i]->oratype == ORA_TYPE_LONGRAW
						|| fdw_state->oraTable->cols[i]->oratype == ORA_TYPE_LONG)
					ereport(ERROR,
							(errcode(ERRCODE_FDW_INVALID_DATA_TYPE),
							errmsg("columns with Oracle type LONG or LONG RAW cannot be used with triggers or in RETURNING clause"),
							errdetail("Column \"%s\" of foreign table \"%s\" is of Oracle type LONG%s.",
								fdw_state->oraTable->cols[i]->pgname,
								fdw_state->oraTable->pgname,
								fdw_state->oraTable->cols[i]->oratype == ORA_TYPE_LONG ? "" : " RAW")));

				fdw_state->oraTable->cols[i]->used = 1;
			}
	}

	/* construct an INSERT query */
	initStringInfo(&buf);
	buildInsertQuery(&buf, fdw_state);
	appendReturningClause(&buf, fdw_state);
	fdw_state->query = pstrdup(buf.data);

	/* get the type output functions for the parameters */
	output_funcs = (regproc *)palloc0(fdw_state->oraTable->ncols * sizeof(regproc *));
	for (param=fdw_state->paramList; param!=NULL; param=param->next)
	{
		/* ignore output parameters */
		if (param->bindType == BIND_OUTPUT)
			continue;

		tuple = SearchSysCache1(TYPEOID, ObjectIdGetDatum(fdw_state->oraTable->cols[param->colnum]->pgtype));
		if (!HeapTupleIsValid(tuple))
			elog(ERROR, "cache lookup failed for type %u", fdw_state->oraTable->cols[param->colnum]->pgtype);
		output_funcs[param->colnum] = ((Form_pg_type)GETSTRUCT(tuple))->typoutput;
		ReleaseSysCache(tuple);
	}

	oraclePrepareQuery(fdw_state->session, fdw_state->query, fdw_state->oraTable, 0);

	/* create a memory context for short-lived memory */
	fdw_state->temp_cxt = AllocSetContextCreate(estate->es_query_cxt,
							"oracle_fdw temporary data",
							ALLOCSET_SMALL_SIZES);

	rinfo->ri_FdwState = (void *)fdw_state;
}

void
oracleEndForeignInsert(EState *estate, ResultRelInfo *rinfo)
{
	struct OracleFdwState *fdw_state = (struct OracleFdwState *)rinfo->ri_FdwState;

	elog(DEBUG3, "oracle_fdw: end foreign table COPY on %d", RelationGetRelid(rinfo->ri_RelationDesc));

	MemoryContextDelete(fdw_state->temp_cxt);

	/* release the Oracle session */
	oracleCloseStatement(fdw_state->session);
	pfree(fdw_state->session);
	fdw_state->session = NULL;
}
#endif  /*PG_VERSION_NUM */

/*
 * oracleExecForeignInsert
 * 		Set the parameter values from the slots and execute the INSERT statement.
 * 		Returns a slot with the results from the RETRUNING clause.
 */
TupleTableSlot *
oracleExecForeignInsert(EState *estate, ResultRelInfo *rinfo, TupleTableSlot *slot, TupleTableSlot *planSlot)
{
	struct OracleFdwState *fdw_state = (struct OracleFdwState *)rinfo->ri_FdwState;
	int rows;
	MemoryContext oldcontext;

	elog(DEBUG3, "oracle_fdw: execute foreign table insert on %d", RelationGetRelid(rinfo->ri_RelationDesc));

	dml_in_transaction = true;

	MemoryContextReset(fdw_state->temp_cxt);
	oldcontext = MemoryContextSwitchTo(fdw_state->temp_cxt);

	/* extract the values from the slot and store them in the parameters */
	setModifyParameters(fdw_state->paramList, slot, planSlot, fdw_state->oraTable, fdw_state->session);

	/* execute the INSERT statement and store RETURNING values in oraTable's columns */
	rows = oracleExecuteQuery(fdw_state->session, fdw_state->oraTable, fdw_state->paramList);

	if (rows > 1)
		ereport(ERROR,
				(errcode(ERRCODE_FDW_UNABLE_TO_CREATE_EXECUTION),
				errmsg("INSERT on Oracle table added %d rows instead of one in iteration %lu", rows, fdw_state->rowcount)));

	MemoryContextSwitchTo(oldcontext);

	/* empty the result slot */
	ExecClearTuple(slot);

	if (rows == 1)
	{
		++fdw_state->rowcount;

		/* convert result for RETURNING to arrays of values and null indicators */
		convertTuple(fdw_state, slot->tts_values, slot->tts_isnull, false);

		/* store the virtual tuple */
		ExecStoreVirtualTuple(slot);
	}

	return slot;
}

/*
 * oracleExecForeignUpdate
 * 		Set the parameter values from the slots and execute the UPDATE statement.
 * 		Returns a slot with the results from the RETRUNING clause.
 */
TupleTableSlot *
oracleExecForeignUpdate(EState *estate, ResultRelInfo *rinfo, TupleTableSlot *slot, TupleTableSlot *planSlot)
{
	struct OracleFdwState *fdw_state = (struct OracleFdwState *)rinfo->ri_FdwState;
	int rows;
	MemoryContext oldcontext;

	elog(DEBUG3, "oracle_fdw: execute foreign table update on %d", RelationGetRelid(rinfo->ri_RelationDesc));

	dml_in_transaction = true;

	MemoryContextReset(fdw_state->temp_cxt);
	oldcontext = MemoryContextSwitchTo(fdw_state->temp_cxt);

	/* extract the values from the slot and store them in the parameters */
	setModifyParameters(fdw_state->paramList, slot, planSlot, fdw_state->oraTable, fdw_state->session);

	/* execute the UPDATE statement and store RETURNING values in oraTable's columns */
	rows = oracleExecuteQuery(fdw_state->session, fdw_state->oraTable, fdw_state->paramList);

	if (rows > 1)
		ereport(ERROR,
				(errcode(ERRCODE_FDW_UNABLE_TO_CREATE_EXECUTION),
				errmsg("UPDATE on Oracle table changed %d rows instead of one in iteration %lu", rows, fdw_state->rowcount),
				errhint("This probably means that you did not set the \"key\" option on all primary key columns.")));

	MemoryContextSwitchTo(oldcontext);

	/* empty the result slot */
	ExecClearTuple(slot);

	if (rows == 1)
	{
		++fdw_state->rowcount;

		/* convert result for RETURNING to arrays of values and null indicators */
		convertTuple(fdw_state, slot->tts_values, slot->tts_isnull, false);

		/* store the virtual tuple */
		ExecStoreVirtualTuple(slot);
	}

	return slot;
}

/*
 * oracleExecForeignDelete
 * 		Set the parameter values from the slots and execute the DELETE statement.
 * 		Returns a slot with the results from the RETRUNING clause.
 */
TupleTableSlot *
oracleExecForeignDelete(EState *estate, ResultRelInfo *rinfo, TupleTableSlot *slot, TupleTableSlot *planSlot)
{
	struct OracleFdwState *fdw_state = (struct OracleFdwState *)rinfo->ri_FdwState;
	int rows;
	MemoryContext oldcontext;

	elog(DEBUG3, "oracle_fdw: execute foreign table delete on %d", RelationGetRelid(rinfo->ri_RelationDesc));

	dml_in_transaction = true;

	MemoryContextReset(fdw_state->temp_cxt);
	oldcontext = MemoryContextSwitchTo(fdw_state->temp_cxt);

	/* extract the values from the slot and store them in the parameters */
	setModifyParameters(fdw_state->paramList, slot, planSlot, fdw_state->oraTable, fdw_state->session);

	/* execute the DELETE statement and store RETURNING values in oraTable's columns */
	rows = oracleExecuteQuery(fdw_state->session, fdw_state->oraTable, fdw_state->paramList);

	if (rows > 1)
		ereport(ERROR,
				(errcode(ERRCODE_FDW_UNABLE_TO_CREATE_EXECUTION),
				errmsg("DELETE on Oracle table removed %d rows instead of one in iteration %lu", rows, fdw_state->rowcount),
				errhint("This probably means that you did not set the \"key\" option on all primary key columns.")));

	MemoryContextSwitchTo(oldcontext);

	/* empty the result slot */
	ExecClearTuple(slot);

	if (rows == 1)
	{
		++fdw_state->rowcount;

		/* convert result for RETURNING to arrays of values and null indicators */
		convertTuple(fdw_state, slot->tts_values, slot->tts_isnull, false);

		/* store the virtual tuple */
		ExecStoreVirtualTuple(slot);
	}

	return slot;
}

/*
 * oracleEndForeignModify
 * 		Close the currently active Oracle statement.
 */
void
oracleEndForeignModify(EState *estate, ResultRelInfo *rinfo){
	struct OracleFdwState *fdw_state = (struct OracleFdwState *)rinfo->ri_FdwState;

	elog(DEBUG1, "oracle_fdw: end foreign table modify on %d", RelationGetRelid(rinfo->ri_RelationDesc));

	MemoryContextDelete(fdw_state->temp_cxt);

	/* release the Oracle session */
	if (fdw_state->session != NULL)
	{
		oracleCloseStatement(fdw_state->session);
		pfree(fdw_state->session);
		fdw_state->session = NULL;
	}
}

/*
 * oracleExplainForeignModify
 * 		Show the Oracle DML statement.
 * 		Nothing special is done for VERBOSE because the query plan is likely trivial.
 */
void
oracleExplainForeignModify(ModifyTableState *mtstate, ResultRelInfo *rinfo, List *fdw_private, int subplan_index, struct ExplainState *es){
	struct OracleFdwState *fdw_state = (struct OracleFdwState *)rinfo->ri_FdwState;

	elog(DEBUG1, "oracle_fdw: explain foreign table modify on %d", RelationGetRelid(rinfo->ri_RelationDesc));

	/* show query */
	ExplainPropertyText("Oracle statement", fdw_state->query, es);
}

/*
 * oracleIsForeignRelUpdatable
 * 		Returns 0 if "readonly" is set, a value indicating that all DML is allowed.
 */
int
oracleIsForeignRelUpdatable(Relation rel)
{
	ListCell *cell;

	/* loop foreign table options */
	foreach(cell, GetForeignTable(RelationGetRelid(rel))->options)
	{
		DefElem *def = (DefElem *) lfirst(cell);
		char *value = strVal(def->arg);
		if (strcmp(def->defname, OPT_READONLY) == 0
				&& optionIsTrue(value))
			return 0;
	}

	return (1 << CMD_UPDATE) | (1 << CMD_INSERT) | (1 << CMD_DELETE);
}

#ifdef IMPORT_API
/*
 * oracleImportForeignSchema
 * 		Returns a List of CREATE FOREIGN TABLE statements.
 */
List *
oracleImportForeignSchema(ImportForeignSchemaStmt *stmt, Oid serverOid)
{
	ForeignServer *server;
	UserMapping *mapping;
	ForeignDataWrapper *wrapper;
	char *tabname, *colname, oldtabname[129] = { '\0' }, *foldedname;
	char *nls_lang = NULL, *user = NULL, *password = NULL, *dbserver = NULL;
	char *dblink = NULL, *max_long = NULL, *sample_percent = NULL, *prefetch = NULL;
	oraType type;
	int charlen, typeprec, typescale, nullable, key, rc;
	List *options, *result = NIL;
	ListCell *cell;
	oracleSession *session;
	fold_t foldcase = CASE_SMART;
	StringInfoData buf;
	bool readonly = false, firstcol = true;
	int collation = DEFAULT_COLLATION_OID;
	oraIsoLevel isolation_level_val = DEFAULT_ISOLATION_LEVEL;
	bool have_nchar = false;

	/* get the foreign server, the user mapping and the FDW */
	server = GetForeignServer(serverOid);
	mapping = GetUserMapping(GetUserId(), serverOid);
	wrapper = GetForeignDataWrapper(server->fdwid);

	/* get all options for these objects */
	options = wrapper->options;
	options = list_concat(options, server->options);
	options = list_concat(options, mapping->options);

	foreach(cell, options)
	{
		DefElem *def = (DefElem *) lfirst(cell);
		if (strcmp(def->defname, OPT_NLS_LANG) == 0)
			nls_lang = strVal(def->arg);
		if (strcmp(def->defname, OPT_DBSERVER) == 0)
			dbserver = strVal(def->arg);
		if (strcmp(def->defname, OPT_ISOLATION_LEVEL) == 0)
			isolation_level_val = getIsolationLevel(strVal(def->arg));
		if (strcmp(def->defname, OPT_USER) == 0)
			user = (strVal(def->arg));
		if (strcmp(def->defname, OPT_PASSWORD) == 0)
			password = strVal(def->arg);
		if (strcmp(def->defname, OPT_NCHAR) == 0)
		{
			char *nchar = strVal(def->arg);

			if (pg_strcasecmp(nchar, "on") == 0
					|| pg_strcasecmp(nchar, "yes") == 0
					|| pg_strcasecmp(nchar, "true") == 0)
				have_nchar = true;
		}
	}

	/* process the options of the IMPORT FOREIGN SCHEMA command */
	foreach(cell, stmt->options)
	{
		DefElem *def = (DefElem *) lfirst(cell);

		if (strcmp(def->defname, "case") == 0)
		{
			char *s = strVal(def->arg);
			if (strcmp(s, "keep") == 0)
				foldcase = CASE_KEEP;
			else if (strcmp(s, "lower") == 0)
				foldcase = CASE_LOWER;
			else if (strcmp(s, "smart") == 0)
				foldcase = CASE_SMART;
			else
				ereport(ERROR,
						(errcode(ERRCODE_FDW_INVALID_ATTRIBUTE_VALUE),
						errmsg("invalid value for option \"%s\"", def->defname),
						errhint("Valid values in this context are: %s", "keep, lower, smart")));
		}
		else if (strcmp(def->defname, "collation") == 0)
		{
			char *s = strVal(def->arg);
			if (pg_strcasecmp(s, "default") != 0) {

			/* look up collation within pg_catalog namespace with the name */

#if PG_VERSION_NUM >= 120000
			collation = GetSysCacheOid3(
							COLLNAMEENCNSP,
							Anum_pg_collation_oid,
							PointerGetDatum(s),
							Int32GetDatum(Int32GetDatum(-1)),
							ObjectIdGetDatum(PG_CATALOG_NAMESPACE)
						);
#else
			collation = GetSysCacheOid3(
							COLLNAMEENCNSP,
							PointerGetDatum(s),
							Int32GetDatum(Int32GetDatum(-1)),
							ObjectIdGetDatum(PG_CATALOG_NAMESPACE)
						);
#endif  /* PG_VERSION_NUM */

			if (!OidIsValid(collation))
				ereport(ERROR,
						(errcode(ERRCODE_FDW_INVALID_ATTRIBUTE_VALUE),
						errmsg("invalid value for option \"%s\"", def->defname),
						errhint("Check the \"pg_collation\" catalog for valid values.")));
			}
		}
		else if (strcmp(def->defname, OPT_READONLY) == 0)
		{
			char *s = strVal(def->arg);
			if (pg_strcasecmp(s, "on") == 0
					|| pg_strcasecmp(s, "yes") == 0
					|| pg_strcasecmp(s, "true") == 0)
				readonly = true;
			else if (pg_strcasecmp(s, "off") == 0
					|| pg_strcasecmp(s, "no") == 0
					|| pg_strcasecmp(s, "false") == 0)
				readonly = false;
			else
				ereport(ERROR,
						(errcode(ERRCODE_FDW_INVALID_ATTRIBUTE_VALUE),
						errmsg("invalid value for option \"%s\"", def->defname)));
		}
		else if (strcmp(def->defname, OPT_DBLINK) == 0)
		{
			char *s = strVal(def->arg);
			if (strchr(s, '"') != NULL)
				ereport(ERROR,
						(errcode(ERRCODE_FDW_INVALID_ATTRIBUTE_VALUE),
						errmsg("invalid value for option \"%s\"", def->defname),
						errhint("Double quotes are not allowed in the dblink name.")));
			dblink = s;
		}
		else if (strcmp(def->defname, OPT_MAX_LONG) == 0)
		{
			char *endptr;
			unsigned long max_long_val;

			max_long = strVal(def->arg);
			errno = 0;
			max_long_val = strtoul(max_long, &endptr, 0);
			if (max_long[0] == '\0' || *endptr != '\0' || errno != 0 || max_long_val < 1 || max_long_val > 1073741823ul)
				ereport(ERROR,
						(errcode(ERRCODE_FDW_INVALID_ATTRIBUTE_VALUE),
						errmsg("invalid value for option \"%s\"", def->defname),
						errhint("Valid values in this context are integers between 1 and 1073741823.")));
		}
		else if (strcmp(def->defname, OPT_SAMPLE) == 0)
		{
			char *endptr;
			double sample_percent_val;

			sample_percent = strVal(def->arg);
			errno = 0;
			sample_percent_val = strtod(sample_percent, &endptr);
			if (sample_percent[0] == '\0' || *endptr != '\0' || errno != 0 || sample_percent_val < 0.000001 || sample_percent_val > 100.0)
				ereport(ERROR,
						(errcode(ERRCODE_FDW_INVALID_ATTRIBUTE_VALUE),
						errmsg("invalid value for option \"%s\"", def->defname),
						errhint("Valid values in this context are numbers between 0.000001 and 100.")));
		}
		else if (strcmp(def->defname, OPT_PREFETCH) == 0)
		{
			char *endptr;
			long prefetch_val;

			prefetch = strVal(def->arg);
			errno = 0;
			prefetch_val = strtol(prefetch, &endptr, 0);
			if (prefetch[0] == '\0' || *endptr != '\0' || errno != 0 || prefetch_val < 0 || prefetch_val > 10240 )
				ereport(ERROR,
						(errcode(ERRCODE_FDW_INVALID_ATTRIBUTE_VALUE),
						errmsg("invalid value for option \"%s\"", def->defname),
						errhint("Valid values in this context are integers between 0 and 10240.")));
		}
		else
			ereport(ERROR,
					(errcode(ERRCODE_FDW_INVALID_OPTION_NAME),
					errmsg("invalid option \"%s\"", def->defname),
					errhint("Valid options in this context are: %s, %s, %s, %s, %s, %s",
						"case, collation", OPT_READONLY, OPT_DBLINK,
						OPT_MAX_LONG, OPT_SAMPLE, OPT_PREFETCH)));
	}

	elog(DEBUG1, "oracle_fdw: import schema \"%s\" from foreign server \"%s\"", stmt->remote_schema, server->servername);

	/* guess a good NLS_LANG environment setting */
	nls_lang = guessNlsLang(nls_lang);

	/* connect to Oracle database */
	session = oracleGetSession(
		dbserver,
		isolation_level_val,
		user,
		password,
		nls_lang,
		(int)have_nchar,
		NULL,
		1
	);

	initStringInfo(&buf);
	do {
		/* get the next column definition */
		rc = oracleGetImportColumn(session, dblink, stmt->remote_schema, &tabname, &colname, &type, &charlen, &typeprec, &typescale, &nullable, &key);

		if (rc == -1)
		{
			/* remote schema does not exist, issue a warning */
			ereport(ERROR,
					(errcode(ERRCODE_FDW_SCHEMA_NOT_FOUND),
					errmsg("remote schema \"%s\" does not exist", stmt->remote_schema),
					errhint("Enclose the schema name in double quotes to prevent case folding.")));

			return NIL;
		}

		if ((rc == 0 && oldtabname[0] != '\0')
			|| (rc == 1 && oldtabname[0] != '\0' && strcmp(tabname, oldtabname)))
		{
			/* finish previous CREATE FOREIGN TABLE statement */
			appendStringInfo(&buf, ") SERVER \"%s\" OPTIONS (schema '%s', table '%s'",
				server->servername, stmt->remote_schema, oldtabname);
			if (dblink)
				appendStringInfo(&buf, ", dblink '%s'", dblink);
			if (readonly)
				appendStringInfo(&buf, ", readonly 'true'");
			if (max_long)
				appendStringInfo(&buf, ", max_long '%s'", max_long);
			if (sample_percent)
				appendStringInfo(&buf, ", sample_percent '%s'", sample_percent);
			if (prefetch)
				appendStringInfo(&buf, ", prefetch '%s'", prefetch);
			appendStringInfo(&buf, ")");

			result = lappend(result, pstrdup(buf.data));
		}

		if (rc == 1 && (oldtabname[0] == '\0' || strcmp(tabname, oldtabname)))
		{
			/* start a new CREATE FOREIGN TABLE statement */
			resetStringInfo(&buf);
			foldedname = fold_case(tabname, foldcase, collation);
			appendStringInfo(&buf, "CREATE FOREIGN TABLE \"%s\" (", foldedname);
			pfree(foldedname);

			firstcol = true;
			strcpy(oldtabname, tabname);
		}

		if (rc == 1)
		{
			/*
			 * Add a column definition.
			 */

			if (firstcol)
				firstcol = false;
			else
				appendStringInfo(&buf, ", ");

			/* column name */
			foldedname = fold_case(colname, foldcase, collation);
			appendStringInfo(&buf, "\"%s\" ", foldedname);
			pfree(foldedname);

			/* data type */
			switch (type)
			{
				case ORA_TYPE_CHAR:
				case ORA_TYPE_NCHAR:
					appendStringInfo(&buf, "character(%d)", charlen == 0 ? 1 : charlen);
					break;
				case ORA_TYPE_VARCHAR2:
				case ORA_TYPE_NVARCHAR2:
					appendStringInfo(&buf, "character varying(%d)", charlen == 0 ? 1 : charlen);
					break;
				case ORA_TYPE_CLOB:
				case ORA_TYPE_LONG:
					appendStringInfo(&buf, "text");
					break;
				case ORA_TYPE_NUMBER:
					if (typeprec == 0)
						appendStringInfo(&buf, "numeric");
					else if (typescale == 0)
					{
						if (typeprec < 5)
							appendStringInfo(&buf, "smallint");
						else if (typeprec < 10)
							appendStringInfo(&buf, "integer");
						else if (typeprec < 19)
							appendStringInfo(&buf, "bigint");
						else
							appendStringInfo(&buf, "numeric(%d)", typeprec);
					}
					else
						/*
						 * in Oracle, precision can be less than scale
						 * (numbers like 0.023), but we have to increase
						 * the precision for such columns in PostgreSQL.
						 */
						appendStringInfo(&buf, "numeric(%d, %d)",
							(typeprec < typescale) ? typescale : typeprec,
							typescale);
					break;
				case ORA_TYPE_FLOAT:
					if (typeprec < 54)
						appendStringInfo(&buf, "float(%d)", typeprec);
					else
						appendStringInfo(&buf, "numeric");
					break;
				case ORA_TYPE_BINARYFLOAT:
					appendStringInfo(&buf, "real");
					break;
				case ORA_TYPE_BINARYDOUBLE:
					appendStringInfo(&buf, "double precision");
					break;
				case ORA_TYPE_RAW:
				case ORA_TYPE_BLOB:
				case ORA_TYPE_BFILE:
				case ORA_TYPE_LONGRAW:
					appendStringInfo(&buf, "bytea");
					break;
				case ORA_TYPE_DATE:
					appendStringInfo(&buf, "timestamp(0) without time zone");
					break;
				case ORA_TYPE_TIMESTAMP:
					appendStringInfo(&buf, "timestamp(%d) without time zone", (typescale > 6) ? 6 : typescale);
					break;
				case ORA_TYPE_TIMESTAMPTZ:
					appendStringInfo(&buf, "timestamp(%d) with time zone", (typescale > 6) ? 6 : typescale);
					break;
				case ORA_TYPE_INTERVALD2S:
					appendStringInfo(&buf, "interval(%d)", (typescale > 6) ? 6 : typescale);
					break;
				case ORA_TYPE_INTERVALY2M:
					appendStringInfo(&buf, "interval(0)");
					break;
				case ORA_TYPE_XMLTYPE:
					appendStringInfo(&buf, "xml");
					break;
				case ORA_TYPE_GEOMETRY:
					if (GEOMETRYOID != InvalidOid)
					{
						appendStringInfo(&buf, "geometry");
						break;
					}
					/* fall through */
				default:
					elog(DEBUG2, "column \"%s\" of table \"%s\" has an untranslatable data type", colname, tabname);
					appendStringInfo(&buf, "text");
			}

			/* part of the primary key */
			if (key)
				appendStringInfo(&buf, " OPTIONS (key 'true')");

			/* not nullable */
			if (!nullable)
				appendStringInfo(&buf, " NOT NULL");
		}
	}
	while (rc == 1);

	return result;
}
#endif  /* IMPORT_API */

/*
 * oracleGetForeignUpperPaths
 *		Add paths for post-join operations like aggregation, grouping etc. if
 *		corresponding operations are safe to push down.
 */
static void
oracleGetForeignUpperPaths(PlannerInfo *root, UpperRelationKind stage,
							 RelOptInfo *input_rel, RelOptInfo *output_rel,
							 void *extra)
{
	struct OracleFdwState *fpinfo;


	/*
	 * If input rel is not safe to pushdown, then simply return as we cannot
	 * perform any post-join operations on the foreign server.
	 */
	if (!input_rel->fdw_private ||
		!((struct OracleFdwState *) input_rel->fdw_private)->pushdown_safe)
		return;

	/* Ignore stages we don't support; and skip any duplicate calls. */
	if ((stage != UPPERREL_GROUP_AGG &&
		 stage != UPPERREL_ORDERED &&
		 stage != UPPERREL_FINAL) ||
		output_rel->fdw_private)
		return;

	fpinfo = (struct OracleFdwState *) palloc0(sizeof(struct OracleFdwState));
	fpinfo->pushdown_safe = false;
	fpinfo->stage = stage;
	output_rel->fdw_private = fpinfo;

	switch (stage)
	{
		case UPPERREL_GROUP_AGG:
			add_foreign_grouping_paths(root, input_rel, output_rel,
									   (GroupPathExtraData *) extra);
			break;
		case UPPERREL_ORDERED:
			add_foreign_ordered_paths(root, input_rel, output_rel);
			break;
		case UPPERREL_FINAL:
			add_foreign_final_paths(root, input_rel, output_rel,
									(FinalPathExtraData *) extra);
			break;
		default:
			elog(ERROR, "unexpected upper relation %d", (int) stage);
			break;
	}
}


/*
 * oraclePlanDirectModify
 *		Consider a direct foreign table modification
 *
 * Decide whether it is safe to modify a foreign table directly, and if so,
 * rewrite subplan accordingly.
 */
static bool
oraclePlanDirectModify(PlannerInfo *root,
						 ModifyTable *plan,
						 Index resultRelation,
						 int subplan_index)
{
	CmdType		operation = plan->operation;
	RelOptInfo *foreignrel;
	RangeTblEntry *rte;
	struct OracleFdwState *fpinfo, *fdwState;
	Relation	rel;
	StringInfoData sql;
	ForeignScan *fscan;
#if PG_VERSION_NUM >= 140000
	List	   *processed_tlist = NIL;
#else
	Plan	   *subplan;
#endif
	List	   *targetAttrs = NIL;
	List	   *remote_exprs;
	List	   *params_list = NIL;
	List	   *returningList = NIL;
	List	   *retrieved_attrs = NIL;
	int	i;

	elog(DEBUG1, "oracle_fdw: plan direct modify");

	/*
	 * Decide whether it is safe to modify a foreign table directly.
	 */

	/*
	 * The table modification must be an UPDATE or DELETE.
	 */
	if (operation != CMD_UPDATE && operation != CMD_DELETE)
		return false;

#if PG_VERSION_NUM >= 140000
	/*
	 * Try to locate the ForeignScan subplan that's scanning resultRelation.
	 */
	fscan = find_modifytable_subplan(root, plan, resultRelation, subplan_index);
	if (!fscan)
		return false;

	/*
	 * It's unsafe to modify a foreign table directly if there are any quals
	 * that should be evaluated locally.
	 */
	if (fscan->scan.plan.qual != NIL)
		return false;
#else
	/*
	 * It's unsafe to modify a foreign table directly if there are any local
	 * joins needed.
	 */
	subplan = (Plan *) list_nth(plan->plans, subplan_index);
	if (!IsA(subplan, ForeignScan))
		return false;
	fscan = (ForeignScan *) subplan;

	/*
	 * It's unsafe to modify a foreign table directly if there are any quals
	 * that should be evaluated locally.
	 */
	if (subplan->qual != NIL)
		return false;
#endif

	/* Safe to fetch data about the target foreign rel */
	if (fscan->scan.scanrelid == 0)
	{
		foreignrel = find_join_rel(root, fscan->fs_relids);
		/* We should have a rel for this foreign join. */
		Assert(foreignrel);
	}
	else
		foreignrel = root->simple_rel_array[resultRelation];
	rte = root->simple_rte_array[resultRelation];

	/* Need to refactor this part, need oraTable only */
	fdwState = getFdwState(rte->relid, NULL, rte->checkAsUser);
	fpinfo = (struct OracleFdwState *) foreignrel->fdw_private;
	fpinfo->oraTable = fdwState->oraTable;

	pfree(fdwState->session);
	fdwState->session = NULL;

	/*
	 * It's unsafe to update a foreign table directly, if any expressions to
	 * assign to the target columns are unsafe to evaluate remotely.
	 */
	if (operation == CMD_UPDATE)
	{
#if PG_VERSION_NUM >= 140000
		ListCell   *lc,
				   *lc2;

		/*
		 * The expressions of concern are the first N columns of the processed
		 * targetlist, where N is the length of the rel's update_colnos.
		 */
		get_translated_update_targetlist(root, resultRelation,
										 &processed_tlist, &targetAttrs);
		forboth(lc, processed_tlist, lc2, targetAttrs)
		{
			TargetEntry *tle = lfirst_node(TargetEntry, lc);
			AttrNumber	attno = lfirst_int(lc2);
			deparse_expr_cxt	context;

			initializeContext(fpinfo, root, foreignrel, foreignrel, &context);

			/* update's new-value expressions shouldn't be resjunk */
			Assert(!tle->resjunk);

			if (attno <= InvalidAttrNumber) /* shouldn't happen */
				elog(ERROR, "system-column update is not supported");

			if (!deparseExpr((Expr *) tle->expr, &context))
				return false;
		}
#else
		int			col;

		/*
		 * We transmit only columns that were explicitly targets of the
		 * UPDATE, so as to avoid unnecessary data transmission.
		 */
		col = -1;
		while ((col = bms_next_member(rte->updatedCols, col)) >= 0)
		{
			/* bit numbers are offset by FirstLowInvalidHeapAttributeNumber */
			AttrNumber	attno = col + FirstLowInvalidHeapAttributeNumber;
			TargetEntry *tle;
			deparse_expr_cxt	context;

			initializeContext(fpinfo, root, foreignrel, foreignrel, &context);

			if (attno <= InvalidAttrNumber) /* shouldn't happen */
				elog(ERROR, "system-column update is not supported");

			tle = get_tle_by_resno(subplan->targetlist, attno);

			if (!tle)
				elog(ERROR, "attribute number %d not found in subplan targetlist",
					 attno);

			if (!deparseExpr((Expr *) tle->expr, &context))
				return false;

			targetAttrs = lappend_int(targetAttrs, attno);
		}
#endif
	}

	/*
	 * Ok, rewrite subplan so as to modify the foreign table directly.
	 */
	initStringInfo(&sql);

	/*
	 * Core code already has some lock on each rel being planned, so we can
	 * use NoLock here.
	 */
	rel = table_open(rte->relid, NoLock);

	/*
	 * Recall the qual clauses that must be evaluated remotely.  (These are
	 * bare clauses not RestrictInfos, but deparse.c's appendConditions()
	 * doesn't care.)
	 */
	remote_exprs = fpinfo->final_remote_exprs;

	/*
	 * Extract the relevant RETURNING list if any.
	 */
	if (plan->returningLists)
	{
		Bitmapset *attrs_used = NULL;

		returningList = (List *) list_nth(plan->returningLists, subplan_index);

		/*
		 * When performing an UPDATE/DELETE .. RETURNING on a join directly,
		 * we fetch from the foreign server any Vars specified in RETURNING
		 * that refer not only to the target relation but to non-target
		 * relations.  So we'll deparse them into the RETURNING clause of the
		 * remote query; use a targetlist consisting of them instead, which
		 * will be adjusted to be new fdw_scan_tlist of the foreign-scan plan
		 * node below.
		 */
		if (fscan->scan.scanrelid == 0)
			returningList = build_remote_returning(resultRelation, rel,
												   returningList);

		if (returningList != NIL)
		{
			bool	have_wholerow;

			/* get all the attributes mentioned there */
			pull_varattnos((Node *) returningList, resultRelation, &attrs_used);

			/* If there's a whole-row reference, we'll need all the columns. */
			have_wholerow = bms_is_member(0 - FirstLowInvalidHeapAttributeNumber,
										attrs_used);

			/* mark the corresponding columns as used */
			for (i=0; i<fpinfo->oraTable->ncols; ++i)
			{
				/* ignore columns that are not in the PostgreSQL table */
				if (fpinfo->oraTable->cols[i]->pgname == NULL)
					continue;

				if (have_wholerow ||
					bms_is_member(fpinfo->oraTable->cols[i]->pgattnum - FirstLowInvalidHeapAttributeNumber,
								  attrs_used))
				{
					/* throw an error if it is a LONG or LONG RAW column */
					if (fpinfo->oraTable->cols[i]->oratype == ORA_TYPE_LONGRAW
							|| fpinfo->oraTable->cols[i]->oratype == ORA_TYPE_LONG)
						ereport(ERROR,
								(errcode(ERRCODE_FDW_INVALID_DATA_TYPE),
								errmsg("columns with Oracle type LONG or LONG RAW cannot be used in RETURNING clause"),
								errdetail("Column \"%s\" of foreign table \"%s\" is of Oracle type LONG%s.",
									fpinfo->oraTable->cols[i]->pgname,
									fpinfo->oraTable->pgname,
									fpinfo->oraTable->cols[i]->oratype == ORA_TYPE_LONG ? "" : " RAW")));

					fpinfo->oraTable->cols[i]->used = 1;
				}
			}
		}
	}

	/*
	 * Construct the SQL command string.
	 */
	switch (operation)
	{
		case CMD_UPDATE:
			oracleDeparseDirectUpdateSql(&sql, root, resultRelation, rel,
								   foreignrel,
#if PG_VERSION_NUM >= 140000
								   processed_tlist,
#else
								   ((Plan *) fscan)->targetlist,
#endif
								   targetAttrs,
								   remote_exprs, &params_list,
								   returningList, &retrieved_attrs);
			break;
		case CMD_DELETE:
			oracleDeparseDirectDeleteSql(&sql, root, resultRelation, rel,
								   foreignrel,
								   remote_exprs, &params_list,
								   returningList, &retrieved_attrs);
			break;
		default:
			elog(ERROR, "unexpected operation: %d", (int) operation);
			break;
	}

	if (returningList != NIL)
		appendReturningClause(&sql, fpinfo);

#if PG_VERSION_NUM >= 140000
	/*
	 * Update the operation and target relation info.
	 */
	fscan->operation = operation;
	fscan->resultRelation = resultRelation;
#else
	/*
	 * Update the operation info.
	 */
	fscan->operation = operation;
#endif

	/*
	 * Update the fdw_exprs list that will be available to the executor.
	 */
	fscan->fdw_exprs = params_list;

	/*
	 * Update the fdw_private list that will be available to the executor.
	 * Items in the list must match enum FdwDirectModifyPrivateIndex, above.
	 */
	fpinfo->query = sql.data;
	elog(DEBUG1, "oracle_fdw: remote query is %s", sql.data);
	fscan->fdw_private = list_make3(serializePlanData(fpinfo),
									makeInteger(retrieved_attrs != NIL),
									makeInteger(plan->canSetTag));

	/*
	 * Update the foreign-join-related fields.
	 */
	if (fscan->scan.scanrelid == 0)
	{
		/* No need for the outer subplan. */
		fscan->scan.plan.lefttree = NULL;

		/* Build new fdw_scan_tlist if UPDATE/DELETE .. RETURNING. */
		if (returningList)
			rebuild_fdw_scan_tlist(fscan, returningList);
	}

	table_close(rel, NoLock);
	return true;
}

/*
 * oracleBeginDirectModify
 *		Prepare a direct foreign table modification
 */
static void
oracleBeginDirectModify(ForeignScanState *node, int eflags)
{
	ForeignScan *fsplan = (ForeignScan *) node->ss.ps.plan;
	EState	   *estate = node->ss.ps.state;
	struct OracleFdwState *dmstate;
	Index		rtindex;
	int			numParams;

	elog(DEBUG1, "oracle_fdw: begin direct modify");

	/* Get private info created by planner functions. */
	dmstate = deserializePlanData(list_nth(fsplan->fdw_private, 0));
	dmstate->has_returning = intVal(list_nth(fsplan->fdw_private, 1));
	dmstate->set_processed = intVal(list_nth(fsplan->fdw_private, 2));

	/* Initialize state variable */
	dmstate->rowcount = -1;	/* -1 means not set yet */
	dmstate->next_tuple = 0;

	/*
	 * We'll save private state in node->fdw_state.
	 */
	node->fdw_state = (void *) dmstate;

	/*
	 * Do nothing in EXPLAIN (no ANALYZE) case.
	 */
	if (eflags & EXEC_FLAG_EXPLAIN_ONLY)
		return;

	/*
	 * Identify which user to do the remote access as.  This should match what
	 * ExecCheckRTEPerms() does.
	 */
#if PG_VERSION_NUM >= 140000
	rtindex = node->resultRelInfo->ri_RangeTableIndex;
#else
	rtindex = estate->es_result_relation_info->ri_RangeTableIndex;
#endif

	/* Get info about foreign table. */
	if (fsplan->scan.scanrelid == 0)
		dmstate->rel = ExecOpenScanRelation(estate, rtindex, eflags);
	else
		dmstate->rel = node->ss.ss_currentRelation;

	/* Update the foreign-join-related fields. */
	if (fsplan->scan.scanrelid == 0)
	{
		/* Save info about foreign table. */
		dmstate->resultRel = dmstate->rel;

		/*
		 * Set dmstate->rel to NULL to teach get_returning_data() and
		 * make_tuple_from_result_row() that columns fetched from the remote
		 * server are described by fdw_scan_tlist of the foreign-scan plan
		 * node, not the tuple descriptor for the target relation.
		 */
		dmstate->rel = NULL;
	}

	/* Create context for per-tuple temp workspace. */
	dmstate->temp_cxt = AllocSetContextCreate(estate->es_query_cxt,
											  "oracle_fdw temporary data",
											  ALLOCSET_SMALL_SIZES);

	/* Prepare for input conversion of RETURNING results. */
	if (dmstate->has_returning)
	{
		/*
		 * When performing an UPDATE/DELETE .. RETURNING on a join directly,
		 * initialize a filter to extract an updated/deleted tuple from a scan
		 * tuple.
		 */
		if (fsplan->scan.scanrelid == 0)
			init_returning_filter(dmstate, fsplan->fdw_scan_tlist, rtindex);
	}

	/*
	 * Prepare for processing of parameters used in remote query, if any.
	 */
	numParams = list_length(fsplan->fdw_exprs);
	dmstate->numParams = numParams;
	if (numParams > 0)
		prepare_query_params(dmstate,
							 (PlanState *) node,
							 fsplan->fdw_exprs,
							 numParams);
}

/*
 * oracleIterateDirectModify
 *		Execute a direct foreign table modification
 */
static TupleTableSlot *
oracleIterateDirectModify(ForeignScanState *node)
{
	struct OracleFdwState *dmstate = (struct OracleFdwState *) node->fdw_state;
	EState	   *estate = node->ss.ps.state;
#if PG_VERSION_NUM >= 140000
	ResultRelInfo *resultRelInfo = node->resultRelInfo;
#else
	ResultRelInfo *resultRelInfo = estate->es_result_relation_info;
#endif

	elog(DEBUG1, "oracle_fdw: iterate direct modify");

	/*
	 * If this is the first call after Begin, execute the statement.
	 */
	if (dmstate->rowcount == -1)
	{
		execute_dml_stmt(node);
	}

	/*
	 * If the local query doesn't specify RETURNING, just clear tuple slot.
	 */
	if (!resultRelInfo->ri_projectReturning)
	{
		TupleTableSlot *slot = node->ss.ss_ScanTupleSlot;
		Instrumentation *instr = node->ss.ps.instrument;

		Assert(!dmstate->has_returning);

		/* Increment the command es_processed count if necessary. */
		if (dmstate->set_processed)
			estate->es_processed += dmstate->rowcount;

		/* Increment the tuple count for EXPLAIN ANALYZE if necessary. */
		if (instr)
			instr->tuplecount += dmstate->rowcount;

		return ExecClearTuple(slot);
	}

	/*
	 * Get the next RETURNING tuple.
	 */
	return get_returning_data(node);
}

/*
 * oracleEndDirectModify
 *		Finish a direct foreign table modification
 */
static void
oracleEndDirectModify(ForeignScanState *node)
{
	struct OracleFdwState *dmstate = (struct OracleFdwState *) node->fdw_state;

	/* if dmstate is NULL, we are in EXPLAIN; nothing to do */
	if (dmstate == NULL)
		return;

	/* release the Oracle session */
	if (dmstate->session)
	{
		oracleCloseStatement(dmstate->session);
		pfree(dmstate->session);
		dmstate->session = NULL;
	}

	/* MemoryContext will be deleted automatically. */
}

/*
 * oracleExplainDirectModify
 *		Produce extra output for EXPLAIN of a ForeignScan that modifies a
 *		foreign table directly
 */
static void
oracleExplainDirectModify(ForeignScanState *node, ExplainState *es)
{
	struct OracleFdwState *dmstate = (struct OracleFdwState *) node->fdw_state;

	if (es->verbose)
	{
		ExplainPropertyText("Remote SQL", dmstate->query, es);
	}
}


/*
 * getFdwState
 * 		Construct an OracleFdwState from the options of the foreign table.
 * 		Establish an Oracle connection and get a description of the
 * 		remote table.
 * 		"sample_percent" is set from the foreign table options.
 * 		"sample_percent" can be NULL, in that case it is not set.
 * 		"userid" determines the use to connect as; if invalid, the current
 * 		user is used.
 */
struct OracleFdwState
*getFdwState(Oid foreigntableid, double *sample_percent, Oid userid)
{
	struct OracleFdwState *fdwState = palloc0(sizeof(struct OracleFdwState));
	char *pgtablename = get_rel_name(foreigntableid);
	List *options;
	ListCell *cell;
	char *isolationlevel = NULL;
	char *dblink = NULL, *schema = NULL, *table = NULL, *maxlong = NULL,
		 *sample = NULL, *fetch = NULL, *nchar = NULL;
	long max_long;
	char *query = NULL, *tablename = NULL;

	/*
	 * Get all relevant options from the foreign table, the user mapping,
	 * the foreign server and the foreign data wrapper.
	 */
	oracleGetOptions(foreigntableid, userid, &options);
	foreach(cell, options)
	{
		DefElem *def = (DefElem *) lfirst(cell);
		if (strcmp(def->defname, OPT_NLS_LANG) == 0)
			fdwState->nls_lang = strVal(def->arg);
		if (strcmp(def->defname, OPT_DBSERVER) == 0)
			fdwState->dbserver = strVal(def->arg);
		if (strcmp(def->defname, OPT_ISOLATION_LEVEL) == 0)
			isolationlevel = strVal(def->arg);
		if (strcmp(def->defname, OPT_USER) == 0)
			fdwState->user = strVal(def->arg);
		if (strcmp(def->defname, OPT_PASSWORD) == 0)
			fdwState->password = strVal(def->arg);
		if (strcmp(def->defname, OPT_DBLINK) == 0)
			dblink = strVal(def->arg);
		if (strcmp(def->defname, OPT_SCHEMA) == 0)
			schema = strVal(def->arg);
		if (strcmp(def->defname, OPT_TABLE) == 0)
			table = strVal(def->arg);
		if (strcmp(def->defname, OPT_MAX_LONG) == 0)
			maxlong = strVal(def->arg);
		if (strcmp(def->defname, OPT_SAMPLE) == 0)
			sample = strVal(def->arg);
		if (strcmp(def->defname, OPT_PREFETCH) == 0)
			fetch = strVal(def->arg);
		if (strcmp(def->defname, OPT_NCHAR) == 0)
			nchar = strVal(def->arg);
	}

	/* set isolation_level (or use default) */
	if (isolationlevel == NULL)
		fdwState->isolation_level = DEFAULT_ISOLATION_LEVEL;
	else
		fdwState->isolation_level = getIsolationLevel(isolationlevel);

	/* convert "max_long" option to number or use default */
	if (maxlong == NULL)
		max_long = DEFAULT_MAX_LONG;
	else
		max_long = strtol(maxlong, NULL, 0);

	/* convert "sample_percent" to double */
	if (sample_percent != NULL)
	{
		if (sample == NULL)
			*sample_percent = 100.0;
		else
			*sample_percent = strtod(sample, NULL);
	}

	/* convert "prefetch" to number (or use default) */
	if (fetch == NULL)
		fdwState->prefetch = DEFAULT_PREFETCH;
	else
		fdwState->prefetch = (unsigned int)strtoul(fetch, NULL, 0);

	/* convert "nchar" option to boolean (or use "false") */
	if (nchar != NULL
		&& (pg_strcasecmp(nchar, "on") == 0
			|| pg_strcasecmp(nchar, "yes") == 0
			|| pg_strcasecmp(nchar, "true") == 0))
		fdwState->have_nchar = true;
	else
		fdwState->have_nchar = false;

	/* check if options are ok */
	if (table == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_FDW_OPTION_NAME_NOT_FOUND),
				errmsg("required option \"%s\" in foreign table \"%s\" missing", OPT_TABLE, pgtablename)));

	/* guess a good NLS_LANG environment setting */
	fdwState->nls_lang = guessNlsLang(fdwState->nls_lang);

	/* connect to Oracle database */
	fdwState->session = oracleGetSession(
		fdwState->dbserver,
		fdwState->isolation_level,
		fdwState->user,
		fdwState->password,
		fdwState->nls_lang,
		(int)fdwState->have_nchar,
		pgtablename,
		GetCurrentTransactionNestLevel()
	);

	/* create tablename and query */
	tablename = oracleCreateTableName(dblink, schema, table);
	query = oracleCreateQuery(tablename);

	/* get remote table description */
	fdwState->oraTable = oracleDescribe(fdwState->session, query, tablename, pgtablename, max_long);

	/* add PostgreSQL data to table description */
	getColumnData(foreigntableid, fdwState->oraTable);

	/* save max_long for re-build oraTable */
	fdwState->max_long = max_long;

	return fdwState;
}

/*
 * oracleGetOptions
 * 		Fetch the options for an oracle_fdw foreign table.
 * 		Returns a union of the options of the foreign data wrapper,
 * 		the foreign server, the user mapping and the foreign table,
 * 		in that order.  Column options are ignored.
 */
void
oracleGetOptions(Oid foreigntableid, Oid userid, List **options)
{
	ForeignTable *table;
	ForeignServer *server;
	UserMapping *mapping;
	ForeignDataWrapper *wrapper;

	/*
	 * Gather all data for the foreign table.
	 */
	table = GetForeignTable(foreigntableid);
	server = GetForeignServer(table->serverid);
	mapping = GetUserMapping(
				(userid != InvalidOid) ? userid : GetUserId(),
				table->serverid
			  );
	wrapper = GetForeignDataWrapper(server->fdwid);

	/* later options override earlier ones */
	*options = NIL;
	*options = list_concat(*options, wrapper->options);
	*options = list_concat(*options, server->options);
	if (mapping != NULL)
		*options = list_concat(*options, mapping->options);
	*options = list_concat(*options, table->options);
}

/*
 * getColumnData
 * 		Get PostgreSQL column name and number, data type and data type modifier.
 * 		Set oraTable->npgcols.
 * 		For PostgreSQL 9.2 and better, find the primary key columns and mark them in oraTable.
 */
void
getColumnData(Oid foreigntableid, struct oraTable *oraTable)
{
	Relation rel;
	TupleDesc tupdesc;
	int i, index;

	rel = table_open(foreigntableid, NoLock);
	tupdesc = rel->rd_att;

	/* number of PostgreSQL columns */
	oraTable->npgcols = tupdesc->natts;

	/* loop through foreign table columns */
	index = 0;
	for (i=0; i<tupdesc->natts; ++i)
	{
		Form_pg_attribute att_tuple = TupleDescAttr(tupdesc, i);
		List *options;
		ListCell *option;

		/* ignore dropped columns */
		if (att_tuple->attisdropped)
			continue;

		++index;
		/* get PostgreSQL column number and type */
		if (index <= oraTable->ncols)
		{
			oraTable->cols[index-1]->pgattnum = att_tuple->attnum;
			oraTable->cols[index-1]->pgtype = att_tuple->atttypid;
			oraTable->cols[index-1]->pgtypmod = att_tuple->atttypmod;
			oraTable->cols[index-1]->pgname = pstrdup(NameStr(att_tuple->attname));
		}

		/* loop through column options */
		options = GetForeignColumnOptions(foreigntableid, att_tuple->attnum);
		foreach(option, options)
		{
			DefElem *def = (DefElem *)lfirst(option);

			/* is it the "key" option and is it set to "true" ? */
			if (strcmp(def->defname, OPT_KEY) == 0 && optionIsTrue(strVal(def->arg)))
			{
				/* mark the column as primary key column */
				oraTable->cols[index-1]->pkey = 1;
			}
			else if (strcmp(def->defname, OPT_STRIP_ZEROS) == 0 && optionIsTrue(strVal(def->arg)))
				oraTable->cols[index-1]->strip_zeros = 1;
			else if (strcmp(def->defname, OPT_COLUMN_NAME) == 0 && strVal(def->arg))
				/* Get the Oracle column name  */
				strcpy(oraTable->cols[index-1]->name, quote_identifier(strVal(def->arg)));
		}
	}

	table_close(rel, NoLock);
}

/*
 * getColumnDataByTupdesc
 *
 * 		Get column data based on the tuple description of the scanning table.
 */
static void
getColumnDataByTupdesc(Relation rel, TupleDesc tupdesc, List *retrieved_attrs, struct oraTable *oraTable)
{
	int index;
	ListCell *lc;

	/* number of PostgreSQL columns */
	oraTable->npgcols = tupdesc->natts;

	/* loop through scan table columns */
	index = 0;
	foreach(lc, retrieved_attrs)
	{
		int			attnum = lfirst_int(lc) - 1;
		Form_pg_attribute att_tuple = TupleDescAttr(tupdesc, attnum);
		List *options;
		ListCell *option;

		/* ignore dropped columns */
		if (att_tuple->attisdropped)
			continue;

		++index;
		/* get PostgreSQL column number and type */
		if (index <= oraTable->ncols)
		{
			oraTable->cols[index-1]->pgattnum = att_tuple->attnum;
			oraTable->cols[index-1]->pgtype = att_tuple->atttypid;
			oraTable->cols[index-1]->pgtypmod = att_tuple->atttypmod;
			oraTable->cols[index-1]->pgname = pstrdup(NameStr(att_tuple->attname));
			oraTable->cols[index-1]->used = 1;
		}

		/* loop through column options, we can only get column option if node type is T_Var */
		if (rel != NULL && oraTable->cols[index-1]->node_type == T_Var)
		{
			Oid relid = RelationGetRelid(rel);

			options = GetForeignColumnOptions(relid, att_tuple->attnum);
			foreach(option, options)
			{
				DefElem *def = (DefElem *)lfirst(option);

				/* is it the "key" option and is it set to "true" ? */
				if (strcmp(def->defname, OPT_KEY) == 0 && optionIsTrue(strVal(def->arg)))
				{
					/* mark the column as primary key column */
					oraTable->cols[index-1]->pkey = 1;
				}
				else if (strcmp(def->defname, OPT_STRIP_ZEROS) == 0 && optionIsTrue(strVal(def->arg)))
					oraTable->cols[index-1]->strip_zeros = 1;
			}
		}
	}
}

/*
 * deparseFromExprForRel
 * 		Construct FROM clause for given relation.
 * 		The function constructs ... JOIN ... ON ... for join relation. For a base
 * 		relation it just returns the table name.
 * 		All tables get an alias based on the range table index.
 */
static void
deparseFromExprForRel(StringInfo buf, RelOptInfo *foreignrel, List **params_list, deparse_expr_cxt *context)
{
	struct OracleFdwState *fpinfo = (struct OracleFdwState *) foreignrel->fdw_private;
	bool use_alias = context->use_alias;
	Index ignore_rel = context->ignore_rel;
	List **ignore_conds = context->ignore_conds;

	if (IS_JOIN_REL(foreignrel))
	{
		/* join relation */
		RelOptInfo *rel_o = fpinfo->outerrel;
		RelOptInfo *rel_i = fpinfo->innerrel;
		StringInfoData join_sql_o;
		StringInfoData join_sql_i;
		bool		outerrel_is_target = false;
		bool		innerrel_is_target = false;

		if (ignore_rel > 0 && bms_is_member(ignore_rel, foreignrel->relids))
		{
			/*
			 * If this is an inner join, add joinclauses to *ignore_conds and
			 * set it to empty so that those can be deparsed into the WHERE
			 * clause.  Note that since the target relation can never be
			 * within the nullable side of an outer join, those could safely
			 * be pulled up into the WHERE clause (see foreign_join_ok()).
			 * Note also that since the target relation is only inner-joined
			 * to any other relation in the query, all conditions in the join
			 * tree mentioning the target relation could be deparsed into the
			 * WHERE clause by doing this recursively.
			 */
			if (fpinfo->jointype == JOIN_INNER)
			{
				*ignore_conds = list_concat(*ignore_conds,
											fpinfo->joinclauses);
				fpinfo->joinclauses = NIL;
			}

			/*
			 * Check if either of the input relations is the target relation.
			 */
			if (rel_o->relid == ignore_rel)
				outerrel_is_target = true;
			else if (rel_i->relid == ignore_rel)
				innerrel_is_target = true;
		}

		/* Deparse outer relation if not the target relation. */
		if (!outerrel_is_target)
		{
			initStringInfo(&join_sql_o);
			oracleDeparseRangeTblRef(&join_sql_o, rel_o,
							   fpinfo->make_outerrel_subquery,
							   params_list, context);

			/*
			 * If inner relation is the target relation, skip deparsing it.
			 * Note that since the join of the target relation with any other
			 * relation in the query is an inner join and can never be within
			 * the nullable side of an outer join, the join could be
			 * interchanged with higher-level joins (cf. identity 1 on outer
			 * join reordering shown in src/backend/optimizer/README), which
			 * means it's safe to skip the target-relation deparsing here.
			 */
			if (innerrel_is_target)
			{
				Assert(fpinfo->jointype == JOIN_INNER);
				Assert(fpinfo->joinclauses == NIL);
				appendBinaryStringInfo(buf, join_sql_o.data, join_sql_o.len);
				return;
			}
		}

		/* Deparse inner relation if not the target relation. */
		if (!innerrel_is_target)
		{
			initStringInfo(&join_sql_i);
			oracleDeparseRangeTblRef(&join_sql_i, rel_i,
							   fpinfo->make_innerrel_subquery,
							   params_list, context);

			/*
			 * If outer relation is the target relation, skip deparsing it.
			 * See the above note about safety.
			 */
			if (outerrel_is_target)
			{
				Assert(fpinfo->jointype == JOIN_INNER);
				Assert(fpinfo->joinclauses == NIL);
				appendBinaryStringInfo(buf, join_sql_i.data, join_sql_i.len);
				return;
			}
		}

		/* Neither of the relations is the target relation. */
		Assert(!outerrel_is_target && !innerrel_is_target);

		/*
		 * For a join relation FROM clause entry is deparsed as
		 *
		 * (outer relation) <join type> (inner relation) ON joinclauses
		 */
		appendStringInfo(buf, "(%s %s JOIN %s ON ",
						join_sql_o.data,
						get_jointype_name(fpinfo->jointype),
						join_sql_i.data
		);

		/* we can only get here if the join is pushed down, so there are join clauses */
		Assert(fpinfo->joinclauses);
		appendConditions(fpinfo->joinclauses, context);

		/* End the FROM clause entry. */
		appendStringInfo(buf, ")");
	}
	else
	{
		appendStringInfo(buf, " %s", fpinfo->oraTable->name);

		/*
		 * Add a unique alias to avoid any conflict in relation names due to
		 * pulled up subqueries in the query being built for a pushed down
		 * join.
		 */
		if (use_alias)
			appendStringInfo(buf, " %s%d", REL_ALIAS_PREFIX, foreignrel->relid);
	}
}

#ifdef JOIN_API
/*
 * appendConditions
 * 		Deparse conditions from the provided list and append them to buf.
 * 		The conditions in the list are assumed to be ANDed.
 * 		This function is used to deparse JOIN ... ON clauses.
 */
static void
appendConditions(List *exprs, deparse_expr_cxt *context)
{
	int nestlevel;
	ListCell *lc;
	bool is_first = true;
	StringInfo buf = context->buf;
	char *condition;

	/* Make sure any constants in the exprs are printed portably */
	nestlevel = set_transmission_modes();

	foreach(lc, exprs)
	{
		Expr  *expr = (Expr *) lfirst(lc);

		/* extract clause from RestrictInfo, if required */
		if (IsA(expr, RestrictInfo))
		{
			RestrictInfo *ri = (RestrictInfo *) expr;
			expr = ri->clause;
		}

		/* connect expressions with AND */
		if (!is_first)
			appendStringInfo(buf, " AND ");

		/* deparse and append a join condition */
		condition = deparseExpr(expr, context);
		appendStringInfo(buf, "%s", condition);

		is_first = false;
	}

	reset_transmission_modes(nestlevel);
}

/*
 * foreign_join_ok
 * 		Assess whether the join between inner and outer relations can be pushed down
 * 		to the foreign server.
 */
static bool
foreign_join_ok(PlannerInfo *root, RelOptInfo *joinrel, JoinType jointype,
								RelOptInfo *outerrel, RelOptInfo *innerrel,
								JoinPathExtraData *extra)
{
	struct OracleFdwState *fdwState;
	struct OracleFdwState *fdwState_o;
	struct OracleFdwState *fdwState_i;

	ListCell   *lc;
	List	   *joinclauses;   /* join quals */
	List	   *otherclauses;  /* pushed-down (other) quals */

	deparse_expr_cxt context;

	/* we support pushing down INNER/OUTER joins */
	if (jointype != JOIN_INNER && jointype != JOIN_LEFT &&
		jointype != JOIN_RIGHT && jointype != JOIN_FULL)
		return false;

	fdwState = (struct OracleFdwState *) joinrel->fdw_private;
	fdwState_o = (struct OracleFdwState *) outerrel->fdw_private;
	fdwState_i = (struct OracleFdwState *) innerrel->fdw_private;

	/* init context */
	initializeContext(fdwState, root, joinrel, joinrel, &context);

	if (!fdwState_o || !fdwState_o->pushdown_safe ||
		!fdwState_i || !fdwState_i->pushdown_safe)
		return false;

	fdwState->outerrel = outerrel;
	fdwState->innerrel = innerrel;
	fdwState->jointype = jointype;

	/*
	 * If joining relations have local conditions, those conditions are
	 * required to be applied before joining the relations. Hence the join can
	 * not be pushed down.
	 */
	if (fdwState_o->local_conds || fdwState_i->local_conds)
		return false;

	/*
	 * Merge FDW options.  We might be tempted to do this after we have deemed
	 * the foreign join to be OK.  But we must do this beforehand so that we
	 * know which quals can be evaluated on the foreign server, which might
	 * depend on shippable_extensions.
	 */
	fdwState->server = fdwState_o->server;
	merge_fdw_state(fdwState, fdwState_o, fdwState_i);

	/* separate restrict list into join quals and pushed-down (other) quals from extra->restrictlist */
	joinclauses = NIL;
	fdwState->joinclauses = NIL;
	if (IS_OUTER_JOIN(jointype))
	{
		extract_actual_join_clauses(extra->restrictlist, joinrel->relids, &joinclauses, &otherclauses);

		/* CROSS JOIN (T1 LEFT/RIGHT/FULL JOIN T2 ON true) is not pushed down */
		if (joinclauses == NIL)
		{
			return false;
		}

		/* join quals must be safe to push down */
		foreach(lc, joinclauses)
		{
			Expr *expr = (Expr *) lfirst(lc);

			if (!deparseExpr(expr, &context))
				return false;
		}

		/* save the join clauses, for later use */
		fdwState->joinclauses = joinclauses;
	}
	else
	{
		/*
		 * Unlike an outer join, for inner join, the join result contains only
		 * the rows which satisfy join clauses, similar to the other clause.
		 * Hence all clauses can be treated the same.
		 *
		 * Note that all join conditions will become remote_conds and
		 * eventually joinclauses again.
		 */
		otherclauses = extract_actual_clauses(extra->restrictlist, false);
		joinclauses = NIL;
	}

	/*
	 * If there is a PlaceHolderVar that needs to be evaluated at a lower level
	 * than the complete join relation, it may happen that a reference from
	 * outside is wrongly evaluated to a non-NULL value.
	 * This can happen if the reason for the value to be NULL is that it comes from
	 * the nullable side of an outer join.
	 * So we don't push down the join in this case - if PostgreSQL performs the join,
	 * it will evaluate the placeholder correctly.
	 */
	foreach(lc, root->placeholder_list)
	{
		PlaceHolderInfo *phinfo = lfirst(lc);
		Relids      relids;

#if PG_VERSION_NUM < 110000
		relids = joinrel->relids;
#else
		/* PlaceHolderInfo refers to parent relids, not child relids. */
		relids = IS_OTHER_REL(joinrel) ?
				joinrel->top_parent_relids : joinrel->relids;
#endif  /* PG_VERSION_NUM */

		if (bms_is_subset(phinfo->ph_eval_at, relids) &&
			bms_nonempty_difference(relids, phinfo->ph_eval_at))
		{
			return false;
		}
	}

	/*
	 * For inner joins, "otherclauses" contains now the join conditions.
	 * For outer joins, it means these Restrictinfos were pushed down from other relation.
	 *
	 * Check which ones can be pushed down to remote server.
	 */
	foreach(lc, otherclauses)
	{
		Expr *expr = (Expr *) lfirst(lc);
		RestrictInfo *rinfo;

		/*
		 * Currently, the core code doesn't wrap havingQuals in
		 * RestrictInfos, so we must make our own.
		 */
		Assert(!IsA(expr, RestrictInfo));
		rinfo = make_restrictinfo(root,
								  expr,
								  true,
								  false,
								  false,
								  root->qual_security_level,
								  joinrel->relids,
								  NULL,
								  NULL);

		if (deparseExpr(expr, &context))
			fdwState->remote_conds = lappend(fdwState->remote_conds, rinfo);
		else
			fdwState->local_conds = lappend(fdwState->local_conds, rinfo);
	}

	/*
	 * Only push down joins for which all join conditions can be pushed down.
	 *
	 * For an INNER join it would be ok to only push own some of the join
	 * conditions and evaluate the others locally, but we cannot be certain
	 * that such a plan is a good or even a feasible one:
	 * With one of the join conditions missing in the pushed down query,
	 * it could be that the "intermediate" join result fetched from the Oracle
	 * side has many more rows than the complete join result.
	 *
	 * We could rely on estimates to see how many rows are returned from such
	 * a join where not all join conditions can be pushed down, but we choose
	 * the safe road of not pushing down such joins at all.
	 */

	if (!IS_OUTER_JOIN(jointype))
	{
		/* for an inner join, we use all or nothing approach */
		if (fdwState->local_conds != NIL)
			return false;

		/* CROSS JOIN (T1 JOIN T2 ON true) is not pushed down */
		if (fdwState->remote_conds == NIL)
			return false;
	}

	/*
	 * By default, both the input relations are not required to be deparsed as
	 * subqueries, but there might be some relations covered by the input
	 * relations that are required to be deparsed as subqueries, so save the
	 * relids of those relations for later use by the deparser.
	 */
	fdwState->make_outerrel_subquery = false;
	fdwState->make_innerrel_subquery = false;
	Assert(bms_is_subset(fdwState_o->lower_subquery_rels, outerrel->relids));
	Assert(bms_is_subset(fdwState_i->lower_subquery_rels, innerrel->relids));
	fdwState->lower_subquery_rels = bms_union(fdwState_o->lower_subquery_rels,
											fdwState_i->lower_subquery_rels);


	/*
	 * Pull the other remote conditions from the joining relations into join
	 * clauses or other remote clauses (remote_conds) of this relation
	 * wherever possible. This avoids building subqueries at every join step,
	 * which is not currently supported by the deparser logic.
	 *
	 * For an INNER join, clauses from both the relations are added to the
	 * other remote clauses.
	 *
	 * For LEFT and RIGHT OUTER join, the clauses from the outer side are added
	 * to remote_conds since those can be evaluated after the join is evaluated.
	 * The clauses from inner side are added to the joinclauses, since they
	 * need to evaluated while constructing the join.
	 *
	 * For a FULL OUTER JOIN, the other clauses from either relation can not
	 * be added to the joinclauses or remote_conds, since each relation acts
	 * as an outer relation for the other. Consider such full outer join as
	 * unshippable because of the reasons mentioned above in this comment.
	 *
	 * The joining sides can not have local conditions, thus no need to test
	 * shippability of the clauses being pulled up.
	 */
	switch (jointype)
	{
		case JOIN_INNER:
			fdwState->remote_conds = list_concat(fdwState->remote_conds,
										  list_copy(fdwState_i->remote_conds));
			fdwState->remote_conds = list_concat(fdwState->remote_conds,
										  list_copy(fdwState_o->remote_conds));
			break;

		case JOIN_LEFT:
			fdwState->joinclauses = list_concat(fdwState->joinclauses,
										  list_copy(fdwState_i->remote_conds));
			fdwState->remote_conds = list_concat(fdwState->remote_conds,
										  list_copy(fdwState_o->remote_conds));
			break;

		case JOIN_RIGHT:
			fdwState->joinclauses = list_concat(fdwState->joinclauses,
										  list_copy(fdwState_o->remote_conds));
			fdwState->remote_conds = list_concat(fdwState->remote_conds,
										  list_copy(fdwState_i->remote_conds));
			break;

		case JOIN_FULL:
			if (fdwState_i->remote_conds || fdwState_o->remote_conds)
				return false;

			break;

		default:
			/* Should not happen, we have just checked this above */
			elog(ERROR, "unsupported join type %d", jointype);
	}

	/*
	 * For an inner join, all restrictions can be treated alike. Treating the
	 * pushed down conditions as join conditions allows a top level full outer
	 * join to be deparsed without requiring subqueries.
	 */
	if (jointype == JOIN_INNER)
	{
		/* for an inner join, remote_conds has all join conditions */
		Assert(!fdwState->joinclauses);
		fdwState->joinclauses = fdwState->remote_conds;
		fdwState->remote_conds = NIL;
	}

	/* set fetch size to minimum of the joining sides */
	if (fdwState_o->prefetch < fdwState_i->prefetch)
		fdwState->prefetch = fdwState_o->prefetch;
	else
		fdwState->prefetch = fdwState_i->prefetch;

	foreach(lc, pull_var_clause((Node *)joinrel->reltarget->exprs, PVC_RECURSE_PLACEHOLDERS))
	{
		Var *var = (Var *) lfirst(lc);

		Assert(IsA(var, Var));

		/*
		 * Whole-row references and system columns are not pushed down.
		 * ToDo: support whole-row by creating oraColumns for that.
		 */
		if (var->varattno <= 0)
			return false;
	}

	/* Mark that this join can be pushed down safely */
	fdwState->pushdown_safe = true;

	/*
	 * Set # of retrieved rows and cached relation costs to some negative
	 * value, so that we can detect when they are set to some sensible values,
	 * during one (usually the first) of the calls to estimate_path_cost_size.
	 */
	fdwState->retrieved_rows = -1;
	fdwState->rel_startup_cost = -1;
	fdwState->rel_total_cost = -1;

	/*
	 * Set the string describing this join relation to be used in EXPLAIN
	 * output of corresponding ForeignScan.  Note that the decoration we add
	 * to the base relation names mustn't include any digits, or it'll confuse
	 * oracleExplainForeignScan.
	 */
	fdwState->relation_name = psprintf("(%s) %s JOIN (%s)",
									 fdwState_o->relation_name,
									 get_jointype_name(fdwState->jointype),
									 fdwState_i->relation_name);

	/*
	 * Set the relation index.  This is defined as the position of this
	 * joinrel in the join_rel_list list plus the length of the rtable list.
	 * Note that since this joinrel is at the end of the join_rel_list list
	 * when we are called, we can get the position by list_length.
	 */
	Assert(fdwState->relation_index == 0);	/* shouldn't be set yet */
	fdwState->relation_index =
		list_length(root->parse->rtable) + list_length(root->join_rel_list);

	return true;
}

/* Output join name for given join type */
const char *
get_jointype_name(JoinType jointype)
{
	switch (jointype)
	{
		case JOIN_INNER:
			return "INNER";

		case JOIN_LEFT:
			return "LEFT";

		case JOIN_RIGHT:
			return "RIGHT";

		case JOIN_FULL:
			return "FULL";

		default:
			/* Shouldn't come here, but protect from buggy code. */
			elog(ERROR, "unsupported join type %d", jointype);
	}

	/* Keep compiler happy */
	return NULL;
}

/*
 * Build the targetlist for given relation to be deparsed as SELECT clause.
 *
 * The output targetlist contains the columns that need to be fetched from the
 * foreign server for the given relation.
 */
static List *
build_tlist_to_deparse(RelOptInfo *foreignrel)
{
	List *tlist = NIL;
	struct OracleFdwState *fdwState = (struct OracleFdwState *)foreignrel->fdw_private;
	ListCell   *lc;

	/*
	 * For an upper relation, we have already built the target list while
	 * checking shippability, so just return that.
	 */
	if (IS_UPPER_REL(foreignrel))
		return fdwState->grouped_tlist;

	/*
	 * We require columns specified in foreignrel->reltarget->exprs and those
	 * required for evaluating the local conditions.
	 */
	tlist = add_to_flat_tlist(tlist,
							  pull_var_clause((Node *) foreignrel->reltarget->exprs,
											  PVC_RECURSE_PLACEHOLDERS));

	foreach(lc, fdwState->local_conds)
	{
		RestrictInfo *rinfo = lfirst_node(RestrictInfo, lc);

		tlist = add_to_flat_tlist(tlist,
								pull_var_clause((Node *) rinfo->clause,
												PVC_RECURSE_PLACEHOLDERS));
	}

	return tlist;
}

#endif  /* JOIN_API */

/*
 * acquireSampleRowsFunc
 * 		Perform a sequential scan on the Oracle table and return a sampe of rows.
 * 		All LOB values are truncated to WIDTH_THRESHOLD+1 because anything
 * 		exceeding this is not used by compute_scalar_stats().
 */
int
acquireSampleRowsFunc(Relation relation, int elevel, HeapTuple *rows, int targrows, double *totalrows, double *totaldeadrows)
{
	int collected_rows = 0, i;
	struct OracleFdwState *fdw_state;
	bool first_column = true;
	StringInfoData query;
	TupleDesc tupDesc = RelationGetDescr(relation);
	Datum *values = (Datum *)palloc(tupDesc->natts * sizeof(Datum));
	bool *nulls = (bool *)palloc(tupDesc->natts * sizeof(bool));
	double rstate, rowstoskip = -1, sample_percent;
	MemoryContext old_cxt, tmp_cxt;

	elog(DEBUG1, "oracle_fdw: analyze foreign table %d", RelationGetRelid(relation));

	*totalrows = 0;

	/* create a memory context for short-lived data in convertTuple() */
	tmp_cxt = AllocSetContextCreate(CurrentMemoryContext,
								"oracle_fdw temporary data",
								ALLOCSET_SMALL_SIZES);

	/* Prepare for sampling rows */
	rstate = anl_init_selection_state(targrows);

	/*
	 * Get connection options, connect and get the remote table description.
	 * Always use the user mapping for the current user.
	 */
	fdw_state = getFdwState(RelationGetRelid(relation), &sample_percent, InvalidOid);
	fdw_state->paramList = NULL;
	fdw_state->rowcount = 0;
	fdw_state->next_tuple = 0;		/* only use for Direct Modification */

	/* construct query */
	initStringInfo(&query);
	appendStringInfo(&query, "SELECT ");

	/* loop columns */
	for (i=0; i<fdw_state->oraTable->ncols; ++i)
	{
		/* don't get LONG, LONG RAW and untranslatable values */
		if (fdw_state->oraTable->cols[i]->oratype == ORA_TYPE_LONG
				|| fdw_state->oraTable->cols[i]->oratype == ORA_TYPE_LONGRAW
				|| fdw_state->oraTable->cols[i]->oratype == ORA_TYPE_GEOMETRY
				|| fdw_state->oraTable->cols[i]->oratype == ORA_TYPE_OTHER)
		{
			fdw_state->oraTable->cols[i]->used = 0;
		}
		else
		{
			/* all columns are used */
			fdw_state->oraTable->cols[i]->used = 1;

			/* allocate memory for return value */
			fdw_state->oraTable->cols[i]->val = (char *)palloc(fdw_state->oraTable->cols[i]->val_size);
			fdw_state->oraTable->cols[i]->val_len = (unsigned int *)palloc0(sizeof(unsigned int));
			fdw_state->oraTable->cols[i]->val_len4 = (unsigned int *)palloc0(sizeof(unsigned int));
			fdw_state->oraTable->cols[i]->val_null = (short *)palloc(sizeof(short));
			memset(fdw_state->oraTable->cols[i]->val_null, 1, sizeof(short));

			if (first_column)
				first_column = false;
			else
				appendStringInfo(&query, ", ");

			/* append column name */
			appendStringInfo(&query, "%s", fdw_state->oraTable->cols[i]->name);
		}
	}

	/* if there are no columns, use NULL */
	if (first_column)
		appendStringInfo(&query, "NULL");

	/* append Oracle table name */
	appendStringInfo(&query, " FROM %s", fdw_state->oraTable->name);

	/* append SAMPLE clause if appropriate */
	if (sample_percent < 100.0)
		appendStringInfo(&query, " SAMPLE BLOCK (%f)", sample_percent);

	fdw_state->query = query.data;
	elog(DEBUG1, "oracle_fdw: remote query is %s", fdw_state->query);

	/* get PostgreSQL column data types, check that they match Oracle's */
	for (i=0; i<fdw_state->oraTable->ncols; ++i)
		if (fdw_state->oraTable->cols[i]->pgname != NULL
				&& fdw_state->oraTable->cols[i]->used)
			checkDataType(
				fdw_state->oraTable->cols[i]->oratype,
				fdw_state->oraTable->cols[i]->scale,
				fdw_state->oraTable->cols[i]->pgtype,
				fdw_state->oraTable->pgname,
				fdw_state->oraTable->cols[i]->pgname
			);

	/* loop through query results */
	while(oracleIsStatementOpen(fdw_state->session)
			? oracleFetchNext(fdw_state->session)
			: (oraclePrepareQuery(fdw_state->session, fdw_state->query, fdw_state->oraTable, fdw_state->prefetch),
				oracleExecuteQuery(fdw_state->session, fdw_state->oraTable, fdw_state->paramList)))
	{
		/* allow user to interrupt ANALYZE */
		vacuum_delay_point();

		++fdw_state->rowcount;

		if (collected_rows < targrows)
		{
			/* the first "targrows" rows are added as samples */

			/* use a temporary memory context during convertTuple */
			old_cxt = MemoryContextSwitchTo(tmp_cxt);
			convertTuple(fdw_state, values, nulls, true);
			MemoryContextSwitchTo(old_cxt);

			rows[collected_rows++] = heap_form_tuple(tupDesc, values, nulls);
			MemoryContextReset(tmp_cxt);
		}
		else
		{
			/*
			 * Skip a number of rows before replacing a random sample row.
			 * A more detailed description of the algorithm can be found in analyze.c
			 */
			if (rowstoskip < 0)
				rowstoskip = anl_get_next_S(*totalrows, targrows, &rstate);

			if (rowstoskip <= 0)
			{
				int k = (int)(targrows * anl_random_fract());

				heap_freetuple(rows[k]);

				/* use a temporary memory context during convertTuple */
				old_cxt = MemoryContextSwitchTo(tmp_cxt);
				convertTuple(fdw_state, values, nulls, true);
				MemoryContextSwitchTo(old_cxt);

				rows[k] = heap_form_tuple(tupDesc, values, nulls);
				MemoryContextReset(tmp_cxt);
			}
		}
	}

	MemoryContextDelete(tmp_cxt);

	*totalrows = (double)fdw_state->rowcount / sample_percent * 100.0;
	*totaldeadrows = 0;

	/* report report */
	ereport(elevel, (errmsg("\"%s\": table contains %lu rows; %d rows in sample",
			RelationGetRelationName(relation), fdw_state->rowcount, collected_rows)));

	return collected_rows;
}

/*
 * appendAsType
 * 		Append "s" to "dest", adding appropriate casts for datetime "type".
 */
void
appendAsType(StringInfoData *dest, const char *s, Oid type)
{
	switch (type)
	{
		case DATEOID:
			appendStringInfo(dest, "CAST (%s AS DATE)", s);
			break;
		case TIMESTAMPOID:
			appendStringInfo(dest, "CAST (%s AS TIMESTAMP)", s);
			break;
		case TIMESTAMPTZOID:
			appendStringInfo(dest, "CAST (%s AS TIMESTAMP WITH TIME ZONE)", s);
			break;
		default:
			appendStringInfo(dest, "%s", s);
	}
}

/*
 * castNullAsType
 *
 * Sometimes, NULL is passed to the remote query, so when building oraTable for scanning table,
 * oracle api don't understand its type. We have to specify type of NULL column. The list below
 * might be updated more types. However, oracle still accept "SELECT NULL FROM tbl" without casting,
 * so does the default case.
 */
void
castNullAsType(StringInfoData *dest, Oid type)
{
	switch (type)
	{
		case INT2OID:
			appendStringInfo(dest, "CAST(NULL AS NUMBER(5))");
			break;
		case INT4OID:
			appendStringInfo(dest, "CAST(NULL AS NUMBER(10))");
			break;
		case INT8OID:
			appendStringInfo(dest, "CAST(NULL AS NUMBER(19))");
			break;
		case FLOAT4OID:
			appendStringInfo(dest, "CAST(NULL AS BINARY_FLOAT)");
			break;
		case FLOAT8OID:
			appendStringInfo(dest, "CAST(NULL AS BINARY_DOUBLE)");
			break;
		default:
			appendStringInfo(dest, "NULL");
			break;
	}
}

/*
 * This macro is used by deparseExpr to identify PostgreSQL
 * types that can be translated to Oracle SQL.
 */
#define canHandleType(x) ((x) == TEXTOID || (x) == CHAROID || (x) == BPCHAROID \
			|| (x) == VARCHAROID || (x) == NAMEOID || (x) == INT8OID || (x) == INT2OID \
			|| (x) == INT4OID || (x) == OIDOID || (x) == FLOAT4OID || (x) == FLOAT8OID \
			|| (x) == NUMERICOID || (x) == DATEOID || (x) == TIMESTAMPOID || (x) == TIMESTAMPTZOID \
			|| (x) == INTERVALOID || (x) == UUIDOID  || (x) == BYTEAOID)

/*
 * deparseExpr
 * 		Create and return an Oracle SQL string from "expr".
 * 		Returns NULL if that is not possible, else a palloc'ed string.
 * 		As a side effect, all Params incorporated in the WHERE clause
 * 		will be stored in "params".
 */
char *
deparseExpr(Expr *expr, deparse_expr_cxt *context)
{
	char *opername, *left, *right, *arg, oprkind;
	char parname[10];
	Param *param;
	Const *constant;
	OpExpr *oper;
	ScalarArrayOpExpr *arrayoper;
	CaseExpr *caseexpr;
	BoolExpr *boolexpr;
	CoalesceExpr *coalesceexpr;
	CoerceViaIO *coerce;
	Var *variable;
	FuncExpr *func;
	Expr *rightexpr;
	ArrayExpr *array;
	ArrayCoerceExpr *arraycoerce;
	MinMaxExpr *minmaxexpr;
#if PG_VERSION_NUM >= 100000
	SQLValueFunction *sqlvalfunc;
#endif
	regproc typoutput;
	HeapTuple tuple;
	ListCell *cell;
	StringInfoData result;
	Oid leftargtype, rightargtype, schema;
	oraType oratype;
	ArrayIterator iterator;
	Datum datum;
	bool first_arg, isNull;
	int index;
	StringInfoData alias;
	const struct oraTable *var_table;  /* oraTable that belongs to a Var */

	RelOptInfo *foreignrel = context->foreignrel;
	struct oraTable *oraTable = context->oraTable;
	List **params = context->params_list;
	bool qualify_col = false;
	char *origin_function;

	/* Need do nothing for empty subexpressions */
	if (expr == NULL)
		return "do nothing";

	switch(expr->type)
	{
		case T_Const:
			constant = (Const *)expr;
			if (constant->constisnull)
			{
				/* only translate NULLs of a type Oracle can handle */
				if (canHandleType(constant->consttype))
				{
					initStringInfo(&result);

					/*
					 * for creating oraTable, to get attributes of remote table we need to define output datatype
					 * todo: Need to add more types in appendAsType
					 */
					castNullAsType(&result, constant->consttype);
				}
				else
					return NULL;
			}
			else
			{
				/* get a string representation of the value */
				char *c = datumToString(constant->constvalue, constant->consttype);
				if (c == NULL)
					return NULL;
				else
				{
					initStringInfo(&result);
					appendStringInfo(&result, "%s", c);
				}
			}
			break;
		case T_Param:
			param = (Param *)expr;

			/* don't try to handle interval parameters */
			if (! canHandleType(param->paramtype) || param->paramtype == INTERVALOID)
				return NULL;

			/*
			 * If it's a MULTIEXPR Param, punt.  We can't tell from here
			 * whether the referenced sublink/subplan contains any remote
			 * Vars; if it does, handling that is too complicated to
			 * consider supporting at present.  Fortunately, MULTIEXPR
			 * Params are not reduced to plain PARAM_EXEC until the end of
			 * planning, so we can easily detect this case.  (Normal
			 * PARAM_EXEC Params are safe to ship because their values
			 * come from somewhere else in the plan tree; but a MULTIEXPR
			 * references a sub-select elsewhere in the same targetlist,
			 * so we'd be on the hook to evaluate it somehow if we wanted
			 * to handle such cases as direct foreign updates.)
			 */
			if (param->paramkind == PARAM_MULTIEXPR)
				return NULL;

			/* find the index in the parameter list */
			index = 0;
			foreach(cell, *params)
			{
				++index;
				if (equal(param, (Node *)lfirst(cell)))
					break;
			}
			if (cell == NULL)
			{
				/* add the parameter to the list */
				++index;
				*params = lappend(*params, param);
			}

			/* parameters will be called :p1, :p2 etc. */
			snprintf(parname, 10, ":p%d", index);
			initStringInfo(&result);
			appendAsType(&result, parname, param->paramtype);

			break;
		case T_Var:
			variable = (Var *)expr;
			var_table = NULL;

			/* check if the variable belongs to one of our foreign tables */
#ifdef JOIN_API
			if (IS_SIMPLE_REL(foreignrel))
			{
#endif  /* JOIN_API */
				if (variable->varno == foreignrel->relid && variable->varlevelsup == 0)
					var_table = oraTable;
			}
#ifdef JOIN_API
			else if (IS_UPPER_REL(foreignrel))
			{
				if (bms_is_member(variable->varno, context->scanrel->relids) && variable->varlevelsup == 0)
				{
					struct OracleFdwState *fdwState = (struct OracleFdwState *) foreignrel->fdw_private;

					/* Aggregation with JOIN */
					if (fdwState->outerrel && IS_JOIN_REL(fdwState->outerrel))
					{
						var_table = getOraTableFromJoinRel(variable, fdwState->outerrel);
					}
					else
						var_table = fdwState->oraTable;
				}
			}
			else
			{
				var_table = getOraTableFromJoinRel(variable, foreignrel);
			}
#endif  /* JOIN_API */

			if (var_table)
			{
				/* the variable belongs to a foreign table, replace it with the name */

				/* we cannot handle system columns */
				if (variable->varattno < 1)
					return NULL;

				/*
				 * Allow boolean columns here.
				 * They will be rendered as ("COL" <> 0).
				 */
				if (! (canHandleType(variable->vartype) || variable->vartype == BOOLOID))
					return NULL;

				/* get var_table column index corresponding to this column (-1 if none) */
				index = var_table->ncols - 1;
				while (index >= 0 && var_table->cols[index]->pgattnum != variable->varattno)
					--index;

				/* if no Oracle column corresponds, translate as NULL */
				if (index == -1)
				{
					initStringInfo(&result);

					/* for creating oraTable, to get attributes of remote table we need to specify output datatype */
					castNullAsType(&result, variable->vartype);
					break;
				}

				/*
				 * Don't try to convert a column reference if the type is
				 * converted from a non-string type in Oracle to a string type
				 * in PostgreSQL because functions and operators won't work the same.
				 */
				oratype = var_table->cols[index]->oratype;
				if ((variable->vartype == TEXTOID
						|| variable->vartype == BPCHAROID
						|| variable->vartype == VARCHAROID)
						&& oratype != ORA_TYPE_VARCHAR2
						&& oratype != ORA_TYPE_CHAR
						&& oratype != ORA_TYPE_NVARCHAR2
						&& oratype != ORA_TYPE_NCHAR
						&& oratype != ORA_TYPE_CLOB)
					return NULL;

				/* Oracle does not support aggregation with CLOB */
				if (context->handle_aggref && oratype == ORA_TYPE_CLOB)
				{
					context->handle_aggref = false;
					return NULL;
				}

				/* Oracle does not handle string comparison with CLOB */
				if (context->string_comparison && oratype == ORA_TYPE_CLOB)
				{
					context->string_comparison = false;
					return NULL;
				}

				/* dont't pushdown length(LONG RAW) */
				if (variable->vartype == BYTEAOID
						&& oratype == ORA_TYPE_LONGRAW
						&& context->handle_length_func)
					return NULL;

				initStringInfo(&result);

				/* work around the lack of booleans in Oracle */
				if (variable->vartype == BOOLOID)
				{
					appendStringInfo(&result, "(");
				}

				/* qualify with an alias based on the range table index */
				initStringInfo(&alias);
				qualify_col = (bms_membership(context->scanrel->relids) == BMS_MULTIPLE);
				if (qualify_col)
					ADD_REL_QUALIFIER(&alias, var_table->cols[index]->varno);

				appendStringInfo(&result, "%s%s", alias.data, var_table->cols[index]->name);

				/* work around the lack of booleans in Oracle */
				if (variable->vartype == BOOLOID)
				{
					appendStringInfo(&result, " <> 0)");
				}
			}
			else
			{
				/* treat it like a parameter */
				/* don't try to handle type interval */
				if (! canHandleType(variable->vartype) || variable->vartype == INTERVALOID)
					return NULL;

				/* find the index in the parameter list */
				index = 0;
				foreach(cell, *params)
				{
					++index;
					if (equal(variable, (Node *)lfirst(cell)))
						break;
				}
				if (cell == NULL)
				{
					/* add the parameter to the list */
					++index;
					*params = lappend(*params, variable);
				}

				/* parameters will be called :p1, :p2 etc. */
				initStringInfo(&result);
				appendStringInfo(&result, ":p%d", index);
			}

			break;
		case T_OpExpr:
			oper = (OpExpr *)expr;

			/* get operator name, kind, argument type and schema */
			tuple = SearchSysCache1(OPEROID, ObjectIdGetDatum(oper->opno));
			if (! HeapTupleIsValid(tuple))
			{
				elog(ERROR, "cache lookup failed for operator %u", oper->opno);
			}
			opername = pstrdup(((Form_pg_operator)GETSTRUCT(tuple))->oprname.data);
			oprkind = ((Form_pg_operator)GETSTRUCT(tuple))->oprkind;
			leftargtype = ((Form_pg_operator)GETSTRUCT(tuple))->oprleft;
			rightargtype = ((Form_pg_operator)GETSTRUCT(tuple))->oprright;
			schema = ((Form_pg_operator)GETSTRUCT(tuple))->oprnamespace;
			ReleaseSysCache(tuple);

			/* ignore operators in other than the pg_catalog schema */
			if (schema != PG_CATALOG_NAMESPACE)
				return NULL;

			if (! canHandleType(rightargtype))
				return NULL;

			/* does not handle binary type in expression */
			if ((leftargtype == BYTEAOID) || (rightargtype == BYTEAOID))
				return NULL;

			/*
			 * Don't translate operations on two intervals.
			 * INTERVAL YEAR TO MONTH and INTERVAL DAY TO SECOND don't mix well.
			 */
			if (leftargtype == INTERVALOID && rightargtype == INTERVALOID)
				return NULL;

			/*
			 * Oracle does not support string comparison with CLOB.
			 * Mark string comparison here and check CLOB in T_Var node.
			 */
			if ((strcmp(opername, "=") == 0	|| strcmp(opername, "<>") == 0)	&& rightargtype == TEXTOID)
				context->string_comparison = true;

			/* the operators that we can translate */
			if (strcmp(opername, "=") == 0
				|| strcmp(opername, "<>") == 0
				/* string comparisons are not safe */
				|| (strcmp(opername, ">") == 0 && rightargtype != TEXTOID && rightargtype != BPCHAROID
					&& rightargtype != NAMEOID && rightargtype != CHAROID)
				|| (strcmp(opername, "<") == 0 && rightargtype != TEXTOID && rightargtype != BPCHAROID
					&& rightargtype != NAMEOID && rightargtype != CHAROID)
				|| (strcmp(opername, ">=") == 0 && rightargtype != TEXTOID && rightargtype != BPCHAROID
					&& rightargtype != NAMEOID && rightargtype != CHAROID)
				|| (strcmp(opername, "<=") == 0 && rightargtype != TEXTOID && rightargtype != BPCHAROID
					&& rightargtype != NAMEOID && rightargtype != CHAROID)
				|| strcmp(opername, "+") == 0
				/* subtracting DATEs yields a NUMBER in Oracle */
				|| (strcmp(opername, "-") == 0)
				|| strcmp(opername, "*") == 0
				|| strcmp(opername, "~~") == 0
				|| strcmp(opername, "!~~") == 0
				|| strcmp(opername, "~~*") == 0
				|| strcmp(opername, "!~~*") == 0
				|| strcmp(opername, "^") == 0
				|| strcmp(opername, "%") == 0
				|| strcmp(opername, "&") == 0
				|| strcmp(opername, "|/") == 0
				|| strcmp(opername, "@") == 0)
			{
				left = deparseExpr(linitial(oper->args), context);
				if (left == NULL)
				{
					pfree(opername);
					return NULL;
				}

				if (oprkind == 'b')
				{
					/* binary operator */
					right = deparseExpr(lsecond(oper->args), context);
					if (right == NULL)
					{
						pfree(left);
						pfree(opername);
						return NULL;
					}

					initStringInfo(&result);
					if (strcmp(opername, "~~") == 0)
					{
						appendStringInfo(&result, "(%s LIKE %s ESCAPE '\\')", left, right);
					}
					else if (strcmp(opername, "!~~") == 0)
					{
						appendStringInfo(&result, "(%s NOT LIKE %s ESCAPE '\\')", left, right);
					}
					else if (strcmp(opername, "~~*") == 0)
					{
						appendStringInfo(&result, "(UPPER(%s) LIKE UPPER(%s) ESCAPE '\\')", left, right);
					}
					else if (strcmp(opername, "!~~*") == 0)
					{
						appendStringInfo(&result, "(UPPER(%s) NOT LIKE UPPER(%s) ESCAPE '\\')", left, right);
					}
					else if (strcmp(opername, "^") == 0)
					{
						appendStringInfo(&result, "POWER(%s, %s)", left, right);
					}
					else if (strcmp(opername, "%") == 0)
					{
						appendStringInfo(&result, "MOD(%s, %s)", left, right);
					}
					else if (strcmp(opername, "&") == 0)
					{
						appendStringInfo(&result, "BITAND(%s, %s)", left, right);
					}
					else
					{
						/* the other operators have the same name in Oracle */
						appendStringInfo(&result, "(%s %s %s)", left, opername, right);
					}
					pfree(right);
					pfree(left);
				}
				else
				{
					/* unary operator */
					initStringInfo(&result);
					if (strcmp(opername, "|/") == 0)
					{
						appendStringInfo(&result, "SQRT(%s)", left);
					}
					else if (strcmp(opername, "@") == 0)
					{
						appendStringInfo(&result, "ABS(%s)", left);
					}
					else
					{
						/* unary + or - */
						appendStringInfo(&result, "(%s%s)", opername, left);
					}
					pfree(left);
				}
			}
			else
			{
				/* cannot translate this operator */
				pfree(opername);
				return NULL;
			}

			pfree(opername);
			break;
		case T_ScalarArrayOpExpr:
			arrayoper = (ScalarArrayOpExpr *)expr;

			/* get operator name, left argument type and schema */
			tuple = SearchSysCache1(OPEROID, ObjectIdGetDatum(arrayoper->opno));
			if (! HeapTupleIsValid(tuple))
			{
				elog(ERROR, "cache lookup failed for operator %u", arrayoper->opno);
			}
			opername = pstrdup(((Form_pg_operator)GETSTRUCT(tuple))->oprname.data);
			leftargtype = ((Form_pg_operator)GETSTRUCT(tuple))->oprleft;
			schema = ((Form_pg_operator)GETSTRUCT(tuple))->oprnamespace;
			ReleaseSysCache(tuple);

			/* get the type's output function */
			tuple = SearchSysCache1(TYPEOID, ObjectIdGetDatum(leftargtype));
			if (!HeapTupleIsValid(tuple))
			{
				elog(ERROR, "cache lookup failed for type %u", leftargtype);
			}
			typoutput = ((Form_pg_type)GETSTRUCT(tuple))->typoutput;
			ReleaseSysCache(tuple);

			/* ignore operators in other than the pg_catalog schema */
			if (schema != PG_CATALOG_NAMESPACE)
				return NULL;

			/* don't try to push down anything but IN and NOT IN expressions */
			if ((strcmp(opername, "=") != 0 || ! arrayoper->useOr)
					&& (strcmp(opername, "<>") != 0 || arrayoper->useOr))
				return NULL;

			if (! canHandleType(leftargtype))
				return NULL;

			left = deparseExpr(linitial(arrayoper->args), context);
			if (left == NULL)
				return NULL;

			/* begin to compose result */
			initStringInfo(&result);
			appendStringInfo(&result, "(%s %s (", left, arrayoper->useOr ? "IN" : "NOT IN");

			/* the second (=last) argument can be Const, ArrayExpr or ArrayCoerceExpr */
			rightexpr = (Expr *)llast(arrayoper->args);
			switch (rightexpr->type)
			{
				case T_Const:
					/* the second (=last) argument is a Const of ArrayType */
					constant = (Const *)rightexpr;

					/* using NULL in place of an array or value list is valid in Oracle and PostgreSQL */
					if (constant->constisnull)
						appendStringInfo(&result, "NULL");
					else
					{
						ArrayType *arr = DatumGetArrayTypeP(constant->constvalue);

						/* loop through the array elements */
						iterator = array_create_iterator(arr, 0);
						first_arg = true;
						while (array_iterate(iterator, &datum, &isNull))
						{
							char *c;

							if (isNull)
								c = "NULL";
							else
							{
								c = datumToString(datum, ARR_ELEMTYPE(arr));
								if (c == NULL)
								{
									array_free_iterator(iterator);
									return NULL;
								}
							}

							/* append the argument */
							appendStringInfo(&result, "%s%s", first_arg ? "" : ", ", c);
							first_arg = false;
						}
						array_free_iterator(iterator);

						/* don't push down empty arrays, since the semantics for NOT x = ANY(<empty array>) differ */
						if (first_arg)
							return NULL;
					}

					break;

				case T_ArrayCoerceExpr:
					/* the second (=last) argument is an ArrayCoerceExpr */
					arraycoerce = (ArrayCoerceExpr *)rightexpr;

					/* if the conversion requires more than binary coercion, don't push it down */
#if PG_VERSION_NUM < 110000
					if (arraycoerce->elemfuncid != InvalidOid)
						return NULL;
#else
					if (arraycoerce->elemexpr && arraycoerce->elemexpr->type != T_RelabelType)
						return NULL;
#endif

					/* punt on anything but ArrayExpr (e.g, parameters) */
					if (arraycoerce->arg->type != T_ArrayExpr)
						return NULL;

					/* the actual array is here */
					rightexpr = arraycoerce->arg;

					/* fall through ! */

				case T_ArrayExpr:
					/* the second (=last) argument is an ArrayExpr */
					array = (ArrayExpr *)rightexpr;

					/* loop the array arguments */
					first_arg = true;
					foreach(cell, array->elements)
					{
						/* convert the argument to a string */
						char *element = deparseExpr((Expr *)lfirst(cell), context);

						/* if any element cannot be converted, give up */
						if (element == NULL)
							return NULL;

						/* append the argument */
						appendStringInfo(&result, "%s%s", first_arg ? "" : ", ", element);
						first_arg = false;
					}

					/* don't push down empty arrays, since the semantics for NOT x = ANY(<empty array>) differ */
					if (first_arg)
						return NULL;

					break;

				default:
					return NULL;
			}

			/* two parentheses close the expression */
			appendStringInfo(&result, "))");

			break;
		case T_NullIfExpr:
			/* get argument type */
			tuple = SearchSysCache1(OPEROID, ObjectIdGetDatum(((NullIfExpr *)expr)->opno));
			if (! HeapTupleIsValid(tuple))
			{
				elog(ERROR, "cache lookup failed for operator %u", ((NullIfExpr *)expr)->opno);
			}
			rightargtype = ((Form_pg_operator)GETSTRUCT(tuple))->oprright;
			ReleaseSysCache(tuple);

			if (! canHandleType(rightargtype))
				return NULL;

			left = deparseExpr(linitial(((NullIfExpr *)expr)->args), context);
			if (left == NULL)
			{
				return NULL;
			}
			right = deparseExpr(lsecond(((NullIfExpr *)expr)->args), context);
			if (right == NULL)
			{
				pfree(left);
				return NULL;
			}

			initStringInfo(&result);
			appendStringInfo(&result, "NULLIF(%s, %s)", left, right);

			break;
		case T_BoolExpr:
			boolexpr = (BoolExpr *)expr;

			arg = deparseExpr(linitial(boolexpr->args), context);
			if (arg == NULL)
				return NULL;

			initStringInfo(&result);
			appendStringInfo(&result, "(%s%s",
					boolexpr->boolop == NOT_EXPR ? "NOT " : "",
					arg);

			do_each_cell(cell, boolexpr->args, list_next(boolexpr->args, list_head(boolexpr->args)))
			{
				arg = deparseExpr((Expr *)lfirst(cell), context);
				if (arg == NULL)
				{
					pfree(result.data);
					return NULL;
				}

				appendStringInfo(&result, " %s %s",
						boolexpr->boolop == AND_EXPR ? "AND" : "OR",
						arg);
			}
			appendStringInfo(&result, ")");

			break;
		case T_RelabelType:
			return deparseExpr(((RelabelType *)expr)->arg, context);
			break;
		case T_CoerceToDomain:
			return deparseExpr(((CoerceToDomain *)expr)->arg, context);
			break;
		case T_CaseExpr:
			caseexpr = (CaseExpr *)expr;

			if (! canHandleType(caseexpr->casetype))
				return NULL;

			initStringInfo(&result);
			appendStringInfo(&result, "CASE");

			/* for the form "CASE arg WHEN ...", add first expression */
			if (caseexpr->arg != NULL)
			{
				arg = deparseExpr(caseexpr->arg, context);
				if (arg == NULL)
				{
					pfree(result.data);
					return NULL;
				}
				else
				{
					appendStringInfo(&result, " %s", arg);
				}
			}

			/* append WHEN ... THEN clauses */
			foreach(cell, caseexpr->args)
			{
				CaseWhen *whenclause = (CaseWhen *)lfirst(cell);

				/* WHEN */
				if (caseexpr->arg == NULL)
				{
					/* for CASE WHEN ..., use the whole expression */
					arg = deparseExpr(whenclause->expr, context);
				}
				else
				{
					/* for CASE arg WHEN ..., use only the right branch of the equality */
					arg = deparseExpr(lsecond(((OpExpr *)whenclause->expr)->args), context);
				}

				if (arg == NULL)
				{
					pfree(result.data);
					return NULL;
				}
				else
				{
					appendStringInfo(&result, " WHEN %s", arg);
					pfree(arg);
				}

				/* THEN */
				arg = deparseExpr(whenclause->result, context);
				if (arg == NULL)
				{
					pfree(result.data);
					return NULL;
				}
				else
				{
					appendStringInfo(&result, " THEN %s", arg);
					pfree(arg);
				}
			}

			/* append ELSE clause if appropriate */
			if (caseexpr->defresult != NULL)
			{
				arg = deparseExpr(caseexpr->defresult, context);
				if (arg == NULL)
				{
					pfree(result.data);
					return NULL;
				}
				else
				{
					appendStringInfo(&result, " ELSE %s", arg);
					pfree(arg);
				}
			}

			/* append END */
			appendStringInfo(&result, " END");
			break;
		case T_CoalesceExpr:
			coalesceexpr = (CoalesceExpr *)expr;

			if (! canHandleType(coalesceexpr->coalescetype))
				return NULL;

			initStringInfo(&result);
			appendStringInfo(&result, "COALESCE(");

			first_arg = true;
			foreach(cell, coalesceexpr->args)
			{
				arg = deparseExpr((Expr *)lfirst(cell), context);
				if (arg == NULL)
				{
					pfree(result.data);
					return NULL;
				}

				if (first_arg)
				{
					appendStringInfo(&result, "%s", arg);
					first_arg = false;
				}
				else
				{
					appendStringInfo(&result, ", %s", arg);
				}
				pfree(arg);
			}

			appendStringInfo(&result, ")");

			break;
		case T_NullTest:
			rightexpr = ((NullTest *)expr)->arg;

			/* since booleans are translated as (expr <> 0), we cannot push them down */
			if (exprType((Node *)rightexpr) == BOOLOID)
				return NULL;

			arg = deparseExpr(rightexpr, context);
			if (arg == NULL)
				return NULL;

			initStringInfo(&result);
			appendStringInfo(&result, "(%s IS %sNULL)",
					arg,
					((NullTest *)expr)->nulltesttype == IS_NOT_NULL ? "NOT " : "");
			break;
		case T_FuncExpr:
			func = (FuncExpr *)expr;
			opername = NULL;
			origin_function = NULL;

			if (! canHandleType(func->funcresulttype))
				return NULL;

			/* do nothing for implicit casts */
			if (func->funcformat == COERCE_IMPLICIT_CAST)
				return deparseExpr(linitial(func->args), context);

			/* get function name */
			origin_function = get_func_name(func->funcid);
			opername = oracle_replace_function(origin_function);

			/* the "normal" functions that we can translate */
			if (exist_in_function_list(origin_function, OracleSupportedBuiltinNumericFunction)
				|| exist_in_function_list(origin_function, OracleSupportedBuiltinStringFunction))
			{
				/* Does not push down the function if number of argument not suitable */
				if (strcmp(opername, "substring") == 0 
					&& list_length(func->args) != 3)
				{
					return NULL;
				}

				initStringInfo(&result);

				/* Does not pushdown these functions if argument is binary */
				if (strcmp(opername, "length") == 0
					|| strcmp(opername, "lengthb") == 0)
				{
					context->handle_length_func = true;
				}

				appendStringInfo(&result, "%s(", opername);

				first_arg = true;
				foreach(cell, func->args)
				{
					arg = deparseExpr(lfirst(cell), context);
					if (arg == NULL)
					{
						pfree(result.data);
						context->handle_length_func = false;
						return NULL;
					}

					if (first_arg)
					{
						first_arg = false;
						appendStringInfo(&result, "%s", arg);
					}
					else
					{
						appendStringInfo(&result, ", %s", arg);
					}
					pfree(arg);
				}

				appendStringInfo(&result, ")");
				context->handle_length_func = false;
			}
			else if (strcmp(opername, "date_part") == 0 ||
					 strcmp(opername, "extract") ==0)
			{
				/* special case: EXTRACT */
				left = deparseExpr(linitial(func->args), context);
				if (left == NULL)
				{
					return NULL;
				}

				/* can only handle these fields in Oracle */
				if (strcmp(left, "'year'") == 0
					|| strcmp(left, "'month'") == 0
					|| strcmp(left, "'day'") == 0
					|| strcmp(left, "'hour'") == 0
					|| strcmp(left, "'minute'") == 0
					|| strcmp(left, "'second'") == 0
					|| strcmp(left, "'timezone_hour'") == 0
					|| strcmp(left, "'timezone_minute'") == 0)
				{
					/* remove final quote */
					left[strlen(left) - 1] = '\0';

					right = deparseExpr(lsecond(func->args), context);
					if (right == NULL)
					{
						pfree(left);
						return NULL;
					}

					initStringInfo(&result);
					appendStringInfo(&result, "EXTRACT(%s FROM %s)", left + 1, right);
					context->can_pushdown_function = true;
				}
				else
				{
					pfree(left);
					return NULL;
				}

				pfree(left);
				pfree(right);
			}
			else if (strcmp(opername, "now") == 0 || strcmp(opername, "transaction_timestamp") == 0)
			{
				/* special case: current timestamp */
				initStringInfo(&result);
				appendStringInfo(&result, "(CAST (:now AS TIMESTAMP WITH TIME ZONE))");
			}
			else if (strcmp(opername, "btrim") == 0)
			{
				Expr	   *arg1;
				Expr	   *arg2;

				/*
				 * We need to convert to trim function because Oracle support this function only.
				 */
				initStringInfo(&result);

				if (list_length(func->args) == 1)
				{
					appendStringInfo(&result, "TRIM(");
					arg1 = linitial(func->args);
					arg = deparseExpr(arg1, context);
					appendStringInfo(&result, "%s", arg);
				}
				else
				{
					appendStringInfo(&result, "TRIM( BOTH ");
					/* Get the first argument */
					arg1 = lsecond(func->args);
					arg = deparseExpr(arg1, context);
					appendStringInfo(&result, "%s FROM ", arg);
					/* Get the last argument */
					arg2 = linitial(func->args);
					arg = deparseExpr(arg2, context);
					appendStringInfo(&result, "%s", arg);
				}
				appendStringInfoChar(&result, ')');
			}
			else if (exist_in_function_list(origin_function, OracleUniqueDateTimeFunction)
					 || exist_in_function_list(origin_function, OracleUniqueNumericFunction))
			{
				initStringInfo(&result);

				/* these functions do not need argument and parentheses */
				if (strcmp(opername, "current_date") == 0 ||
					strcmp(opername, "current_timestamp") == 0 ||
					strcmp(opername, "localtimestamp") == 0 ||
					strcmp(opername, "dbtimezone") == 0)
				{
					appendStringInfoString(&result, opername);
				}
				else
				{
					appendStringInfo(&result, "%s(", opername);

					first_arg = true;
					foreach(cell, func->args)
					{
						arg = deparseExpr(lfirst(cell), context);
						if (arg == NULL)
						{
							pfree(result.data);
							return NULL;
						}

						if (first_arg)
						{
							first_arg = false;
							appendStringInfo(&result, "%s", arg);
						}
						else
						{
							appendStringInfo(&result, ", %s", arg);
						}
						pfree(arg);
					}

					appendStringInfo(&result, ")");
				}
			}
			else if (strcmp(opername, "concat") == 0)
			{
				initStringInfo(&result);
				result.data = oracleDeparseConcat(func->args, context);
			}
			else
			{
				/* function that we cannot render for Oracle */
				return NULL;
			}

			context->can_pushdown_function = true;
			break;
		case T_CoerceViaIO:
			/*
			 * We will only handle casts of 'now'.
			 */
			coerce = (CoerceViaIO *)expr;

			/* only casts to these types are handled */
			if (coerce->resulttype != DATEOID
					&& coerce->resulttype != TIMESTAMPOID
					&& coerce->resulttype != TIMESTAMPTZOID)
				return NULL;

			/* the argument must be a Const */
			if (coerce->arg->type != T_Const)
				return NULL;

			/* the argument must be a not-NULL text constant */
			constant = (Const *)coerce->arg;
			if (constant->constisnull || (constant->consttype != CSTRINGOID && constant->consttype != TEXTOID))
				return NULL;

			/* get the type's output function */
			tuple = SearchSysCache1(TYPEOID, ObjectIdGetDatum(constant->consttype));
			if (!HeapTupleIsValid(tuple))
			{
				elog(ERROR, "cache lookup failed for type %u", constant->consttype);
			}
			typoutput = ((Form_pg_type)GETSTRUCT(tuple))->typoutput;
			ReleaseSysCache(tuple);

			/* the value must be "now" */
			if (strcmp(DatumGetCString(OidFunctionCall1(typoutput, constant->constvalue)), "now") != 0)
				return NULL;

			initStringInfo(&result);
			switch (coerce->resulttype)
			{
				case DATEOID:
					appendStringInfo(&result, "TRUNC(CAST (CAST(:now AS TIMESTAMP WITH TIME ZONE) AS DATE))");
					break;
				case TIMESTAMPOID:
					appendStringInfo(&result, "(CAST (CAST (:now AS TIMESTAMP WITH TIME ZONE) AS TIMESTAMP))");
					break;
				case TIMESTAMPTZOID:
					appendStringInfo(&result, "(CAST (:now AS TIMESTAMP WITH TIME ZONE))");
			}

			break;
#if PG_VERSION_NUM >= 100000
		case T_SQLValueFunction:
			sqlvalfunc = (SQLValueFunction *)expr;

			switch (sqlvalfunc->op)
			{
				case SVFOP_CURRENT_DATE:
					initStringInfo(&result);
					appendStringInfo(&result, "TRUNC(CAST (CAST(:now AS TIMESTAMP WITH TIME ZONE) AS DATE))");
					break;
				case SVFOP_CURRENT_TIMESTAMP:
					initStringInfo(&result);
					appendStringInfo(&result, "(CAST (:now AS TIMESTAMP WITH TIME ZONE))");
					break;
				case SVFOP_LOCALTIMESTAMP:
					initStringInfo(&result);
					appendStringInfo(&result, "(CAST (CAST (:now AS TIMESTAMP WITH TIME ZONE) AS TIMESTAMP))");
					break;
				default:
					return NULL;  /* don't push down other functions */
			}

			break;
#endif
		case T_Aggref:
			{
				initStringInfo(&result);
				result.data = oracleDeparseAggref((Aggref *) expr, context);
			}
			break;
		case T_MinMaxExpr:
			{
				minmaxexpr = (MinMaxExpr *)expr;

				initStringInfo(&result);

				if (minmaxexpr->op == IS_GREATEST)
					appendStringInfo(&result, "GREATEST(");
				else if (minmaxexpr->op == IS_LEAST)
					appendStringInfo(&result, "LEAST(");

				first_arg = true;
				foreach(cell, minmaxexpr->args)
				{
					arg = deparseExpr(lfirst(cell), context);
					if (arg == NULL)
					{
						pfree(result.data);
						return NULL;
					}

					if (first_arg)
					{
						first_arg = false;
						appendStringInfo(&result, "%s", arg);
					}
					else
					{
						appendStringInfo(&result, ", %s", arg);
					}
					pfree(arg);
				}

				appendStringInfo(&result, ")");
			}
			break;
		default:
			/* we cannot translate this to Oracle */
			return NULL;
	}

	return result.data;
}

/*
 * datumToString
 * 		Convert a Datum to a string by calling the type output function.
 * 		Returns the result or NULL if it cannot be converted to Oracle SQL.
 */
static char
*datumToString(Datum datum, Oid type)
{
	StringInfoData result;
	regproc typoutput;
	HeapTuple tuple;
	char *str, *p;

	/* get the type's output function */
	tuple = SearchSysCache1(TYPEOID, ObjectIdGetDatum(type));
	if (!HeapTupleIsValid(tuple))
	{
		elog(ERROR, "cache lookup failed for type %u", type);
	}
	typoutput = ((Form_pg_type)GETSTRUCT(tuple))->typoutput;
	ReleaseSysCache(tuple);

	/* render the constant in Oracle SQL */
	switch (type)
	{
		case TEXTOID:
		case CHAROID:
		case BPCHAROID:
		case VARCHAROID:
		case NAMEOID:
		case UUIDOID:
			str = DatumGetCString(OidFunctionCall1(typoutput, datum));

			/*
			 * Don't try to convert empty strings to Oracle.
			 * Oracle treats empty strings as NULL.
			 */
			if (str[0] == '\0')
				return NULL;

			/* strip "-" from "uuid" values */
			if (type == UUIDOID)
				convertUUID(str);

			/* quote string */
			initStringInfo(&result);
			appendStringInfo(&result, "'");
			for (p=str; *p; ++p)
			{
				if (*p == '\'')
					appendStringInfo(&result, "'");
				appendStringInfo(&result, "%c", *p);
			}
			appendStringInfo(&result, "'");
			break;
		case INT8OID:
		case INT2OID:
		case INT4OID:
		case OIDOID:
		case FLOAT4OID:
		case FLOAT8OID:
		case NUMERICOID:
			str = DatumGetCString(OidFunctionCall1(typoutput, datum));
			initStringInfo(&result);
			appendStringInfo(&result, "%s", str);
			break;
		case DATEOID:
			str = deparseDate(datum);
			initStringInfo(&result);
			appendStringInfo(&result, "(CAST ('%s' AS DATE))", str);
			break;
		case TIMESTAMPOID:
			str = deparseTimestamp(datum, false);
			initStringInfo(&result);
			appendStringInfo(&result, "(CAST ('%s' AS TIMESTAMP))", str);
			break;
		case TIMESTAMPTZOID:
			str = deparseTimestamp(datum, true);
			initStringInfo(&result);
			appendStringInfo(&result, "(CAST ('%s' AS TIMESTAMP WITH TIME ZONE))", str);
			break;
		case INTERVALOID:
			str = deparseInterval(datum);
			if (str == NULL)
				return NULL;
			initStringInfo(&result);
			appendStringInfo(&result, "%s", str);
			break;
		default:
			return NULL;
	}

	return result.data;
}

/*
 * getUsedColumns
 * 		Set "used=true" in oraTable for all columns used in the expression.
 */
void
getUsedColumns(Expr *expr, struct oraTable *oraTable, int foreignrelid)
{
	ListCell *cell;
	Var *variable;
	int index;

	if (expr == NULL)
		return;

	switch(expr->type)
	{
		case T_RestrictInfo:
			getUsedColumns(((RestrictInfo *)expr)->clause, oraTable, foreignrelid);
			break;
		case T_TargetEntry:
			getUsedColumns(((TargetEntry *)expr)->expr, oraTable, foreignrelid);
			break;
		case T_Const:
		case T_Param:
		case T_CaseTestExpr:
		case T_CoerceToDomainValue:
		case T_CurrentOfExpr:
#if PG_VERSION_NUM >= 100000
		case T_NextValueExpr:
#endif
		/*
		 * Does not care about items in GROUP BY/ORDER BY clause
		 * because they existed in the target list
		 */
		case T_SortGroupClause:
			break;
		case T_Var:
			variable = (Var *)expr;

			/* ignore columns belonging to a different foreign table */
			if (variable->varno != foreignrelid)
				break;

			/* ignore system columns */
			if (variable->varattno < 0)
				break;

			/* if this is a wholerow reference, we need all columns */
			if (variable->varattno == 0) {
				for (index=0; index<oraTable->ncols; ++index)
					if (oraTable->cols[index]->pgname)
						oraTable->cols[index]->used = 1;
				break;
			}

			/* get oraTable column index corresponding to this column (-1 if none) */
			index = oraTable->ncols - 1;
			while (index >= 0 && oraTable->cols[index]->pgattnum != variable->varattno)
				--index;

			if (index == -1)
			{
				ereport(WARNING,
						(errcode(ERRCODE_WARNING),
						errmsg("column number %d of foreign table \"%s\" does not exist in foreign Oracle table, will be replaced by NULL", variable->varattno, oraTable->pgname)));
			}
			else
			{
				oraTable->cols[index]->used = 1;
			}
			break;
		case T_Aggref:
			foreach(cell, ((Aggref *)expr)->args)
			{
				getUsedColumns((Expr *)lfirst(cell), oraTable, foreignrelid);
			}
			foreach(cell, ((Aggref *)expr)->aggorder)
			{
				getUsedColumns((Expr *)lfirst(cell), oraTable, foreignrelid);
			}
			foreach(cell, ((Aggref *)expr)->aggdistinct)
			{
				getUsedColumns((Expr *)lfirst(cell), oraTable, foreignrelid);
			}
			break;
		case T_WindowFunc:
			foreach(cell, ((WindowFunc *)expr)->args)
			{
				getUsedColumns((Expr *)lfirst(cell), oraTable, foreignrelid);
			}
			break;
#if PG_VERSION_NUM < 120000
		case T_ArrayRef:
			{
				ArrayRef *ref = (ArrayRef *)expr;
#else
		case T_SubscriptingRef:
			{
				SubscriptingRef *ref = (SubscriptingRef *)expr;
#endif

				foreach(cell, ref->refupperindexpr)
				{
					getUsedColumns((Expr *)lfirst(cell), oraTable, foreignrelid);
				}
				foreach(cell, ref->reflowerindexpr)
				{
					getUsedColumns((Expr *)lfirst(cell), oraTable, foreignrelid);
				}
				getUsedColumns(ref->refexpr, oraTable, foreignrelid);
				getUsedColumns(ref->refassgnexpr, oraTable, foreignrelid);
				break;
			}
		case T_FuncExpr:
			foreach(cell, ((FuncExpr *)expr)->args)
			{
				getUsedColumns((Expr *)lfirst(cell), oraTable, foreignrelid);
			}
			break;
		case T_OpExpr:
			foreach(cell, ((OpExpr *)expr)->args)
			{
				getUsedColumns((Expr *)lfirst(cell), oraTable, foreignrelid);
			}
			break;
		case T_DistinctExpr:
			foreach(cell, ((DistinctExpr *)expr)->args)
			{
				getUsedColumns((Expr *)lfirst(cell), oraTable, foreignrelid);
			}
			break;
		case T_NullIfExpr:
			foreach(cell, ((NullIfExpr *)expr)->args)
			{
				getUsedColumns((Expr *)lfirst(cell), oraTable, foreignrelid);
			}
			break;
		case T_ScalarArrayOpExpr:
			foreach(cell, ((ScalarArrayOpExpr *)expr)->args)
			{
				getUsedColumns((Expr *)lfirst(cell), oraTable, foreignrelid);
			}
			break;
		case T_BoolExpr:
			foreach(cell, ((BoolExpr *)expr)->args)
			{
				getUsedColumns((Expr *)lfirst(cell), oraTable, foreignrelid);
			}
			break;
		case T_SubPlan:
			{
				SubPlan *subplan = (SubPlan *)expr;

				getUsedColumns((Expr *)(subplan->testexpr), oraTable, foreignrelid);

				foreach(cell, subplan->args)
				{
					getUsedColumns((Expr *)lfirst(cell), oraTable, foreignrelid);
				}
			}
			break;
		case T_AlternativeSubPlan:
			/* examine only first alternative */
			getUsedColumns((Expr *)linitial(((AlternativeSubPlan *)expr)->subplans), oraTable, foreignrelid);
			break;
		case T_NamedArgExpr:
			getUsedColumns(((NamedArgExpr *)expr)->arg, oraTable, foreignrelid);
			break;
		case T_RelabelType:
			getUsedColumns(((RelabelType *)expr)->arg, oraTable, foreignrelid);
			break;
		case T_CoerceViaIO:
			getUsedColumns(((CoerceViaIO *)expr)->arg, oraTable, foreignrelid);
			break;
		case T_ArrayCoerceExpr:
			getUsedColumns(((ArrayCoerceExpr *)expr)->arg, oraTable, foreignrelid);
			break;
		case T_ConvertRowtypeExpr:
			getUsedColumns(((ConvertRowtypeExpr *)expr)->arg, oraTable, foreignrelid);
			break;
		case T_CollateExpr:
			getUsedColumns(((CollateExpr *)expr)->arg, oraTable, foreignrelid);
			break;
		case T_CaseExpr:
			foreach(cell, ((CaseExpr *)expr)->args)
			{
				getUsedColumns((Expr *)lfirst(cell), oraTable, foreignrelid);
			}
			getUsedColumns(((CaseExpr *)expr)->arg, oraTable, foreignrelid);
			getUsedColumns(((CaseExpr *)expr)->defresult, oraTable, foreignrelid);
			break;
		case T_CaseWhen:
			getUsedColumns(((CaseWhen *)expr)->expr, oraTable, foreignrelid);
			getUsedColumns(((CaseWhen *)expr)->result, oraTable, foreignrelid);
			break;
		case T_ArrayExpr:
			foreach(cell, ((ArrayExpr *)expr)->elements)
			{
				getUsedColumns((Expr *)lfirst(cell), oraTable, foreignrelid);
			}
			break;
		case T_RowExpr:
			foreach(cell, ((RowExpr *)expr)->args)
			{
				getUsedColumns((Expr *)lfirst(cell), oraTable, foreignrelid);
			}
			break;
		case T_RowCompareExpr:
			foreach(cell, ((RowCompareExpr *)expr)->largs)
			{
				getUsedColumns((Expr *)lfirst(cell), oraTable, foreignrelid);
			}
			foreach(cell, ((RowCompareExpr *)expr)->rargs)
			{
				getUsedColumns((Expr *)lfirst(cell), oraTable, foreignrelid);
			}
			break;
		case T_CoalesceExpr:
			foreach(cell, ((CoalesceExpr *)expr)->args)
			{
				getUsedColumns((Expr *)lfirst(cell), oraTable, foreignrelid);
			}
			break;
		case T_MinMaxExpr:
			foreach(cell, ((MinMaxExpr *)expr)->args)
			{
				getUsedColumns((Expr *)lfirst(cell), oraTable, foreignrelid);
			}
			break;
		case T_XmlExpr:
			foreach(cell, ((XmlExpr *)expr)->named_args)
			{
				getUsedColumns((Expr *)lfirst(cell), oraTable, foreignrelid);
			}
			foreach(cell, ((XmlExpr *)expr)->args)
			{
				getUsedColumns((Expr *)lfirst(cell), oraTable, foreignrelid);
			}
			break;
		case T_NullTest:
			getUsedColumns(((NullTest *)expr)->arg, oraTable, foreignrelid);
			break;
		case T_BooleanTest:
			getUsedColumns(((BooleanTest *)expr)->arg, oraTable, foreignrelid);
			break;
		case T_CoerceToDomain:
			getUsedColumns(((CoerceToDomain *)expr)->arg, oraTable, foreignrelid);
			break;
		case T_PlaceHolderVar:
			getUsedColumns(((PlaceHolderVar *)expr)->phexpr, oraTable, foreignrelid);
			break;
#if PG_VERSION_NUM >= 100000
		case T_SQLValueFunction:
			break;  /* contains no column references */
#endif
		default:
			/*
			 * We must be able to handle all node types that can
			 * appear because we cannot omit a column from the remote
			 * query that will be needed.
			 * Throw an error if we encounter an unexpected node type.
			 */
			ereport(ERROR,
					(errcode(ERRCODE_FDW_UNABLE_TO_CREATE_REPLY),
					errmsg("Internal oracle_fdw error: encountered unknown node type %d.", expr->type)));
	}
}

/*
 * checkDataType
 * 		Check that the Oracle data type of a column can be
 * 		converted to the PostgreSQL data type, raise an error if not.
 */
void
checkDataType(oraType oratype, int scale, Oid pgtype, const char *tablename, const char *colname)
{
	/* the binary Oracle types can be converted to bytea */
	if ((oratype == ORA_TYPE_RAW
			|| oratype == ORA_TYPE_BLOB
			|| oratype == ORA_TYPE_BFILE
			|| oratype == ORA_TYPE_LONGRAW)
			&& pgtype == BYTEAOID)
		return;

	/* Oracle RAW can be converted to uuid */
	if (oratype == ORA_TYPE_RAW && pgtype == UUIDOID)
		return;

	/* all other Oracle types can be transformed to strings */
	if (oratype != ORA_TYPE_OTHER
			&& oratype != ORA_TYPE_RAW
			&& oratype != ORA_TYPE_BLOB
			&& oratype != ORA_TYPE_BFILE
			&& oratype != ORA_TYPE_LONGRAW
			&& (pgtype == TEXTOID || pgtype == VARCHAROID || pgtype == BPCHAROID))
		return;

	/* all numeric Oracle types can be transformed to floating point types */
	if ((oratype == ORA_TYPE_NUMBER
			|| oratype == ORA_TYPE_FLOAT
			|| oratype == ORA_TYPE_BINARYFLOAT
			|| oratype == ORA_TYPE_BINARYDOUBLE)
			&& (pgtype == NUMERICOID
			|| pgtype == FLOAT4OID
			|| pgtype == FLOAT8OID))
		return;

	/*
	 * NUMBER columns without decimal fractions can be transformed to
	 * integers or booleans
	 */
	if (oratype == ORA_TYPE_NUMBER && scale <= 0
			&& (pgtype == INT2OID
			|| pgtype == INT4OID
			|| pgtype == INT8OID
			|| pgtype == BOOLOID))
		return;

	/* DATE and timestamps can be transformed to each other */
	if ((oratype == ORA_TYPE_DATE
			|| oratype == ORA_TYPE_TIMESTAMP
			|| oratype == ORA_TYPE_TIMESTAMPTZ)
			&& (pgtype == DATEOID
			|| pgtype == TIMESTAMPOID
			|| pgtype == TIMESTAMPTZOID))
		return;

	/* interval types can be transformed to interval */
	if ((oratype == ORA_TYPE_INTERVALY2M
			|| oratype == ORA_TYPE_INTERVALD2S)
			&& pgtype == INTERVALOID)
		return;
	/* SDO_GEOMETRY can be converted to geometry */
	if (oratype == ORA_TYPE_GEOMETRY
			&& pgtype == GEOMETRYOID)
		return;

	/* VARCHAR2 and CLOB can be converted to json */
	if ((oratype == ORA_TYPE_VARCHAR2
			|| oratype == ORA_TYPE_CLOB)
			&& pgtype == JSONOID)
		return;

	/* XMLTYPE can be converted to xml */
	if (oratype == ORA_TYPE_XMLTYPE && pgtype == XMLOID)
		return;

	/* otherwise, report an error */
	ereport(ERROR,
			(errcode(ERRCODE_FDW_INVALID_DATA_TYPE),
			errmsg("column \"%s\" of foreign table \"%s\" cannot be converted to or from Oracle data type", colname, tablename)));
}

/*
 * deparseWhereConditions
 * 		Classify conditions into remote_conds or local_conds.
 * 		Those conditions that can be pushed down will be collected into
 * 		an Oracle WHERE clause that is returned.
 */
char *
deparseWhereConditions(struct OracleFdwState *fdwState, PlannerInfo *root, RelOptInfo *baserel, List **local_conds, List **remote_conds)
{
	List *conditions = baserel->baserestrictinfo;
	ListCell *cell;
	char *where;
	char *keyword = "WHERE";
	StringInfoData where_clause;
	deparse_expr_cxt context;

	/* init context */
	initializeContext(fdwState, root, baserel, baserel, &context);

	initStringInfo(&where_clause);
	foreach(cell, conditions)
	{
		RestrictInfo *ri = lfirst_node(RestrictInfo, cell);
		/* check if the condition can be pushed down */
		where = deparseExpr
					(ri->clause,
					&context);

		if (where != NULL) {
			*remote_conds = lappend(*remote_conds, ri);

			/* append new WHERE clause to query string */
			appendStringInfo(&where_clause, " %s %s", keyword, where);
			keyword = "AND";
			pfree(where);
		}
		else
			*local_conds = lappend(*local_conds, ri);
	}
	return where_clause.data;
}

/*
 * guessNlsLang
 * 		If nls_lang is not NULL, return "NLS_LANG=<nls_lang>".
 * 		Otherwise, return a good guess for Oracle's NLS_LANG.
 */
char
*guessNlsLang(char *nls_lang)
{
	char *server_encoding, *lc_messages, *language = "AMERICAN_AMERICA", *charset = NULL;
	StringInfoData buf;

	initStringInfo(&buf);
	if (nls_lang == NULL)
	{
		server_encoding = pstrdup(GetConfigOption("server_encoding", false, true));

		/* find an Oracle client character set that matches the database encoding */
		if (strcmp(server_encoding, "UTF8") == 0)
			charset = "AL32UTF8";
		else if (strcmp(server_encoding, "EUC_JP") == 0)
			charset = "JA16EUC";
		else if (strcmp(server_encoding, "EUC_JIS_2004") == 0)
			charset = "JA16SJIS";
		else if (strcmp(server_encoding, "EUC_TW") == 0)
			charset = "ZHT32EUC";
		else if (strcmp(server_encoding, "ISO_8859_5") == 0)
			charset = "CL8ISO8859P5";
		else if (strcmp(server_encoding, "ISO_8859_6") == 0)
			charset = "AR8ISO8859P6";
		else if (strcmp(server_encoding, "ISO_8859_7") == 0)
			charset = "EL8ISO8859P7";
		else if (strcmp(server_encoding, "ISO_8859_8") == 0)
			charset = "IW8ISO8859P8";
		else if (strcmp(server_encoding, "KOI8R") == 0)
			charset = "CL8KOI8R";
		else if (strcmp(server_encoding, "KOI8U") == 0)
			charset = "CL8KOI8U";
		else if (strcmp(server_encoding, "LATIN1") == 0)
			charset = "WE8ISO8859P1";
		else if (strcmp(server_encoding, "LATIN2") == 0)
			charset = "EE8ISO8859P2";
		else if (strcmp(server_encoding, "LATIN3") == 0)
			charset = "SE8ISO8859P3";
		else if (strcmp(server_encoding, "LATIN4") == 0)
			charset = "NEE8ISO8859P4";
		else if (strcmp(server_encoding, "LATIN5") == 0)
			charset = "WE8ISO8859P9";
		else if (strcmp(server_encoding, "LATIN6") == 0)
			charset = "NE8ISO8859P10";
		else if (strcmp(server_encoding, "LATIN7") == 0)
			charset = "BLT8ISO8859P13";
		else if (strcmp(server_encoding, "LATIN8") == 0)
			charset = "CEL8ISO8859P14";
		else if (strcmp(server_encoding, "LATIN9") == 0)
			charset = "WE8ISO8859P15";
		else if (strcmp(server_encoding, "WIN866") == 0)
			charset = "RU8PC866";
		else if (strcmp(server_encoding, "WIN1250") == 0)
			charset = "EE8MSWIN1250";
		else if (strcmp(server_encoding, "WIN1251") == 0)
			charset = "CL8MSWIN1251";
		else if (strcmp(server_encoding, "WIN1252") == 0)
			charset = "WE8MSWIN1252";
		else if (strcmp(server_encoding, "WIN1253") == 0)
			charset = "EL8MSWIN1253";
		else if (strcmp(server_encoding, "WIN1254") == 0)
			charset = "TR8MSWIN1254";
		else if (strcmp(server_encoding, "WIN1255") == 0)
			charset = "IW8MSWIN1255";
		else if (strcmp(server_encoding, "WIN1256") == 0)
			charset = "AR8MSWIN1256";
		else if (strcmp(server_encoding, "WIN1257") == 0)
			charset = "BLT8MSWIN1257";
		else if (strcmp(server_encoding, "WIN1258") == 0)
			charset = "VN8MSWIN1258";
		else
		{
			/* warn if we have to resort to 7-bit ASCII */
			charset = "US7ASCII";

			ereport(WARNING,
					(errcode(ERRCODE_WARNING),
					errmsg("no Oracle character set for database encoding \"%s\"", server_encoding),
					errdetail("All but ASCII characters will be lost."),
					errhint("You can set the option \"%s\" on the foreign data wrapper to force an Oracle character set.", OPT_NLS_LANG)));
		}

		lc_messages = pstrdup(GetConfigOption("lc_messages", false, true));
		/* try to guess those for which there is a backend translation */
		if (strncmp(lc_messages, "de_", 3) == 0 || pg_strncasecmp(lc_messages, "german", 6) == 0)
			language = "GERMAN_GERMANY";
		if (strncmp(lc_messages, "es_", 3) == 0 || pg_strncasecmp(lc_messages, "spanish", 7) == 0)
			language = "SPANISH_SPAIN";
		if (strncmp(lc_messages, "fr_", 3) == 0 || pg_strncasecmp(lc_messages, "french", 6) == 0)
			language = "FRENCH_FRANCE";
		if (strncmp(lc_messages, "in_", 3) == 0 || pg_strncasecmp(lc_messages, "indonesian", 10) == 0)
			language = "INDONESIAN_INDONESIA";
		if (strncmp(lc_messages, "it_", 3) == 0 || pg_strncasecmp(lc_messages, "italian", 7) == 0)
			language = "ITALIAN_ITALY";
		if (strncmp(lc_messages, "ja_", 3) == 0 || pg_strncasecmp(lc_messages, "japanese", 8) == 0)
			language = "JAPANESE_JAPAN";
		if (strncmp(lc_messages, "pt_", 3) == 0 || pg_strncasecmp(lc_messages, "portuguese", 10) == 0)
			language = "BRAZILIAN PORTUGUESE_BRAZIL";
		if (strncmp(lc_messages, "ru_", 3) == 0 || pg_strncasecmp(lc_messages, "russian", 7) == 0)
			language = "RUSSIAN_RUSSIA";
		if (strncmp(lc_messages, "tr_", 3) == 0 || pg_strncasecmp(lc_messages, "turkish", 7) == 0)
			language = "TURKISH_TURKEY";
		if (strncmp(lc_messages, "zh_CN", 5) == 0 || pg_strncasecmp(lc_messages, "chinese-simplified", 18) == 0)
			language = "SIMPLIFIED CHINESE_CHINA";
		if (strncmp(lc_messages, "zh_TW", 5) == 0 || pg_strncasecmp(lc_messages, "chinese-traditional", 19) == 0)
			language = "TRADITIONAL CHINESE_TAIWAN";

		appendStringInfo(&buf, "NLS_LANG=%s.%s", language, charset);
	}
	else
	{
		appendStringInfo(&buf, "NLS_LANG=%s", nls_lang);
	}

	elog(DEBUG1, "oracle_fdw: set %s", buf.data);

	return buf.data;
}

oracleSession *
oracleConnectServer(Name srvname)
{
	Oid srvId = InvalidOid;
	HeapTuple tup;
	Relation rel;
	ForeignServer *server;
	UserMapping *mapping;
	ForeignDataWrapper *wrapper;
	List *options;
	ListCell *cell;
	char *nls_lang = NULL, *user = NULL, *password = NULL, *dbserver = NULL;
	oraIsoLevel isolation_level = DEFAULT_ISOLATION_LEVEL;
	bool have_nchar = false;

	/* look up foreign server with this name */
	rel = table_open(ForeignServerRelationId, AccessShareLock);

	tup = SearchSysCacheCopy1(FOREIGNSERVERNAME, NameGetDatum(srvname));
	if (!HeapTupleIsValid(tup))
		ereport(ERROR,
			(errcode(ERRCODE_UNDEFINED_OBJECT),
			errmsg("server \"%s\" does not exist", NameStr(*srvname))));

#if PG_VERSION_NUM < 120000
	srvId = HeapTupleGetOid(tup);
#else
	srvId = ((Form_pg_foreign_server)GETSTRUCT(tup))->oid;
#endif

	table_close(rel, AccessShareLock);

	/* get the foreign server, the user mapping and the FDW */
	server = GetForeignServer(srvId);
	mapping = GetUserMapping(GetUserId(), srvId);
	wrapper = GetForeignDataWrapper(server->fdwid);

	/* get all options for these objects */
	options = wrapper->options;
	options = list_concat(options, server->options);
	options = list_concat(options, mapping->options);

	foreach(cell, options)
	{
		DefElem *def = (DefElem *) lfirst(cell);
		if (strcmp(def->defname, OPT_NLS_LANG) == 0)
			nls_lang = strVal(def->arg);
		if (strcmp(def->defname, OPT_DBSERVER) == 0)
			dbserver = strVal(def->arg);
		if (strcmp(def->defname, OPT_ISOLATION_LEVEL) == 0)
			isolation_level = getIsolationLevel(strVal(def->arg));
		if (strcmp(def->defname, OPT_USER) == 0)
			user = strVal(def->arg);
		if (strcmp(def->defname, OPT_PASSWORD) == 0)
			password = strVal(def->arg);
		if (strcmp(def->defname, OPT_NCHAR) == 0)
		{
			char *nchar = strVal(def->arg);

			if ((pg_strcasecmp(nchar, "on") == 0
				|| pg_strcasecmp(nchar, "yes") == 0
				|| pg_strcasecmp(nchar, "true") == 0))
			have_nchar = true;
		}
	}

	/* guess a good NLS_LANG environment setting */
	nls_lang = guessNlsLang(nls_lang);

	/* connect to Oracle database */
	return oracleGetSession(
		dbserver,
		isolation_level,
		user,
		password,
		nls_lang,
		(int)have_nchar,
		NULL,
		1
	);
}


#define serializeInt(x) makeConst(INT4OID, -1, InvalidOid, 4, Int32GetDatum((int32)(x)), false, true)
#define serializeOid(x) makeConst(OIDOID, -1, InvalidOid, 4, ObjectIdGetDatum(x), false, true)

/*
 * serializePlanData
 * 		Create a List representation of plan data that copyObject can copy.
 * 		This List can be parsed by deserializePlanData.
 */

List
*serializePlanData(struct OracleFdwState *fdwState)
{
	List *result = NIL;
	int i, len = 0;
	const struct paramDesc *param;

	/* dbserver */
	result = lappend(result, serializeString(fdwState->dbserver));
	/* isolation_level */
	result = lappend(result, serializeInt((int)fdwState->isolation_level));
	/* have_nchar */
	result = lappend(result, serializeInt((int)fdwState->have_nchar));
	/* user name */
	result = lappend(result, serializeString(fdwState->user));
	/* password */
	result = lappend(result, serializeString(fdwState->password));
	/* nls_lang */
	result = lappend(result, serializeString(fdwState->nls_lang));
	/* query */
	result = lappend(result, serializeString(fdwState->query));
	/* Oracle prefetch count */
	result = lappend(result, serializeInt((int)fdwState->prefetch));
	/* Oracle table name */
	result = lappend(result, serializeString(fdwState->oraTable->name));
	/* PostgreSQL table name */
	result = lappend(result, serializeString(fdwState->oraTable->pgname));
	/* number of columns in Oracle table */
	result = lappend(result, serializeInt(fdwState->oraTable->ncols));
	/* number of columns in PostgreSQL table */
	result = lappend(result, serializeInt(fdwState->oraTable->npgcols));
	/* column data */
	for (i=0; i<fdwState->oraTable->ncols; ++i)
	{
		result = lappend(result, serializeString(fdwState->oraTable->cols[i]->name));
		result = lappend(result, serializeInt(fdwState->oraTable->cols[i]->oratype));
		result = lappend(result, serializeInt(fdwState->oraTable->cols[i]->scale));
		result = lappend(result, serializeString(fdwState->oraTable->cols[i]->pgname));
		result = lappend(result, serializeInt(fdwState->oraTable->cols[i]->pgattnum));
		result = lappend(result, serializeOid(fdwState->oraTable->cols[i]->pgtype));
		result = lappend(result, serializeInt(fdwState->oraTable->cols[i]->pgtypmod));
		result = lappend(result, serializeInt(fdwState->oraTable->cols[i]->used));
		result = lappend(result, serializeInt(fdwState->oraTable->cols[i]->strip_zeros));
		result = lappend(result, serializeInt(fdwState->oraTable->cols[i]->pkey));
		result = lappend(result, serializeLong(fdwState->oraTable->cols[i]->val_size));
		result = lappend(result, serializeInt(fdwState->oraTable->cols[i]->node_type));
		/* don't serialize val, val_len, val_len4, val_null and varno */
	}

	/* find length of parameter list */
	for (param=fdwState->paramList; param; param=param->next)
		++len;
	/* serialize length */
	result = lappend(result, serializeInt(len));
	/* parameter list entries */
	for (param=fdwState->paramList; param; param=param->next)
	{
		result = lappend(result, serializeString(param->name));
		result = lappend(result, serializeOid(param->type));
		result = lappend(result, serializeInt((int)param->bindType));
		result = lappend(result, serializeInt((int)param->colnum));
		/* don't serialize value, node and bindh */
	}
	/*
	 * Don't serialize params, startup_cost, total_cost, rowcount, columnindex,
	 * temp_cxt, order_clause, usable_pathkeys and where_clause.
	 */

	result = lappend(result, fdwState->retrieved_attrs);

	/* use max_long for building oraTable of scanning table */
	result = lappend(result, serializeLong((long)fdwState->max_long));

	return result;
}

/*
 * serializeString
 * 		Create a Const that contains the string.
 */

Const
*serializeString(const char *s)
{
	if (s == NULL)
		return makeNullConst(TEXTOID, -1, InvalidOid);
	else
		return makeConst(TEXTOID, -1, InvalidOid, -1, PointerGetDatum(cstring_to_text(s)), false, false);
}

/*
 * serializeLong
 * 		Create a Const that contains the long integer.
 */

Const
*serializeLong(long i)
{
	if (sizeof(long) <= 4)
		return makeConst(INT4OID, -1, InvalidOid, 4, Int32GetDatum((int32)i), false, true);
	else
		return makeConst(INT4OID, -1, InvalidOid, 8, Int64GetDatum((int64)i), false,
#ifdef USE_FLOAT8_BYVAL
					true
#else
					false
#endif  /* USE_FLOAT8_BYVAL */
				);
}

/*
 * deserializePlanData
 * 		Extract the data structures from a List created by serializePlanData.
 * 		Allocates memory for values returned from Oracle.
 */

struct OracleFdwState
*deserializePlanData(List *list)
{
	struct OracleFdwState *state = palloc(sizeof(struct OracleFdwState));
	ListCell *cell = list_head(list);
	int i, len;
	struct paramDesc *param;

	/* session will be set upon connect */
	state->session = NULL;
	/* these fields are not needed during execution */
	state->startup_cost = 0;
	state->total_cost = 0;
	state->order_clause = NULL;
	state->usable_pathkeys = NULL;
	/* these are not serialized */
	state->rowcount = 0;
	state->columnindex = 0;
	state->params = NULL;
	state->temp_cxt = NULL;

	/* dbserver */
	state->dbserver = deserializeString(lfirst(cell));
	cell = list_next(list, cell);

	/* isolation_level */
	state->isolation_level = (oraIsoLevel)DatumGetInt32(((Const *)lfirst(cell))->constvalue);
	cell = list_next(list, cell);

	/* have_nchar */
	state->have_nchar = (bool)DatumGetInt32(((Const *)lfirst(cell))->constvalue);
	cell = list_next(list, cell);

	/* user */
	state->user = deserializeString(lfirst(cell));
	cell = list_next(list, cell);

	/* password */
	state->password = deserializeString(lfirst(cell));
	cell = list_next(list, cell);

	/* nls_lang */
	state->nls_lang = deserializeString(lfirst(cell));
	cell = list_next(list, cell);

	/* query */
	state->query = deserializeString(lfirst(cell));
	cell = list_next(list, cell);

	/* Oracle prefetch count */
	state->prefetch = (unsigned int)DatumGetInt32(((Const *)lfirst(cell))->constvalue);
	cell = list_next(list, cell);

	/* table data */
	state->oraTable = (struct oraTable *)palloc(sizeof(struct oraTable));
	state->oraTable->name = deserializeString(lfirst(cell));
	cell = list_next(list, cell);
	state->oraTable->pgname = deserializeString(lfirst(cell));
	cell = list_next(list, cell);
	state->oraTable->ncols = (int)DatumGetInt32(((Const *)lfirst(cell))->constvalue);
	cell = list_next(list, cell);
	state->oraTable->npgcols = (int)DatumGetInt32(((Const *)lfirst(cell))->constvalue);
	cell = list_next(list, cell);
	state->oraTable->cols = (struct oraColumn **)palloc(sizeof(struct oraColumn *) * state->oraTable->ncols);

	/* loop columns */
	for (i=0; i<state->oraTable->ncols; ++i)
	{
		state->oraTable->cols[i] = (struct oraColumn *)palloc(sizeof(struct oraColumn));
		state->oraTable->cols[i]->name = deserializeString(lfirst(cell));
		cell = list_next(list, cell);
		state->oraTable->cols[i]->oratype = (oraType)DatumGetInt32(((Const *)lfirst(cell))->constvalue);
		cell = list_next(list, cell);
		state->oraTable->cols[i]->scale = (int)DatumGetInt32(((Const *)lfirst(cell))->constvalue);
		cell = list_next(list, cell);
		state->oraTable->cols[i]->pgname = deserializeString(lfirst(cell));
		cell = list_next(list, cell);
		state->oraTable->cols[i]->pgattnum = (int)DatumGetInt32(((Const *)lfirst(cell))->constvalue);
		cell = list_next(list, cell);
		state->oraTable->cols[i]->pgtype = DatumGetObjectId(((Const *)lfirst(cell))->constvalue);
		cell = list_next(list, cell);
		state->oraTable->cols[i]->pgtypmod = (int)DatumGetInt32(((Const *)lfirst(cell))->constvalue);
		cell = list_next(list, cell);
		state->oraTable->cols[i]->used = (int)DatumGetInt32(((Const *)lfirst(cell))->constvalue);
		cell = list_next(list, cell);
		state->oraTable->cols[i]->strip_zeros = (int)DatumGetInt32(((Const *)lfirst(cell))->constvalue);
		cell = list_next(list, cell);
		state->oraTable->cols[i]->pkey = (int)DatumGetInt32(((Const *)lfirst(cell))->constvalue);
		cell = list_next(list, cell);
		state->oraTable->cols[i]->val_size = deserializeLong(lfirst(cell));
		cell = list_next(list, cell);
		state->oraTable->cols[i]->node_type = (int)DatumGetInt32(((Const *)lfirst(cell))->constvalue);
		cell = list_next(list, cell);

		/* allocate memory for the result value */
		state->oraTable->cols[i]->val = (char *)palloc(state->oraTable->cols[i]->val_size + 1);
		state->oraTable->cols[i]->val_len = (unsigned int *)palloc0(sizeof(unsigned int));
		state->oraTable->cols[i]->val_len4 = (unsigned int *)palloc0(sizeof(unsigned int));
		state->oraTable->cols[i]->val_null = (short *)palloc(sizeof(short));
		memset(state->oraTable->cols[i]->val_null, 1, sizeof(short));
	}

	/* length of parameter list */
	len = (int)DatumGetInt32(((Const *)lfirst(cell))->constvalue);
	cell = list_next(list, cell);

	/* parameter table entries */
	state->paramList = NULL;
	for (i=0; i<len; ++i)
	{
		param = (struct paramDesc *)palloc(sizeof(struct paramDesc));
		param->name = deserializeString(lfirst(cell));
		cell = list_next(list, cell);
		param->type = DatumGetObjectId(((Const *)lfirst(cell))->constvalue);
		cell = list_next(list, cell);
		param->bindType = (oraBindType)DatumGetInt32(((Const *)lfirst(cell))->constvalue);
		cell = list_next(list, cell);
		if (param->bindType == BIND_OUTPUT)
			param->value = (void *)42;  /* something != NULL */
		else
			param->value = NULL;
		param->node = NULL;
		param->bindh = NULL;
		param->colnum = (int)DatumGetInt32(((Const *)lfirst(cell))->constvalue);
		cell = list_next(list, cell);
		param->next = state->paramList;
		state->paramList = param;
	}

	state->retrieved_attrs = lfirst(cell);
	cell = list_next(list, cell);

	state->max_long = deserializeLong(lfirst(cell));

	return state;
}

/*
 * deserializeString
 * 		Extracts a string from a Const, returns a palloc'ed copy.
 */

char
*deserializeString(Const *constant)
{
	if (constant->constisnull)
		return NULL;
	else
		return text_to_cstring(DatumGetTextP(constant->constvalue));
}

/*
 * deserializeLong
 * 		Extracts a long integer from a Const.
 */

long
deserializeLong(Const *constant)
{
	if (sizeof(long) <= 4)
		return (long)DatumGetInt32(constant->constvalue);
	else
		return (long)DatumGetInt64(constant->constvalue);
}

/*
 * optionIsTrue
 * 		Returns true if the string is "true", "on" or "yes".
 */
bool
optionIsTrue(const char *value)
{
	if (pg_strcasecmp(value, "on") == 0
			|| pg_strcasecmp(value, "yes") == 0
			|| pg_strcasecmp(value, "true") == 0)
		return true;
	else
		return false;
}

#if PG_VERSION_NUM < 130000
/*
 * find_em_expr_for_rel
 * 		Find an equivalence class member expression, all of whose Vars come from
 * 		the indicated relation.
 * 		This is copied from the PostgreSQL source, because before v13 it was
 * 		not exported.
 */
Expr *
find_em_expr_for_rel(EquivalenceClass *ec, RelOptInfo *rel)
{
	ListCell   *lc_em;

	foreach(lc_em, ec->ec_members)
	{
		EquivalenceMember *em = lfirst(lc_em);

		if (bms_equal(em->em_relids, rel->relids))
		{
			/*
			 * If there is more than one equivalence member whose Vars are
			 * taken entirely from this relation, we'll be content to choose
			 * any one of those.
			 */
			return em->em_expr;
		}
	}

	/* We didn't find any suitable equivalence class expression */
	return NULL;
}
#endif  /* PG_VERSION_NUM */

/*
 * deparseDate
 * 		Render a PostgreSQL date so that Oracle can parse it.
 */
char *
deparseDate(Datum datum)
{
	struct pg_tm datetime_tm;
	StringInfoData s;

	if (DATE_NOT_FINITE(DatumGetDateADT(datum)))
		ereport(ERROR,
				(errcode(ERRCODE_FDW_INVALID_ATTRIBUTE_VALUE),
				errmsg("infinite date value cannot be stored in Oracle")));

	/* get the parts */
	(void)j2date(DatumGetDateADT(datum) + POSTGRES_EPOCH_JDATE,
			&(datetime_tm.tm_year),
			&(datetime_tm.tm_mon),
			&(datetime_tm.tm_mday));

	initStringInfo(&s);
	appendStringInfo(&s, "%04d-%02d-%02d 00:00:00 %s",
			datetime_tm.tm_year > 0 ? datetime_tm.tm_year : -datetime_tm.tm_year + 1,
			datetime_tm.tm_mon, datetime_tm.tm_mday,
			(datetime_tm.tm_year > 0) ? "AD" : "BC");

	return s.data;
}

/*
 * deparseTimestamp
 * 		Render a PostgreSQL timestamp so that Oracle can parse it.
 */
char *
deparseTimestamp(Datum datum, bool hasTimezone)
{
	struct pg_tm datetime_tm;
	int32 tzoffset;
	fsec_t datetime_fsec;
	StringInfoData s;

	/* this is sloppy, but DatumGetTimestampTz and DatumGetTimestamp are the same */
	if (TIMESTAMP_NOT_FINITE(DatumGetTimestampTz(datum)))
		ereport(ERROR,
				(errcode(ERRCODE_FDW_INVALID_ATTRIBUTE_VALUE),
				errmsg("infinite timestamp value cannot be stored in Oracle")));

	/* get the parts */
	tzoffset = 0;
	(void)timestamp2tm(DatumGetTimestampTz(datum),
				hasTimezone ? &tzoffset : NULL,
				&datetime_tm,
				&datetime_fsec,
				NULL,
				NULL);

	initStringInfo(&s);
	if (hasTimezone)
		appendStringInfo(&s, "%04d-%02d-%02d %02d:%02d:%02d.%06d%+03d:%02d %s",
			datetime_tm.tm_year > 0 ? datetime_tm.tm_year : -datetime_tm.tm_year + 1,
			datetime_tm.tm_mon, datetime_tm.tm_mday, datetime_tm.tm_hour,
			datetime_tm.tm_min, datetime_tm.tm_sec, (int32)datetime_fsec,
			-tzoffset / 3600, ((tzoffset > 0) ? tzoffset % 3600 : -tzoffset % 3600) / 60,
			(datetime_tm.tm_year > 0) ? "AD" : "BC");
	else
		appendStringInfo(&s, "%04d-%02d-%02d %02d:%02d:%02d.%06d %s",
			datetime_tm.tm_year > 0 ? datetime_tm.tm_year : -datetime_tm.tm_year + 1,
			datetime_tm.tm_mon, datetime_tm.tm_mday, datetime_tm.tm_hour,
			datetime_tm.tm_min, datetime_tm.tm_sec, (int32)datetime_fsec,
			(datetime_tm.tm_year > 0) ? "AD" : "BC");

	return s.data;
}

/*
 * deparseInterval
 * 		Render a PostgreSQL timestamp so that Oracle can parse it.
 */
char
*deparseInterval(Datum datum)
{
	struct pg_tm tm;
	fsec_t fsec;
	StringInfoData s;
	char *sign;

	if (interval2tm(*DatumGetIntervalP(datum), &tm, &fsec) != 0)
	{
		elog(ERROR, "could not convert interval to tm");
	}

	/* only translate intervals that can be translated to INTERVAL DAY TO SECOND */
	if (tm.tm_year != 0 || tm.tm_mon != 0)
		return NULL;

	/* Oracle intervals have only one sign */
	if (tm.tm_mday < 0 || tm.tm_hour < 0 || tm.tm_min < 0 || tm.tm_sec < 0 || fsec < 0)
	{
		sign = "-";
		/* all signs must match */
		if (tm.tm_mday > 0 || tm.tm_hour > 0 || tm.tm_min > 0 || tm.tm_sec > 0 || fsec > 0)
			return NULL;
		tm.tm_mday = -tm.tm_mday;
		tm.tm_hour = -tm.tm_hour;
		tm.tm_min = -tm.tm_min;
		tm.tm_sec = -tm.tm_sec;
		fsec = -fsec;
	}
	else
		sign = "";

	initStringInfo(&s);
	appendStringInfo(&s, "INTERVAL '%s%d %02d:%02d:%02d.%06d' DAY(9) TO SECOND(6)", sign, tm.tm_mday, tm.tm_hour, tm.tm_min, tm.tm_sec, fsec);

	return s.data;
}

/*
 * convertUUID
 * 		Strip "-" from a PostgreSQL "uuid" so that Oracle can parse it.
 * 		In addition, convert the string to upper case.
 * 		This modifies the argument in place!
 */
char
*convertUUID(char *uuid)
{
	char *p = uuid, *q = uuid, c;

	while (*p != '\0')
	{
		if (*p == '-')
			++p;
		c = *(p++);
		if (c >= 'a' && c <= 'f')
			*(q++) = c - ('a' - 'A');
		else
			*(q++) = c;
	}
	*q = '\0';

	return uuid;
}

/*
 * subtransactionCallback
 * 		Set or rollback to Oracle savepoints when appropriate.
 */
void
subtransactionCallback(SubXactEvent event, SubTransactionId mySubid, SubTransactionId parentSubid, void *arg)
{
	/* rollback to the appropriate savepoint on subtransaction abort */
	if (event == SUBXACT_EVENT_ABORT_SUB || event == SUBXACT_EVENT_PRE_COMMIT_SUB)
		oracleEndSubtransaction(arg, GetCurrentTransactionNestLevel(), event == SUBXACT_EVENT_PRE_COMMIT_SUB);
}

/*
 * addParam
 * 		Creates a new struct paramDesc with the given values and adds it to the list.
 * 		A palloc'ed copy of "name" is used.
 */
void
addParam(struct paramDesc **paramList, char *name, Oid pgtype, oraType oratype, int colnum)
{
	struct paramDesc *param;

	param = palloc(sizeof(struct paramDesc));
	param->name = pstrdup(name);
	param->type = pgtype;
	switch (oratype)
	{
		case ORA_TYPE_NUMBER:
		case ORA_TYPE_FLOAT:
		case ORA_TYPE_BINARYFLOAT:
		case ORA_TYPE_BINARYDOUBLE:
			param->bindType = BIND_NUMBER;
			break;
		case ORA_TYPE_LONG:
		case ORA_TYPE_CLOB:
			param->bindType = BIND_LONG;
			break;
		case ORA_TYPE_RAW:
			if (param->type == UUIDOID)
				param->bindType = BIND_STRING;
			else
				param->bindType = BIND_LONGRAW;
			break;
		case ORA_TYPE_LONGRAW:
		case ORA_TYPE_BLOB:
			param->bindType = BIND_LONGRAW;
			break;
		case ORA_TYPE_BFILE:
			ereport(ERROR,
					(errcode(ERRCODE_FDW_INVALID_DATA_TYPE),
					errmsg("cannot update or insert BFILE column in Oracle foreign table")));
			break;
		case ORA_TYPE_GEOMETRY:
			param->bindType = BIND_GEOMETRY;
			break;
		default:
			param->bindType = BIND_STRING;
	}
	param->value = NULL;
	param->node = NULL;
	param->bindh = NULL;
	param->colnum = colnum;
	param->next = *paramList;
	*paramList = param;
}

/*
 * setModifyParameters
 * 		Set the parameter values from the values in the slots.
 * 		"newslot" contains the new values, "oldslot" the old ones.
 */
void
setModifyParameters(struct paramDesc *paramList, TupleTableSlot *newslot, TupleTableSlot *oldslot, struct oraTable *oraTable, oracleSession *session)
{
	struct paramDesc *param;
	Datum datum;
	bool isnull;
	int32 value_len;
	struct pg_tm datetime_tm;
	fsec_t datetime_fsec;
	StringInfoData s;
	Oid pgtype;

	for (param=paramList; param != NULL; param=param->next)
	{
		/* don't do anything for output parameters */
		if (param->bindType == BIND_OUTPUT)
			continue;

		if (param->name[1] == 'k')
		{
			/* for primary key parameters extract the resjunk entry */
			datum = ExecGetJunkAttribute(oldslot, oraTable->cols[param->colnum]->pkey, &isnull);
		}
		else
		{
			/* for other parameters extract the datum from newslot */
			datum = slot_getattr(newslot, oraTable->cols[param->colnum]->pgattnum, &isnull);
		}

		switch (param->bindType)
		{
			case BIND_STRING:
			case BIND_NUMBER:
				if (isnull)
				{
					param->value = NULL;
					break;
				}

				pgtype = oraTable->cols[param->colnum]->pgtype;

				/* special treatment for date, timestamps and intervals */
				if (pgtype == DATEOID)
				{
					param->value = deparseDate(datum);
					break;  /* from switch (param->bindType) */
				}
				else if (pgtype == TIMESTAMPOID || pgtype == TIMESTAMPTZOID)
				{
					param->value = deparseTimestamp(datum, (pgtype == TIMESTAMPTZOID));
					break;  /* from switch (param->bindType) */
				}
				else if (pgtype == INTERVALOID)
				{
					char sign = '+';

					/* get the parts */
					(void)interval2tm(*DatumGetIntervalP(datum), &datetime_tm, &datetime_fsec);

					switch (oraTable->cols[param->colnum]->oratype)
					{
						case ORA_TYPE_INTERVALY2M:
							if (datetime_tm.tm_mday != 0 || datetime_tm.tm_hour != 0
									|| datetime_tm.tm_min != 0 || datetime_tm.tm_sec != 0 || datetime_fsec != 0)
								ereport(ERROR,
										(errcode(ERRCODE_FDW_INVALID_ATTRIBUTE_VALUE),
										errmsg("invalid value for Oracle INTERVAL YEAR TO MONTH"),
										errdetail("Only year and month can be non-zero for such an interval.")));
							if (datetime_tm.tm_year < 0 || datetime_tm.tm_mon < 0)
							{
								if (datetime_tm.tm_year > 0 || datetime_tm.tm_mon > 0)
									ereport(ERROR,
											(errcode(ERRCODE_FDW_INVALID_ATTRIBUTE_VALUE),
											errmsg("invalid value for Oracle INTERVAL YEAR TO MONTH"),
											errdetail("Year and month must be either both positive or both negative.")));
								sign = '-';
								datetime_tm.tm_year = -datetime_tm.tm_year;
								datetime_tm.tm_mon = -datetime_tm.tm_mon;
							}

							initStringInfo(&s);
							appendStringInfo(&s, "%c%d-%d", sign, datetime_tm.tm_year, datetime_tm.tm_mon);
							param->value = s.data;
							break;
						case ORA_TYPE_INTERVALD2S:
							if (datetime_tm.tm_year != 0 || datetime_tm.tm_mon != 0)
								ereport(ERROR,
										(errcode(ERRCODE_FDW_INVALID_ATTRIBUTE_VALUE),
										errmsg("invalid value for Oracle INTERVAL DAY TO SECOND"),
										errdetail("Year and month must be zero for such an interval.")));
							if (datetime_tm.tm_mday < 0 || datetime_tm.tm_hour < 0 || datetime_tm.tm_min < 0
								|| datetime_tm.tm_sec < 0 || datetime_fsec < 0)
							{
								if (datetime_tm.tm_mday > 0 || datetime_tm.tm_hour > 0 || datetime_tm.tm_min > 0
									|| datetime_tm.tm_sec > 0 || datetime_fsec > 0)
									ereport(ERROR,
											(errcode(ERRCODE_FDW_INVALID_ATTRIBUTE_VALUE),
											errmsg("invalid value for Oracle INTERVAL DAY TO SECOND"),
											errdetail("Interval elements must be either all positive or all negative.")));
								sign = '-';
								datetime_tm.tm_mday = -datetime_tm.tm_mday;
								datetime_tm.tm_hour = -datetime_tm.tm_hour;
								datetime_tm.tm_min = -datetime_tm.tm_min;
								datetime_tm.tm_sec = -datetime_tm.tm_sec;
								datetime_fsec = -datetime_fsec;
							}

							initStringInfo(&s);
							appendStringInfo(&s, "%c%d %02d:%02d:%02d.%06d",
									sign, datetime_tm.tm_mday, datetime_tm.tm_hour, datetime_tm.tm_min,
									datetime_tm.tm_sec, (int32)datetime_fsec);
							param->value = s.data;
							break;
						default:
							elog(ERROR, "impossible Oracle type for interval");
					}
					break;  /* from switch (param->bindType) */
				}

				/* convert the parameter value into a string */
				param->value = DatumGetCString(OidFunctionCall1(output_funcs[param->colnum], datum));

				/* some data types need additional processing */
				switch (oraTable->cols[param->colnum]->pgtype)
				{
					case UUIDOID:
						/* remove the minus signs for UUIDs */
						convertUUID(param->value);
						break;
					case BOOLOID:
						/* convert booleans to numbers */
						if (param->value[0] == 't')
							param->value[0] = '1';
						else
							param->value[0] = '0';
						param->value[1] = '\0';
						break;
					default:
						/* nothing to be done */
						break;
				}
				break;
			case BIND_LONG:
			case BIND_LONGRAW:
				if (isnull)
				{
					param->value = NULL;
					break;
				}

				/* detoast it if necessary */
				datum = (Datum)PG_DETOAST_DATUM(datum);

				value_len = VARSIZE(datum) - VARHDRSZ;

				/* the first 4 bytes contain the length */
				param->value = palloc(value_len + 4);
				memcpy(param->value, (const char *)&value_len, 4);
				memcpy(param->value + 4, VARDATA(datum), value_len);
				break;
			case BIND_GEOMETRY:
				if (isnull)
				{
					param->value = (char *)oracleEWKBToGeom(session, 0, NULL);
				}
				else
				{
					/* detoast it if necessary */
					datum = (Datum)PG_DETOAST_DATUM(datum);

					/* will allocate objects in the Oracle object cache */
					param->value = (char *)oracleEWKBToGeom(session, VARSIZE(datum) - VARHDRSZ, VARDATA(datum));
				}
				value_len = 0;  /* not used */
				break;
			case BIND_OUTPUT:
				/* unreachable */
				break;
		}
	}
}

bool
hasTrigger(Relation rel, CmdType cmdtype)
{
	return rel->trigdesc
			&& ((cmdtype == CMD_UPDATE && rel->trigdesc->trig_update_after_row)
				|| (cmdtype == CMD_INSERT && rel->trigdesc->trig_insert_after_row)
				|| (cmdtype == CMD_DELETE && rel->trigdesc->trig_delete_after_row)
				|| (cmdtype == CMD_UPDATE && rel->trigdesc->trig_update_before_row));
}

void
buildInsertQuery(StringInfo sql, struct OracleFdwState *fdwState)
{
	bool firstcol;
	int i;
	char paramName[10];

	appendStringInfo(sql, "INSERT INTO %s (", fdwState->oraTable->name);

	firstcol = true;
	for (i = 0; i < fdwState->oraTable->ncols; ++i)
	{
		/* don't add columns beyond the end of the PostgreSQL table */
		if (fdwState->oraTable->cols[i]->pgname == NULL)
			continue;

		if (firstcol)
			firstcol = false;
		else
			appendStringInfo(sql, ", ");
		appendStringInfo(sql, "%s", fdwState->oraTable->cols[i]->name);
	}

	appendStringInfo(sql, ") VALUES (");

	firstcol = true;
	for (i = 0; i < fdwState->oraTable->ncols; ++i)
	{
		/* don't add columns beyond the end of the PostgreSQL table */
		if (fdwState->oraTable->cols[i]->pgname == NULL)
			continue;

		/* check that the data types can be converted */
		checkDataType(
			fdwState->oraTable->cols[i]->oratype,
			fdwState->oraTable->cols[i]->scale,
			fdwState->oraTable->cols[i]->pgtype,
			fdwState->oraTable->pgname,
			fdwState->oraTable->cols[i]->pgname
		);

		/* add a parameter description for the column */
		snprintf(paramName, 9, ":p%d", fdwState->oraTable->cols[i]->pgattnum);
		addParam(&fdwState->paramList, paramName, fdwState->oraTable->cols[i]->pgtype,
			fdwState->oraTable->cols[i]->oratype, i);

		/* add parameter name */
		if (firstcol)
			firstcol = false;
		else
			appendStringInfo(sql, ", ");

		appendAsType(sql, paramName, fdwState->oraTable->cols[i]->pgtype);
	}

	appendStringInfo(sql, ")");
}

void
buildUpdateQuery(StringInfo sql, struct OracleFdwState *fdwState, List *targetAttrs)
{
	bool firstcol;
	int i;
	char paramName[10];
	ListCell *cell;

	appendStringInfo(sql, "UPDATE %s SET ", fdwState->oraTable->name);

	firstcol = true;
	i = 0;
	foreach(cell, targetAttrs)
	{
		/* find the corresponding oraTable entry */
		while (i < fdwState->oraTable->ncols && fdwState->oraTable->cols[i]->pgattnum < lfirst_int(cell))
			++i;
		if (i == fdwState->oraTable->ncols)
			break;

		/* ignore columns that don't occur in the foreign table */
		if (fdwState->oraTable->cols[i]->pgtype == 0)
			continue;

		/* check that the data types can be converted */
		checkDataType(
			fdwState->oraTable->cols[i]->oratype,
			fdwState->oraTable->cols[i]->scale,
			fdwState->oraTable->cols[i]->pgtype,
			fdwState->oraTable->pgname,
			fdwState->oraTable->cols[i]->pgname
		);

		/* add a parameter description for the column */
		snprintf(paramName, 9, ":p%d", lfirst_int(cell));
		addParam(&fdwState->paramList, paramName, fdwState->oraTable->cols[i]->pgtype,
			fdwState->oraTable->cols[i]->oratype, i);

		/* add the parameter name to the query */
		if (firstcol)
			firstcol = false;
		else
			appendStringInfo(sql, ", ");

		appendStringInfo(sql, "%s = ", fdwState->oraTable->cols[i]->name);
		appendAsType(sql, paramName, fdwState->oraTable->cols[i]->pgtype);
	}

	/* throw a meaningful error if nothing is updated */
	if (firstcol)
		ereport(ERROR,
				(errcode(ERRCODE_FDW_UNABLE_TO_CREATE_EXECUTION),
				errmsg("no Oracle column modified by UPDATE"),
				errdetail("The UPDATE statement only changes colums that do not exist in the Oracle table.")));
}

void
appendReturningClause(StringInfo sql, struct OracleFdwState *fdwState)
{
	int i;
	bool firstcol;
	struct paramDesc *param;
	char paramName[10];

	/* add the RETURNING clause itself */
	firstcol = true;
	for (i=0; i<fdwState->oraTable->ncols; ++i)
		if (fdwState->oraTable->cols[i]->used)
		{
			if (firstcol)
			{
				firstcol = false;
				appendStringInfo(sql, " RETURNING ");
			}
			else
				appendStringInfo(sql, ", ");
			if (fdwState->oraTable->cols[i]->oratype == ORA_TYPE_XMLTYPE)
				appendStringInfo(sql, "(%s).getclobval()", fdwState->oraTable->cols[i]->name);
			else
				appendStringInfo(sql, "%s", fdwState->oraTable->cols[i]->name);
		}

	/* add the parameters for the RETURNING clause */
	firstcol = true;
	for (i=0; i<fdwState->oraTable->ncols; ++i)
		if (fdwState->oraTable->cols[i]->used)
		{
			/* check that the data types can be converted */
			checkDataType(
				fdwState->oraTable->cols[i]->oratype,
				fdwState->oraTable->cols[i]->scale,
				fdwState->oraTable->cols[i]->pgtype,
				fdwState->oraTable->pgname,
				fdwState->oraTable->cols[i]->pgname
			);

			/* create a new entry in the parameter list */
			param = (struct paramDesc *)palloc(sizeof(struct paramDesc));
			snprintf(paramName, 9, ":r%d", fdwState->oraTable->cols[i]->pgattnum);
			param->name = pstrdup(paramName);
			param->type = fdwState->oraTable->cols[i]->pgtype;
			param->bindType = BIND_OUTPUT;
			param->value = (void *)42;  /* something != NULL */
			param->node = NULL;
			param->bindh = NULL;
			param->colnum = i;
			param->next = fdwState->paramList;
			fdwState->paramList = param;

			if (firstcol)
			{
				firstcol = false;
				appendStringInfo(sql, " INTO ");
			}
			else
				appendStringInfo(sql, ", ");
			appendStringInfo(sql, "%s", paramName);
		}
}

/*
 * transactionCallback
 * 		Commit or rollback Oracle transactions when appropriate.
 */
void
transactionCallback(XactEvent event, void *arg)
{
	switch(event)
	{
		case XACT_EVENT_PRE_COMMIT:
#if PG_VERSION_NUM >= 90500
		case XACT_EVENT_PARALLEL_PRE_COMMIT:
#endif  /* PG_VERSION_NUM */
			/* remote commit */
			oracleEndTransaction(arg, 1, 0);
			break;
		case XACT_EVENT_PRE_PREPARE:
			ereport(ERROR,
					(errcode(ERRCODE_FDW_UNABLE_TO_CREATE_EXECUTION),
					errmsg("cannot prepare a transaction that used remote tables")));
			break;
		case XACT_EVENT_COMMIT:
		case XACT_EVENT_PREPARE:
#if PG_VERSION_NUM >= 90500
		case XACT_EVENT_PARALLEL_COMMIT:
#endif  /* PG_VERSION_NUM */
			/*
			 * Commit the remote transaction ignoring errors.
			 * In 9.3 or higher, the transaction must already be closed, so this does nothing.
			 * In 9.2 or lower, this is ok since nothing can have been modified remotely.
			 */
			oracleEndTransaction(arg, 1, 1);
			break;
		case XACT_EVENT_ABORT:
#if PG_VERSION_NUM >= 90500
		case XACT_EVENT_PARALLEL_ABORT:
#endif  /* PG_VERSION_NUM */
			/* remote rollback */
			oracleEndTransaction(arg, 0, 1);
			break;
	}

	dml_in_transaction = false;
}

/*
 * exitHook
 * 		Close all Oracle connections on process exit.
 */

void
exitHook(int code, Datum arg)
{
	oracleShutdown();
}

/*
 * oracleDie
 * 		Terminate the current query and prepare backend shutdown.
 * 		This is a signal handler function.
 */
void
oracleDie(SIGNAL_ARGS)
{
	/*
	 * Terminate any running queries.
	 * The Oracle sessions will be terminated by exitHook().
	 */
	oracleCancel();

	/*
	 * Call the original backend shutdown function.
	 * If a query was canceled above, an error from Oracle would result.
	 * To have the backend report the correct FATAL error instead,
	 * we have to call CHECK_FOR_INTERRUPTS() before we report that error;
	 * this is done in oracleError_d.
	 */
	die(postgres_signal_arg);
}

/*
 * setSelectParameters
 * 		Set the current values of the parameters into paramList.
 * 		Return a string containing the parameters set for a DEBUG message.
 */
char *
setSelectParameters(struct paramDesc *paramList, ExprContext *econtext)
{
	struct paramDesc *param;
	Datum datum;
	HeapTuple tuple;
	TimestampTz tstamp;
	bool is_null;
	bool first_param = true;
	MemoryContext oldcontext;
	StringInfoData info;  /* list of parameters for DEBUG message */
	initStringInfo(&info);

	/* switch to short lived memory context */
	oldcontext = MemoryContextSwitchTo(econtext->ecxt_per_tuple_memory);

	/* iterate parameter list and fill values */
	for (param=paramList; param; param=param->next)
	{
		if (strcmp(param->name, ":now") == 0)
		{
			/* get transaction start timestamp */
			tstamp = GetCurrentTransactionStartTimestamp();

			datum = TimestampGetDatum(tstamp);
			is_null = false;
		}
		else
		{
			/*
			 * Evaluate the expression.
			 * This code path cannot be reached in 9.1
			 */
#if PG_VERSION_NUM < 100000
			datum = ExecEvalExpr((ExprState *)(param->node), econtext, &is_null, NULL);
#else
			datum = ExecEvalExpr((ExprState *)(param->node), econtext, &is_null);
#endif  /* PG_VERSION_NUM */
		}

		if (is_null)
		{
			param->value = NULL;
		}
		else
		{
			if (param->type == DATEOID)
				param->value = deparseDate(datum);
			else if (param->type == TIMESTAMPOID || param->type == TIMESTAMPTZOID)
				param->value = deparseTimestamp(datum, (param->type == TIMESTAMPTZOID));
			else
			{
				regproc typoutput;

				/* get the type's output function */
				tuple = SearchSysCache1(TYPEOID, ObjectIdGetDatum(param->type));
				if (!HeapTupleIsValid(tuple))
				{
					elog(ERROR, "cache lookup failed for type %u", param->type);
				}
				typoutput = ((Form_pg_type)GETSTRUCT(tuple))->typoutput;
				ReleaseSysCache(tuple);

				/* convert the parameter value into a string */
				param->value = DatumGetCString(OidFunctionCall1(typoutput, datum));
			}
		}

		/* build a parameter list for the DEBUG message */
		if (first_param)
		{
			first_param = false;
			appendStringInfo(&info, ", parameters %s=\"%s\"", param->name,
				(param->value ? param->value : "(null)"));
		}
		else
		{
			appendStringInfo(&info, ", %s=\"%s\"", param->name,
				(param->value ? param->value : "(null)"));
		}
	}

	/* reset memory context */
	MemoryContextSwitchTo(oldcontext);

	return info.data;
}

/*
 * convertTuple
 * 		Convert a result row from Oracle stored in oraTable
 * 		into arrays of values and null indicators.
 * 		If trunc_lob it true, truncate LOBs to WIDTH_THRESHOLD+1 bytes.
 */
void
convertTuple(struct OracleFdwState *fdw_state, Datum *values, bool *nulls, bool trunc_lob)
{
	char *value = NULL;
	long value_len = 0;
	int j, index = -1;
	ErrorContextCallback errcb;
	Oid pgtype;

	/* initialize error context callback, install it only during conversions */
	errcb.callback = errorContextCallback;
	errcb.arg = (void *)fdw_state;

	/* assign result values */
	for (j=0; j<fdw_state->oraTable->npgcols; ++j)
	{
		/* for dropped columns, insert a NULL */
		if ((index + 1 < fdw_state->oraTable->ncols)
				&& (fdw_state->oraTable->cols[index + 1]->pgattnum > j + 1))
		{
			nulls[j] = true;
			values[j] = PointerGetDatum(NULL);
			continue;
		}
		else
			++index;

		/*
		 * Columns exceeding the length of the Oracle table will be NULL,
		 * as well as columns that are not used in the query.
		 * Geometry columns are NULL if the value is NULL,
		 * for all other types use the NULL indicator.
		 */
		if (index >= fdw_state->oraTable->ncols
			|| fdw_state->oraTable->cols[index]->used == 0
			|| (fdw_state->oraTable->cols[index]->oratype == ORA_TYPE_GEOMETRY
				&& ((ora_geometry *)fdw_state->oraTable->cols[index]->val)->geometry == NULL)
			|| *(fdw_state->oraTable->cols[index]->val_null + fdw_state->next_tuple) == -1)
		{
			nulls[j] = true;
			values[j] = PointerGetDatum(NULL);
			continue;
		}

		/* from here on, we can assume columns to be NOT NULL */
		nulls[j] = false;
		pgtype = fdw_state->oraTable->cols[index]->pgtype;

		/* get the data and its length */
		if (fdw_state->oraTable->cols[index]->oratype == ORA_TYPE_BLOB
				|| fdw_state->oraTable->cols[index]->oratype == ORA_TYPE_BFILE
				|| fdw_state->oraTable->cols[index]->oratype == ORA_TYPE_CLOB)
		{
			/* for LOBs, get the actual LOB contents (palloc'ed), truncated if desired */
			oracleGetLob(fdw_state->session,
				(void *)fdw_state->oraTable->cols[index]->val, fdw_state->oraTable->cols[index]->oratype,
				&value, &value_len, trunc_lob ? (WIDTH_THRESHOLD+1) : 0);
		}
		else if (fdw_state->oraTable->cols[index]->oratype == ORA_TYPE_GEOMETRY)
		{
			ora_geometry *geom = (ora_geometry *)fdw_state->oraTable->cols[index]->val;

			/* install error context callback */
			errcb.previous = error_context_stack;
			error_context_stack = &errcb;
			fdw_state->columnindex = index;

			value_len = oracleGetEWKBLen(fdw_state->session, geom);

			/* uninstall error context callback */
			error_context_stack = errcb.previous;

			value = NULL;  /* we will fetch that later to avoid unnecessary copying */
		}
		else if (fdw_state->oraTable->cols[index]->oratype == ORA_TYPE_LONG
				|| fdw_state->oraTable->cols[index]->oratype == ORA_TYPE_LONGRAW)
		{
			/* for LONG and LONG RAW, the first 4 bytes contain the length */
			value_len = *((int32 *)fdw_state->oraTable->cols[index]->val);
			/* the rest is the actual data */
			value = fdw_state->oraTable->cols[index]->val + 4;
			/* terminating zero byte (needed for LONGs) */
			value[value_len] = '\0';
		}
		else
		{
			int col_index = fdw_state->next_tuple * fdw_state->oraTable->cols[index]->val_size;
			char *oraval = fdw_state->oraTable->cols[index]->val + col_index;

			/* special handling for NUMBER's "infinity tilde" */
			if ((fdw_state->oraTable->cols[index]->oratype == ORA_TYPE_FLOAT
					|| fdw_state->oraTable->cols[index]->oratype == ORA_TYPE_NUMBER)
				&& (oraval[0] == '~' || (oraval[0] == '-' && oraval[1] == '~')))
			{
				/* "numeric" does not know infinity, so map to NaN */
				if (pgtype == NUMERICOID)
					strcpy(oraval, "Nan");
				else
					strcpy(oraval, (oraval[0] == '-' ? "-inf" : "inf"));
			}

			/* for other data types, oraTable contains the results */
			value = oraval;
			value_len = *(fdw_state->oraTable->cols[index]->val_len + fdw_state->next_tuple);
		}

		/* fill the TupleSlot with the data (after conversion if necessary) */
		if (fdw_state->oraTable->cols[index]->oratype == ORA_TYPE_GEOMETRY)
		{
			ora_geometry *geom = (ora_geometry *)fdw_state->oraTable->cols[index]->val;
			struct varlena *result = NULL;

			/* install error context callback */
			errcb.previous = error_context_stack;
			error_context_stack = &errcb;
			fdw_state->columnindex = index;

			result = (bytea *)palloc(value_len + VARHDRSZ);
			oracleFillEWKB(fdw_state->session, geom, value_len, VARDATA(result));
			SET_VARSIZE(result, value_len + VARHDRSZ);

			/* uninstall error context callback */
			error_context_stack = errcb.previous;

			values[j] = PointerGetDatum(result);

			/* free the storage for the object */
			oracleGeometryFree(fdw_state->session, geom);
		}
		else if (pgtype == BYTEAOID)
		{
			/* binary columns are not converted */
			bytea *result = (bytea *)palloc(value_len + VARHDRSZ);
			memcpy(VARDATA(result), value, value_len);
			SET_VARSIZE(result, value_len + VARHDRSZ);

			values[j] = PointerGetDatum(result);
		}
		else if (pgtype == BOOLOID)
			values[j] = BoolGetDatum(value[0] != '0' || value_len > 1);
		else
		{
			regproc typinput;
			HeapTuple tuple;
			Datum dat;

			/*
			 * Negative INTERVAL DAY TO SECOND need some preprocessing:
			 * In Oracle they are rendered like this: "-01 12:00:00.000000"
			 * They have to be changed to "-01 -12:00:00.000000" for PostgreSQL.
			 */
			if (fdw_state->oraTable->cols[index]->oratype == ORA_TYPE_INTERVALD2S
				&& value[0] == '-')
			{
				char *newval = palloc(strlen(value) + 2);
				char *pos = strchr(value, ' ');

				if (pos == NULL)
					elog(ERROR, "no space in INTERVAL DAY TO SECOND");
				strncpy(newval, value, pos - value + 1);
				newval[pos - value + 1] = '\0';
				strcat(newval, "-");
				strcat(newval, pos + 1);

				value = newval;
			}

			/* find the appropriate conversion function */
			tuple = SearchSysCache1(TYPEOID, ObjectIdGetDatum(pgtype));
			if (!HeapTupleIsValid(tuple))
			{
				elog(ERROR, "cache lookup failed for type %u", pgtype);
			}
			typinput = ((Form_pg_type)GETSTRUCT(tuple))->typinput;
			ReleaseSysCache(tuple);

			dat = CStringGetDatum(value);

			/* install error context callback */
			errcb.previous = error_context_stack;
			error_context_stack = &errcb;
			fdw_state->columnindex = index;

			if (pgtype == BPCHAROID || pgtype == VARCHAROID || pgtype == TEXTOID)
			{
				/* optionally strip zero bytes from string types */
				if (fdw_state->oraTable->cols[index]->strip_zeros)
				{
					char *from_p, *to_p = value;
					long new_length = value_len;

					for (from_p = value; from_p < value + value_len; ++from_p)
						if (*from_p != '\0')
							*to_p++ = *from_p;
						else
							--new_length;

					value_len = new_length;
					value[value_len] = '\0';
				}

				/* check that the string types are in the database encoding */
				(void)pg_verify_mbstr(GetDatabaseEncoding(), value, value_len, false);
			}

			/* call the type input function */
			switch (pgtype)
			{
				case BPCHAROID:
				case VARCHAROID:
				case TIMESTAMPOID:
				case TIMESTAMPTZOID:
				case INTERVALOID:
				case NUMERICOID:
					/* these functions require the type modifier */
					values[j] = OidFunctionCall3(typinput,
						dat,
						ObjectIdGetDatum(InvalidOid),
						Int32GetDatum(fdw_state->oraTable->cols[index]->pgtypmod));
					break;
				default:
					/* the others don't */
					values[j] = OidFunctionCall1(typinput, dat);
			}

			/* uninstall error context callback */
			error_context_stack = errcb.previous;
		}

		/* free the data buffer for LOBs */
		if (fdw_state->oraTable->cols[index]->oratype == ORA_TYPE_BLOB
				|| fdw_state->oraTable->cols[index]->oratype == ORA_TYPE_BFILE
				|| fdw_state->oraTable->cols[index]->oratype == ORA_TYPE_CLOB)
			pfree(value);
	}
}

/*
 * errorContextCallback
 * 		Provides the context for an error message during a type input conversion.
 * 		The argument must be a pointer to a "struct OracleFdwState".
 */
void
errorContextCallback(void *arg)
{
	struct OracleFdwState *fdw_state = (struct OracleFdwState *)arg;

	errcontext("converting column \"%s\" for foreign table scan of \"%s\", row %lu",
		quote_identifier(fdw_state->oraTable->cols[fdw_state->columnindex]->pgname),
		quote_identifier(fdw_state->oraTable->pgname),
		fdw_state->rowcount);
}

#ifdef IMPORT_API
/*
 * fold_case
 * 		Returns a palloc'ed string that is the case-folded first argument.
 */
char *
fold_case(char *name, fold_t foldcase, int collation)
{
	if (foldcase == CASE_KEEP)
		return pstrdup(name);

	if (foldcase == CASE_LOWER)
		return str_tolower(name, strlen(name), collation);

	if (foldcase == CASE_SMART)
	{
		char *upstr = str_toupper(name, strlen(name), collation);

		/* fold case only if it does not contain lower case characters */
		if (strcmp(upstr, name) == 0)
			return str_tolower(name, strlen(name), collation);
		else
			return pstrdup(name);
	}

	elog(ERROR, "impossible case folding type %d", foldcase);

	return NULL;  /* unreachable, but keeps compiler happy */
}
#endif  /* IMPORT_API */

/*
 * getIsolationLevel
 *		Converts Oracle isolation level string to oraIsoLevel.
 *      Throws an error for invalid values.
 */
oraIsoLevel
getIsolationLevel(const char *isolation_level)
{
	oraIsoLevel val = 0;

	Assert(isolation_level);

	if (strcmp(isolation_level, "serializable") == 0)
		val = ORA_TRANS_SERIALIZABLE;
	else if (strcmp(isolation_level, "read_committed") == 0)
		val = ORA_TRANS_READ_COMMITTED;
	else if (strcmp(isolation_level, "read_only") == 0)
		val = ORA_TRANS_READ_ONLY;
	else
		ereport(ERROR,
				(errcode(ERRCODE_FDW_INVALID_ATTRIBUTE_VALUE),
				errmsg("invalid value for option \"%s\"", OPT_ISOLATION_LEVEL),
				errhint("Valid values in this context are: serializable/read_committed/read_only")));

	return val;
}

/*
 * deparseLimit
 * 		Deparse LIMIT clause into FETCH FIRST N ROWS ONLY.
 * 		If OFFSET is set, the offset value is added to the LIMIT value
 * 		to give the Oracle optimizer the right clue.
 */
char *
deparseLimit(PlannerInfo *root, struct OracleFdwState *fdwState)
{
	StringInfoData limit_clause;
	char *limit_val, *offset_val = NULL;

	/* don't push down LIMIT if the query has a GROUP BY clause or aggregates */
	if (root->parse->groupClause != NULL || root->parse->hasAggs)
		return NULL;

	/* only push down LIMIT if all WHERE conditions can be pushed down */
	if (fdwState->local_conds != NIL)
		return NULL;

	/* only push down constant LIMITs that are not NULL */
	if (root->parse->limitCount != NULL && IsA(root->parse->limitCount, Const))
	{
		Const *limit = (Const *)root->parse->limitCount;

		if (limit->constisnull)
			return NULL;

		limit_val = datumToString(limit->constvalue, limit->consttype);
	}
	else
		return NULL;

	/* only consider OFFSETS that are non-NULL constants */
	if (root->parse->limitOffset != NULL && IsA(root->parse->limitOffset, Const))
	{
		Const *offset = (Const *)root->parse->limitOffset;

		if (! offset->constisnull)
			offset_val = datumToString(offset->constvalue, offset->consttype);
	}

	initStringInfo(&limit_clause);

	if (offset_val)
	{
		appendStringInfo(&limit_clause,
						 "OFFSET %s ROWS FETCH NEXT %s ROWS ONLY",
						 offset_val, limit_val);
	}
	else
		appendStringInfo(&limit_clause,
						 "FETCH FIRST %s ROWS ONLY",
						 limit_val);

	return limit_clause.data;
}

/*
 * oracleGetShareFileName
 * 		Returns the (palloc'ed) absolute path of a file in the "share" directory.
 */
char *
oracleGetShareFileName(const char *relativename)
{
	char share_path[MAXPGPATH], *result;

	get_share_path(my_exec_path, share_path);

	result = palloc(MAXPGPATH);
	snprintf(result, MAXPGPATH, "%s/%s", share_path, relativename);

	return result;
}

/*
 * oracleRegisterCallback
 * 		Register a callback for PostgreSQL transaction events.
 */
void
oracleRegisterCallback(void *arg)
{
	RegisterXactCallback(transactionCallback, arg);
	RegisterSubXactCallback(subtransactionCallback, arg);
}

/*
 * oracleUnregisterCallback
 * 		Unregister a callback for PostgreSQL transaction events.
 */
void
oracleUnregisterCallback(void *arg)
{
	UnregisterXactCallback(transactionCallback, arg);
	UnregisterSubXactCallback(subtransactionCallback, arg);
}

/*
 * oracleAlloc
 * 		Expose palloc() to Oracle functions.
 */
void
*oracleAlloc(size_t size)
{
	return palloc(size);
}

/*
 * oracleRealloc
 * 		Expose repalloc() to Oracle functions.
 */
void
*oracleRealloc(void *p, size_t size)
{
	return repalloc(p, size);
}

/*
 * oracleFree
 * 		Expose pfree() to Oracle functions.
 */
void
oracleFree(void *p)
{
	pfree(p);
}

/*
 * oracleSetHandlers
 * 		Set signal handler for SIGTERM.
 */
void
oracleSetHandlers()
{
	pqsignal(SIGTERM, oracleDie);
}

/* get a PostgreSQL error code from an oraError */
#define to_sqlstate(x) \
	(x==FDW_UNABLE_TO_ESTABLISH_CONNECTION ? ERRCODE_FDW_UNABLE_TO_ESTABLISH_CONNECTION : \
	(x==FDW_UNABLE_TO_CREATE_REPLY ? ERRCODE_FDW_UNABLE_TO_CREATE_REPLY : \
	(x==FDW_TABLE_NOT_FOUND ? ERRCODE_FDW_TABLE_NOT_FOUND : \
	(x==FDW_UNABLE_TO_CREATE_EXECUTION ? ERRCODE_FDW_UNABLE_TO_CREATE_EXECUTION : \
	(x==FDW_OUT_OF_MEMORY ? ERRCODE_FDW_OUT_OF_MEMORY : \
	(x==FDW_SERIALIZATION_FAILURE ? ERRCODE_T_R_SERIALIZATION_FAILURE : \
	(x==FDW_UNIQUE_VIOLATION ? ERRCODE_UNIQUE_VIOLATION : \
	(x==FDW_DEADLOCK_DETECTED ? ERRCODE_T_R_DEADLOCK_DETECTED : \
	(x==FDW_NOT_NULL_VIOLATION ? ERRCODE_NOT_NULL_VIOLATION : \
	(x==FDW_CHECK_VIOLATION ? ERRCODE_CHECK_VIOLATION : \
	(x==FDW_FOREIGN_KEY_VIOLATION ? ERRCODE_FOREIGN_KEY_VIOLATION : ERRCODE_FDW_ERROR)))))))))))

/*
 * oracleError_d
 * 		Report a PostgreSQL error with a detail message.
 */
void
oracleError_d(oraError sqlstate, const char *message, const char *detail)
{
	/* if the backend was terminated, report that rather than the Oracle error */
	CHECK_FOR_INTERRUPTS();

	ereport(ERROR,
			(errcode(to_sqlstate(sqlstate)),
			errmsg("%s", message),
			errdetail("%s", detail)));
}

/*
 * oracleError_sd
 * 		Report a PostgreSQL error with a string argument and a detail message.
 */
void
oracleError_sd(oraError sqlstate, const char *message, const char *arg, const char *detail)
{
	ereport(ERROR,
			(errcode(to_sqlstate(sqlstate)),
			errmsg(message, arg),
			errdetail("%s", detail)));
}

/*
 * oracleError_ssdh
 * 		Report a PostgreSQL error with two string arguments, a detail message and a hint.
 */
void
oracleError_ssdh(oraError sqlstate, const char *message, const char *arg1, const char* arg2, const char *detail, const char *hint)
{
	ereport(ERROR,
			(errcode(to_sqlstate(sqlstate)),
			errmsg(message, arg1, arg2),
			errdetail("%s", detail),
			errhint("%s", hint)));
}

/*
 * oracleError_ii
 * 		Report a PostgreSQL error with 2 integer arguments.
 */
void
oracleError_ii(oraError sqlstate, const char *message, int arg1, int arg2)
{
	ereport(ERROR,
			(errcode(to_sqlstate(sqlstate)),
			errmsg(message, arg1, arg2)));
}

/*
 * oracleError_i
 * 		Report a PostgreSQL error with integer argument.
 */
void
oracleError_i(oraError sqlstate, const char *message, int arg)
{
	ereport(ERROR,
			(errcode(to_sqlstate(sqlstate)),
			errmsg(message, arg)));
}

/*
 * oracleError
 * 		Report a PostgreSQL error without detail message.
 */
void
oracleError(oraError sqlstate, const char *message)
{
	/* use errcode_for_file_access() if the message contains %m */
	if (strstr(message, "%m")) {
		ereport(ERROR,
				(errcode_for_file_access(),
				errmsg(message, "")));
	} else {
		ereport(ERROR,
				(errcode(to_sqlstate(sqlstate)),
				errmsg("%s", message)));
	}
}

/*
 * oracleDebug2
 * 		Report a PostgreSQL message at level DEBUG2.
 */
void
oracleDebug2(const char *message)
{
	elog(DEBUG2, "%s", message);
}

/*
 * initializePostGIS
 * 		Checks if PostGIS is installed and sets GEOMETRYOID if it is.
 */
void
initializePostGIS()
{
	CatCList *catlist;
	int i, argcount = 1;
	Oid argtypes[] = { INTERNALOID };

	/* this needs to be done only once per database session */
	if (geometry_is_setup)
		return;

	geometry_is_setup = true;

	/* find all functions called "geometry_recv" with "internal" argument type */
	catlist = SearchSysCacheList2(
					PROCNAMEARGSNSP,
					CStringGetDatum("geometry_recv"),
					PointerGetDatum(buildoidvector(argtypes, argcount)));

	for (i = 0; i < catlist->n_members; i++)
	{
		HeapTuple proctup = &catlist->members[i]->tuple;
		Form_pg_proc procform = (Form_pg_proc)GETSTRUCT(proctup);

		/*
		 * If we find more than one "geometry_recv" function, there is
		 * probably more than one installation of PostGIS.
		 * We don't know which one to use and give up trying.
		 */
		if (GEOMETRYOID != InvalidOid)
		{
			elog(DEBUG1, "oracle_fdw: more than one PostGIS installation found, giving up");

			GEOMETRYOID = InvalidOid;
			break;
		}

		/* "geometry" is the return type of the "geometry_recv" function */
		GEOMETRYOID = procform->prorettype;

		elog(DEBUG1, "oracle_fdw: PostGIS is installed, GEOMETRYOID = %d", GEOMETRYOID);
	}
	ReleaseSysCacheList(catlist);
}

/*
 * Initialize context
 */
static void
initializeContext(struct OracleFdwState *fdwState,
						 PlannerInfo *root,
						 RelOptInfo *foreignrel,
						 RelOptInfo *scanrel,
						 deparse_expr_cxt *context)
{
	context->root = root;
	context->foreignrel = foreignrel;
	context->scanrel = scanrel;
	context->session = fdwState->session;
	context->params_list = &(fdwState->params);
	context->oraTable = fdwState->oraTable;
	context->string_comparison = false;
	context->handle_length_func = false;
	context->can_pushdown_function = false;
	context->handle_aggref = false;
}

/*
 * Find an equivalence class member expression to be computed as a sort column
 * in the given target.
 */
Expr *
find_em_expr_for_input_target(PlannerInfo *root,
							  EquivalenceClass *ec,
							  PathTarget *target)
{
	ListCell   *lc1;
	int			i;

	i = 0;
	foreach(lc1, target->exprs)
	{
		Expr	   *expr = (Expr *) lfirst(lc1);
		Index		sgref = get_pathtarget_sortgroupref(target, i);
		ListCell   *lc2;

		/* Ignore non-sort expressions */
		if (sgref == 0 ||
			get_sortgroupref_clause_noerr(sgref,
										  root->parse->sortClause) == NULL)
		{
			i++;
			continue;
		}

		/* We ignore binary-compatible relabeling on both ends */
		while (expr && IsA(expr, RelabelType))
			expr = ((RelabelType *) expr)->arg;

		/* Locate an EquivalenceClass member matching this expr, if any */
		foreach(lc2, ec->ec_members)
		{
			EquivalenceMember *em = (EquivalenceMember *) lfirst(lc2);
			Expr	   *em_expr;

			/* Don't match constants */
			if (em->em_is_const)
				continue;

			/* Ignore child members */
			if (em->em_is_child)
				continue;

			/* Match if same expression (after stripping relabel) */
			em_expr = em->em_expr;
			while (em_expr && IsA(em_expr, RelabelType))
				em_expr = ((RelabelType *) em_expr)->arg;

			if (equal(em_expr, expr))
				return em->em_expr;
		}

		i++;
	}

	elog(ERROR, "could not find pathkey item to sort");
	return NULL;				/* keep compiler quiet */
}

/*
 * add_foreign_grouping_paths
 *		Add foreign path for grouping and/or aggregation.
 *
 * Given input_rel represents the underlying scan.  The paths are added to the
 * given grouped_rel.
 */
static void
add_foreign_grouping_paths(PlannerInfo *root, RelOptInfo *input_rel,
						   RelOptInfo *grouped_rel,
						   GroupPathExtraData *extra)
{
	Query	   *parse = root->parse;
	struct OracleFdwState *ifpinfo = input_rel->fdw_private;
	struct OracleFdwState *fpinfo = grouped_rel->fdw_private;
	ForeignPath *grouppath;
	double		rows;
	int			width;
	Cost		startup_cost;
	Cost		total_cost;

	/* Nothing to be done, if there is no grouping or aggregation required. */
	if (!parse->groupClause && !parse->groupingSets && !parse->hasAggs &&
		!root->hasHavingQual)
		return;

	Assert(extra->patype == PARTITIONWISE_AGGREGATE_NONE ||
		   extra->patype == PARTITIONWISE_AGGREGATE_FULL);

	/* save the input_rel as outerrel in fpinfo */
	fpinfo->outerrel = input_rel;

	/*
	 * Copy foreign table, foreign server, user mapping, FDW options etc.
	 * details from the input relation's fpinfo.
	 */
	fpinfo->table = ifpinfo->table;
	fpinfo->server = ifpinfo->server;
	merge_fdw_state(fpinfo, ifpinfo, NULL);

	/*
	 * Assess if it is safe to push down aggregation and grouping.
	 *
	 * Use HAVING qual from extra. In case of child partition, it will have
	 * translated Vars.
	 */
	if (!foreign_grouping_ok(root, grouped_rel, extra->havingQual))
		return;

	/*
	 * Compute the selectivity and cost of the local_conds, so we don't have
	 * to do it over again for each path.  (Currently we create just a single
	 * path here, but in future it would be possible that we build more paths
	 * such as pre-sorted paths as in oracleGetForeignPaths and
	 * oracleGetForeignJoinPaths.)  The best we can do for these conditions is
	 * to estimate selectivity on the basis of local statistics.
	 */
	fpinfo->local_conds_sel = clauselist_selectivity(root,
													 fpinfo->local_conds,
													 0,
													 JOIN_INNER,
													 NULL);

	cost_qual_eval(&fpinfo->local_conds_cost, fpinfo->local_conds, root);

	/* Estimate the cost of push down */
	estimate_path_cost_size(root, grouped_rel, NIL, NIL, NULL,
							&rows, &width, &startup_cost, &total_cost);

	/* store cost estimation results */
	grouped_rel->rows = rows;

	/* Now update this information in the fpinfo */
	fpinfo->rows = rows;
	fpinfo->width = width;
	fpinfo->startup_cost = startup_cost;
	fpinfo->total_cost = total_cost;

	/* Create and add foreign path to the grouping relation. */
	grouppath = create_foreign_upper_path(root,
										  grouped_rel,
										  grouped_rel->reltarget,
										  rows,
										  startup_cost,
										  total_cost,
										  NIL,	/* no pathkeys */
										  NULL,
										  NIL); /* no fdw_private */

	/* Add generated path into grouped_rel by add_path(). */
	add_path(grouped_rel, (Path *) grouppath);
}


/*
 * add_foreign_ordered_paths
 *		Add foreign paths for performing the final sort remotely.
 *
 * Given input_rel contains the source-data Paths.  The paths are added to the
 * given ordered_rel.
 */
static void
add_foreign_ordered_paths(PlannerInfo *root, RelOptInfo *input_rel,
						  RelOptInfo *ordered_rel)
{
	Query	   *parse = root->parse;
	struct OracleFdwState *ifpinfo = input_rel->fdw_private;
	struct OracleFdwState *fpinfo = ordered_rel->fdw_private;
	OracleFdwPathExtraData *fpextra;
	double		rows;
	int			width;
	Cost		startup_cost;
	Cost		total_cost;
	List	   *fdw_private;
	ForeignPath *ordered_path;
	ListCell   *lc;
	deparse_expr_cxt context;

	/* Shouldn't get here unless the query has ORDER BY */
	Assert(parse->sortClause);

	/* We don't support cases where there are any SRFs in the targetlist */
	if (parse->hasTargetSRFs)
		return;

	/* Save the input_rel as outerrel in fpinfo */
	fpinfo->outerrel = input_rel;

	/* Init context */
	initializeContext(fpinfo, root, ordered_rel, input_rel, &context);

	/*
	 * Copy foreign table, foreign server, user mapping, FDW options etc.
	 * details from the input relation's fpinfo.
	 */
	fpinfo->table = ifpinfo->table;
	fpinfo->server = ifpinfo->server;
	merge_fdw_state(fpinfo, ifpinfo, NULL);

	/*
	 * If the input_rel is a base or join relation, we would already have
	 * considered pushing down the final sort to the remote server when
	 * creating pre-sorted foreign paths for that relation, because the
	 * query_pathkeys is set to the root->sort_pathkeys in that case (see
	 * standard_qp_callback()).
	 */
	if (input_rel->reloptkind == RELOPT_BASEREL ||
		input_rel->reloptkind == RELOPT_JOINREL)
	{
		Assert(root->query_pathkeys == root->sort_pathkeys);

		/* Safe to push down if the query_pathkeys is safe to push down */
		fpinfo->pushdown_safe = ifpinfo->qp_is_pushdown_safe;

		return;
	}

	/* The input_rel should be a grouping relation */
	Assert(input_rel->reloptkind == RELOPT_UPPER_REL &&
		   ifpinfo->stage == UPPERREL_GROUP_AGG);

	/*
	 * We try to create a path below by extending a simple foreign path for
	 * the underlying grouping relation to perform the final sort remotely,
	 * which is stored into the fdw_private list of the resulting path.
	 */

	/* Assess if it is safe to push down the final sort */
	foreach(lc, root->sort_pathkeys)
	{
		PathKey    *pathkey = (PathKey *) lfirst(lc);
		EquivalenceClass *pathkey_ec = pathkey->pk_eclass;
		Expr	   *sort_expr;
		Oid	em_type;

		/*
		 * deparseExpr would detect volatile expressions as well, but
		 * checking ec_has_volatile here saves some cycles.
		 */
		if (pathkey_ec->ec_has_volatile)
			return;

		/* Get the sort expression for the pathkey_ec */
		sort_expr = find_em_expr_for_input_target(root,
												  pathkey_ec,
												  input_rel->reltarget);

		em_type = exprType((Node *)sort_expr);

		/* expressions of a type different from this are not safe to push down into ORDER BY clauses */
		if (em_type != INT8OID && em_type != INT2OID
			&& em_type != INT4OID && em_type != OIDOID
			&& em_type != FLOAT4OID && em_type != FLOAT8OID
			&& em_type != NUMERICOID && em_type != DATEOID
			&& em_type != TIMESTAMPOID && em_type != TIMESTAMPTZOID
			&& em_type != INTERVALOID)
			return;

		/* If it's unsafe to remote, we cannot push down the final sort */
		if (!deparseExpr(sort_expr, &context))
			return;
	}

	/* Safe to push down */
	fpinfo->pushdown_safe = true;

	/* Construct PgFdwPathExtraData */
	fpextra = (OracleFdwPathExtraData *) palloc0(sizeof(OracleFdwPathExtraData));
	fpextra->target = root->upper_targets[UPPERREL_ORDERED];
	fpextra->has_final_sort = true;

	/* Estimate the costs of performing the final sort remotely */
	estimate_path_cost_size(root, input_rel, NIL, root->sort_pathkeys, fpextra,
							&rows, &width, &startup_cost, &total_cost);

	/*
	 * Build the fdw_private list that will be used by oracleGetForeignPlan.
	 * Items in the list must match order in enum FdwPathPrivateIndex.
	 */
	fdw_private = list_make2(makeInteger(true), makeInteger(false));

	/* Create foreign ordering path */
	ordered_path = create_foreign_upper_path(root,
											 input_rel,
											 root->upper_targets[UPPERREL_ORDERED],
											 rows,
											 startup_cost,
											 total_cost,
											 root->sort_pathkeys,
											 NULL,	/* no extra plan */
											 fdw_private);

	/* and add it to the ordered_rel */
	add_path(ordered_rel, (Path *) ordered_path);
}


/*
 * add_foreign_final_paths
 *		Add foreign paths for performing the final processing remotely.
 *
 * Given input_rel contains the source-data Paths.  The paths are added to the
 * given final_rel.
 */
static void
add_foreign_final_paths(PlannerInfo *root, RelOptInfo *input_rel,
						RelOptInfo *final_rel,
						FinalPathExtraData *extra)
{
	Query	   *parse = root->parse;
	struct OracleFdwState *ifpinfo = (struct OracleFdwState *) input_rel->fdw_private;
	struct OracleFdwState *fpinfo = (struct OracleFdwState *) final_rel->fdw_private;
	bool		has_final_sort = false;
	List		*pathkeys = NIL;
	OracleFdwPathExtraData *fpextra;
	double		rows;
	int		width;
	Cost		startup_cost;
	Cost		total_cost;
	List		*fdw_private;
	ForeignPath *final_path;
	deparse_expr_cxt context;

	/* not pushdown if limit_clause is NULL */
	if (!ifpinfo->limit_clause)
		return;

	/*
	 * Currently, we only support this for SELECT commands
	 */
	if (parse->commandType != CMD_SELECT)
		return;

	/*
	 * No work if there is no FOR UPDATE/SHARE clause and if there is no need
	 * to add a LIMIT node
	 */
	if (!parse->rowMarks && !extra->limit_needed)
		return;

	/* We don't support cases where there are any SRFs in the targetlist */
	if (parse->hasTargetSRFs)
		return;

	/* Save the input_rel as outerrel in fpinfo */
	fpinfo->outerrel = input_rel;

	/* Init context */
	initializeContext(fpinfo, root, final_rel, input_rel, &context);

	/*
	 * Copy foreign table, foreign server, user mapping, FDW options etc.
	 * details from the input relation's fpinfo.
	 */
	fpinfo->table = ifpinfo->table;
	fpinfo->server = ifpinfo->server;
	merge_fdw_state(fpinfo, ifpinfo, NULL);

	/*
	 * If there is no need to add a LIMIT node, there might be a ForeignPath
	 * in the input_rel's pathlist that implements all behavior of the query.
	 * Note: we would already have accounted for the query's FOR UPDATE/SHARE
	 * (if any) before we get here.
	 */
	if (!extra->limit_needed)
	{
		ListCell   *lc;

		Assert(parse->rowMarks);

		/*
		 * Grouping and aggregation are not supported with FOR UPDATE/SHARE,
		 * so the input_rel should be a base, join, or ordered relation; and
		 * if it's an ordered relation, its input relation should be a base or
		 * join relation.
		 */
		Assert(input_rel->reloptkind == RELOPT_BASEREL ||
			   input_rel->reloptkind == RELOPT_JOINREL ||
			   (input_rel->reloptkind == RELOPT_UPPER_REL &&
				ifpinfo->stage == UPPERREL_ORDERED &&
				(ifpinfo->outerrel->reloptkind == RELOPT_BASEREL ||
				 ifpinfo->outerrel->reloptkind == RELOPT_JOINREL)));

		foreach(lc, input_rel->pathlist)
		{
			Path	   *path = (Path *) lfirst(lc);

			/*
			 * apply_scanjoin_target_to_paths() uses create_projection_path()
			 * to adjust each of its input paths if needed, whereas
			 * create_ordered_paths() uses apply_projection_to_path() to do
			 * that.  So the former might have put a ProjectionPath on top of
			 * the ForeignPath; look through ProjectionPath and see if the
			 * path underneath it is ForeignPath.
			 */
			if (IsA(path, ForeignPath) ||
				(IsA(path, ProjectionPath) &&
				 IsA(((ProjectionPath *) path)->subpath, ForeignPath)))
			{
				/*
				 * Create foreign final path; this gets rid of a
				 * no-longer-needed outer plan (if any), which makes the
				 * EXPLAIN output look cleaner
				 */
				final_path = create_foreign_upper_path(root,
													   path->parent,
													   path->pathtarget,
													   path->rows,
													   path->startup_cost,
													   path->total_cost,
													   path->pathkeys,
													   NULL,	/* no extra plan */
													   NULL);	/* no fdw_private */

				/* and add it to the final_rel */
				add_path(final_rel, (Path *) final_path);

				/* Safe to push down */
				fpinfo->pushdown_safe = true;

				return;
			}
		}

		/*
		 * If we get here it means no ForeignPaths; since we would already
		 * have considered pushing down all operations for the query to the
		 * remote server, give up on it.
		 */
		return;
	}

	Assert(extra->limit_needed);

	/*
	 * If the input_rel is an ordered relation, replace the input_rel with its
	 * input relation
	 */
	if (input_rel->reloptkind == RELOPT_UPPER_REL &&
		ifpinfo->stage == UPPERREL_ORDERED)
	{
		input_rel = ifpinfo->outerrel;
		ifpinfo = (struct OracleFdwState *) input_rel->fdw_private;
		has_final_sort = true;
		pathkeys = root->sort_pathkeys;
	}

	/*
	* According to oracle specification, if using LIMIT without ORDER BY,
	* it does not make sense because the result will be returned in random order,
	* so to ensure stable result, we should not pushdown LIMIT without ORDER BY.
	*/
	if (!pathkeys)
		return;

	/* The input_rel should be a base, join, or grouping relation */
	Assert(input_rel->reloptkind == RELOPT_BASEREL ||
		   input_rel->reloptkind == RELOPT_JOINREL ||
		   (input_rel->reloptkind == RELOPT_UPPER_REL &&
			ifpinfo->stage == UPPERREL_GROUP_AGG));

	/*
	 * We try to create a path below by extending a simple foreign path for
	 * the underlying base, join, or grouping relation to perform the final
	 * sort (if has_final_sort) and the LIMIT restriction remotely, which is
	 * stored into the fdw_private list of the resulting path.  (We
	 * re-estimate the costs of sorting the underlying relation, if
	 * has_final_sort.)
	 */

	/*
	 * Assess if it is safe to push down the LIMIT and OFFSET to the remote
	 * server
	 */

	/*
	 * If the underlying relation has any local conditions, the LIMIT/OFFSET
	 * cannot be pushed down.
	 */
	if (ifpinfo->local_conds)
		return;

	/* Safe to push down */
	fpinfo->pushdown_safe = true;

	/* Construct PgFdwPathExtraData */
	fpextra = (OracleFdwPathExtraData *) palloc0(sizeof(OracleFdwPathExtraData));
	fpextra->target = root->upper_targets[UPPERREL_FINAL];
	fpextra->has_final_sort = has_final_sort;
	fpextra->has_limit = extra->limit_needed;
	fpextra->limit_tuples = extra->limit_tuples;
	fpextra->count_est = extra->count_est;
	fpextra->offset_est = extra->offset_est;

	estimate_path_cost_size(root, input_rel, NIL, pathkeys, fpextra,
							&rows, &width, &startup_cost, &total_cost);

	/*
	 * Build the fdw_private list that will be used by oracleGetForeignPlan.
	 * Items in the list must match order in enum FdwPathPrivateIndex.
	 */
	fdw_private = list_make2(makeInteger(has_final_sort),
							 makeInteger(extra->limit_needed));

	/*
	 * Although ORDER BY and LIMIT are marked to be pushed down here,
	 * however, the core code will decide the best path according to
	 * not only the marked flags but also the estimated cost.
	 * So, to make sure ORDER BY and LIMIT are always be pushed down,
	 * we need to fix cost small enough.
	 */
	startup_cost = 10.0;
	total_cost = startup_cost + rows * 10.0;

	/*
	 * Create foreign final path; this gets rid of a no-longer-needed outer
	 * plan (if any), which makes the EXPLAIN output look cleaner
	 */
	final_path = create_foreign_upper_path(root,
										   input_rel,
										   root->upper_targets[UPPERREL_FINAL],
										   rows,
										   startup_cost,
										   total_cost,
										   pathkeys,
										   NULL,	/* no extra plan */
										   fdw_private);

	/* and add it to the final_rel */
	add_path(final_rel, (Path *) final_path);
}


/*
 * Merge FDW state from input relations into a new state for a join
 * or an upper rel.
 *
 * For a join relation, FDW-specific information about the inner and outer
 * relations is provided using fpinfo_i and fpinfo_o.  For an upper relation,
 * fpinfo_o provides the information for the input relation; fpinfo_i is
 * expected to NULL.
 */
static void
merge_fdw_state(struct OracleFdwState *fpinfo,
				  const struct OracleFdwState *fpinfo_o,
				  const struct OracleFdwState *fpinfo_i)
{
	/* We must always have fpinfo_o. */
	Assert(fpinfo_o);

	/* fpinfo_i may be NULL, but if present the servers must both match. */
	Assert(!fpinfo_i ||
		   fpinfo_i->server->serverid == fpinfo_o->server->serverid);

	/*
	 * Copy the server specific FDW options.  (For a join, both relations come
	 * from the same server, so the server options should have the same value
	 * for both relations.)
	 */
	fpinfo->fdw_startup_cost = fpinfo_o->fdw_startup_cost;
	fpinfo->fdw_tuple_cost = fpinfo_o->fdw_tuple_cost;

	if (fpinfo_o->limit_clause)
		fpinfo->limit_clause = pstrdup(fpinfo_o->limit_clause);

	/* oraTable is used for deparseExpr */
	fpinfo->oraTable = fpinfo_o->oraTable;

	/* copy outerrel's infomation to fdwstate */
	fpinfo->dbserver = fpinfo_o->dbserver;
	fpinfo->isolation_level = fpinfo_o->isolation_level;
	fpinfo->user     = fpinfo_o->user;
	fpinfo->password = fpinfo_o->password;
	fpinfo->nls_lang = fpinfo_o->nls_lang;
	fpinfo->have_nchar = fpinfo_o->have_nchar;
}

/*
 * Assess whether the aggregation, grouping and having operations can be pushed
 * down to the foreign server.  As a side effect, save information we obtain in
 * this function to PgFdwRelationInfo of the input relation.
 */
static bool
foreign_grouping_ok(PlannerInfo *root, RelOptInfo *grouped_rel,
					Node *havingQual)
{
	Query	   *query = root->parse;
	struct OracleFdwState *fpinfo = (struct OracleFdwState *) grouped_rel->fdw_private;
	PathTarget *grouping_target = grouped_rel->reltarget;
	struct OracleFdwState *ofpinfo;
	ListCell   *lc;
	int			i;
	List	   *tlist = NIL;
	deparse_expr_cxt context;

	/* init context */
	initializeContext(fpinfo, root, grouped_rel, fpinfo->outerrel, &context);

	/* We currently don't support pushing Grouping Sets. */
	if (query->groupingSets)
		return false;

	/* Get the fpinfo of the underlying scan relation. */
	ofpinfo = (struct OracleFdwState *) fpinfo->outerrel->fdw_private;

	/*
	 * If underlying scan relation has any local conditions, those conditions
	 * are required to be applied before performing aggregation.  Hence the
	 * aggregate cannot be pushed down.
	 */
	if (ofpinfo->local_conds)
		return false;

	/*
	 * Examine grouping expressions, as well as other expressions we'd need to
	 * compute, and check whether they are safe to push down to the foreign
	 * server.  All GROUP BY expressions will be part of the grouping target
	 * and thus there is no need to search for them separately.  Add grouping
	 * expressions into target list which will be passed to foreign server.
	 *
	 * A tricky fine point is that we must not put any expression into the
	 * target list that is just a foreign param (that is, something that
	 * deparse.c would conclude has to be sent to the foreign server).  If we
	 * do, the expression will also appear in the fdw_exprs list of the plan
	 * node, and setrefs.c will get confused and decide that the fdw_exprs
	 * entry is actually a reference to the fdw_scan_tlist entry, resulting in
	 * a broken plan.  Somewhat oddly, it's OK if the expression contains such
	 * a node, as long as it's not at top level; then no match is possible.
	 */
	i = 0;
	foreach(lc, grouping_target->exprs)
	{
		Expr	   *expr = (Expr *) lfirst(lc);
		Index		sgref = get_pathtarget_sortgroupref(grouping_target, i);
		ListCell   *l;

		/*
		 * Oracle does not support aggregation with CLOB data.
		 * Mark here and check again in T_Var node of deparseExpr
		 */
		context.handle_aggref = true;

		/* Check whether this expression is part of GROUP BY clause */
		if (sgref && get_sortgroupref_clause_noerr(sgref, query->groupClause))
		{
			TargetEntry *tle;

			/*
			 * If any GROUP BY expression is not shippable, then we cannot
			 * push down aggregation to the foreign server.
			 */
			if (!deparseExpr(expr, &context))
				return false;

			/*
			 * If it would be a foreign param, we can't put it into the tlist,
			 * so we have to fail.
			 */
			if (is_foreign_param(root, grouped_rel, expr))
				return false;

			/*
			 * Pushable, so add to tlist.  We need to create a TLE for this
			 * expression and apply the sortgroupref to it.  We cannot use
			 * add_to_flat_tlist() here because that avoids making duplicate
			 * entries in the tlist.  If there are duplicate entries with
			 * distinct sortgrouprefs, we have to duplicate that situation in
			 * the output tlist.
			 */
			tle = makeTargetEntry(expr, list_length(tlist) + 1, NULL, false);
			tle->ressortgroupref = sgref;
			tlist = lappend(tlist, tle);
		}
		else
		{
			/*
			 * Non-grouping expression we need to compute.  Can we ship it
			 * as-is to the foreign server?
			 */
			if (deparseExpr(expr, &context) &&
				!is_foreign_param(root, grouped_rel, expr))
			{
				/* Yes, so add to tlist as-is; OK to suppress duplicates */
				tlist = add_to_flat_tlist(tlist, list_make1(expr));
			}
			else
			{
				/* Not pushable as a whole; extract its Vars and aggregates */
				List	   *aggvars;

				aggvars = pull_var_clause((Node *) expr,
										  PVC_INCLUDE_AGGREGATES);

				/*
				 * If any aggregate expression is not shippable, then we
				 * cannot push down aggregation to the foreign server.  (We
				 * don't have to check is_foreign_param, since that certainly
				 * won't return true for any such expression.)
				 */
				if (!deparseExpr((Expr *) aggvars, &context))
					return false;

				/*
				 * Add aggregates, if any, into the targetlist.  Plain Vars
				 * outside an aggregate can be ignored, because they should be
				 * either same as some GROUP BY column or part of some GROUP
				 * BY expression.  In either case, they are already part of
				 * the targetlist and thus no need to add them again.  In fact
				 * including plain Vars in the tlist when they do not match a
				 * GROUP BY column would cause the foreign server to complain
				 * that the shipped query is invalid.
				 */
				foreach(l, aggvars)
				{
					Expr	   *expr = (Expr *) lfirst(l);

					if (IsA(expr, Aggref))
						tlist = add_to_flat_tlist(tlist, list_make1(expr));
				}
			}
		}

		/* Reset */
		context.handle_aggref = false;

		i++;
	}

	/*
	 * Classify the pushable and non-pushable HAVING clauses and save them in
	 * remote_conds and local_conds of the grouped rel's fpinfo.
	 */
	if (havingQual)
	{
		ListCell   *lc;

		foreach(lc, (List *) havingQual)
		{
			Expr	   *expr = (Expr *) lfirst(lc);
			RestrictInfo *rinfo;

			/*
			 * Currently, the core code doesn't wrap havingQuals in
			 * RestrictInfos, so we must make our own.
			 */
			Assert(!IsA(expr, RestrictInfo));
			rinfo = make_restrictinfo(root,
									  expr,
									  true,
									  false,
									  false,
									  root->qual_security_level,
									  grouped_rel->relids,
									  NULL,
									  NULL);

			if (deparseExpr(expr, &context))
				fpinfo->remote_conds = lappend(fpinfo->remote_conds, rinfo);
			else
				fpinfo->local_conds = lappend(fpinfo->local_conds, rinfo);
		}
	}

	/*
	 * If there are any local conditions, pull Vars and aggregates from it and
	 * check whether they are safe to pushdown or not.
	 */
	if (fpinfo->local_conds)
	{
		List	   *aggvars = NIL;
		ListCell   *lc;

		foreach(lc, fpinfo->local_conds)
		{
			RestrictInfo *rinfo = lfirst_node(RestrictInfo, lc);

			aggvars = list_concat(aggvars,
								  pull_var_clause((Node *) rinfo->clause,
												  PVC_INCLUDE_AGGREGATES));
		}

		foreach(lc, aggvars)
		{
			Expr	   *expr = (Expr *) lfirst(lc);

			/*
			 * If aggregates within local conditions are not safe to push
			 * down, then we cannot push down the query.  Vars are already
			 * part of GROUP BY clause which are checked above, so no need to
			 * access them again here.  Again, we need not check
			 * is_foreign_param for a foreign aggregate.
			 */
			if (IsA(expr, Aggref))
			{
				if (!deparseExpr(expr, &context))
					return false;

				tlist = add_to_flat_tlist(tlist, list_make1(expr));
			}
		}
	}

	/* Store generated targetlist */
	fpinfo->grouped_tlist = tlist;

	/* Safe to pushdown */
	fpinfo->pushdown_safe = true;

	/*
	 * Set # of retrieved rows and cached relation costs to some negative
	 * value, so that we can detect when they are set to some sensible values,
	 * during one (usually the first) of the calls to estimate_path_cost_size.
	 */
	fpinfo->retrieved_rows = -1;
	fpinfo->rel_startup_cost = -1;
	fpinfo->rel_total_cost = -1;

	/*
	 * Set the string describing this grouped relation to be used in EXPLAIN
	 * output of corresponding ForeignScan.  Note that the decoration we add
	 * to the base relation name mustn't include any digits, or it'll confuse
	 * oracleExplainForeignScan.
	 */
	fpinfo->relation_name = psprintf("Aggregate on (%s)",
									 ofpinfo->relation_name);

	/* copy outerrel's infomation to fdwstate */
	fpinfo->dbserver = ofpinfo->dbserver;
	fpinfo->isolation_level = ofpinfo->isolation_level;
	fpinfo->user     = ofpinfo->user;
	fpinfo->password = ofpinfo->password;
	fpinfo->nls_lang = ofpinfo->nls_lang;
	fpinfo->have_nchar = ofpinfo->have_nchar;

	return true;
}

/*
 * Returns true if given expr is something we'd have to send the value of
 * to the foreign server.
 *
 * This should return true when the expression is a shippable node that
 * deparseExpr would add to context->params_list.  Note that we don't care
 * if the expression *contains* such a node, only whether one appears at top
 * level.  We need this to detect cases where setrefs.c would recognize a
 * false match between an fdw_exprs item (which came from the params_list)
 * and an entry in fdw_scan_tlist (which we're considering putting the given
 * expression into).
 */
static bool
is_foreign_param(PlannerInfo *root,
				 RelOptInfo *baserel,
				 Expr *expr)
{
	if (expr == NULL)
		return false;

	switch (nodeTag(expr))
	{
		case T_Var:
			{
				/* It would have to be sent unless it's a foreign Var */
				Var		   *var = (Var *) expr;
				struct OracleFdwState *fpinfo = (struct OracleFdwState *) (baserel->fdw_private);
				Relids		relids;

				if (IS_UPPER_REL(baserel))
					relids = fpinfo->outerrel->relids;
				else
					relids = baserel->relids;

				if (bms_is_member(var->varno, relids) && var->varlevelsup == 0)
					return false;	/* foreign Var, so not a param */
				else
					return true;	/* it'd have to be a param */
				break;
			}
		case T_Param:
			/* Params always have to be sent to the foreign server */
			return true;
		default:
			break;
	}
	return false;
}

/*
 * estimate_path_cost_size
 *		Get cost and size estimates for a foreign scan on given foreign relation
 *		either a base relation or a join between foreign relations or an upper
 *		relation containing foreign relations.
 *
 * param_join_conds are the parameterization clauses with outer relations.
 * pathkeys specify the expected sort order if any for given path being costed.
 * fpextra specifies additional post-scan/join-processing steps such as the
 * final sort and the LIMIT restriction.
 *
 * The function returns the cost and size estimates in p_rows, p_width,
 * p_startup_cost and p_total_cost variables.
 */
static void
estimate_path_cost_size(PlannerInfo *root,
						RelOptInfo *foreignrel,
						List *param_join_conds,
						List *pathkeys,
						OracleFdwPathExtraData *fpextra,
						double *p_rows, int *p_width,
						Cost *p_startup_cost, Cost *p_total_cost)
{
	struct OracleFdwState *fpinfo = (struct OracleFdwState *) foreignrel->fdw_private;
	double		rows;
	double		retrieved_rows;
	int			width;
	Cost		startup_cost;
	Cost		total_cost;
	Cost		run_cost = 0;

	/* Make sure the core code has set up the relation's reltarget */
	Assert(foreignrel->reltarget);

	/*
	 * We don't support join conditions in this mode (hence, no
	 * parameterized paths can be made).
	 */
	Assert(param_join_conds == NIL);

	/*
	 * We will come here again and again with different set of pathkeys or
	 * additional post-scan/join-processing steps that caller wants to
	 * cost.  We don't need to calculate the cost/size estimates for the
	 * underlying scan, join, or grouping each time.  Instead, use those
	 * estimates if we have cached them already.
	 */
	if (fpinfo->rel_startup_cost >= 0 && fpinfo->rel_total_cost >= 0)
	{
		Assert(fpinfo->retrieved_rows >= 0);

		rows = fpinfo->rows;
		retrieved_rows = fpinfo->retrieved_rows;
		width = fpinfo->width;
		startup_cost = fpinfo->rel_startup_cost;
		run_cost = fpinfo->rel_total_cost - fpinfo->rel_startup_cost;

		/*
		 * If we estimate the costs of a foreign scan or a foreign join
		 * with additional post-scan/join-processing steps, the scan or
		 * join costs obtained from the cache wouldn't yet contain the
		 * eval costs for the final scan/join target, which would've been
		 * updated by apply_scanjoin_target_to_paths(); add the eval costs
		 * now.
		 */
		if (fpextra && !IS_UPPER_REL(foreignrel))
		{
			/* Shouldn't get here unless we have LIMIT */
			Assert(fpextra->has_limit);
			Assert(foreignrel->reloptkind == RELOPT_BASEREL ||
				   foreignrel->reloptkind == RELOPT_JOINREL);
			startup_cost += foreignrel->reltarget->cost.startup;
			run_cost += foreignrel->reltarget->cost.per_tuple * rows;
		}
	}
	else if (IS_UPPER_REL(foreignrel))
	{
		RelOptInfo *outerrel = fpinfo->outerrel;
		struct OracleFdwState *ofpinfo;
		AggClauseCosts aggcosts;
		double input_rows;
		int numGroupCols;
		double numGroups = 1;

		/* The upper relation should have its outer relation set */
		Assert(outerrel);
		/* and that outer relation should have its reltarget set */
		Assert(outerrel->reltarget);

		/*
		 * This cost model is mixture of costing done for sorted and
		 * hashed aggregates in cost_agg().  We are not sure which
		 * strategy will be considered at remote side, thus for
		 * simplicity, we put all startup related costs in startup_cost
		 * and all finalization and run cost are added in total_cost.
		 */

		ofpinfo = (struct OracleFdwState *)outerrel->fdw_private;

		/* Get rows from input rel */
		input_rows = ofpinfo->rows;

		/* Collect statistics about aggregates for estimating costs. */
		MemSet(&aggcosts, 0, sizeof(AggClauseCosts));
		if (root->parse->hasAggs)
		{
#if PG_VERSION_NUM >= 140000
			get_agg_clause_costs(root, AGGSPLIT_SIMPLE, &aggcosts);
#else

			get_agg_clause_costs(root, (Node *) fpinfo->grouped_tlist,
								 AGGSPLIT_SIMPLE, &aggcosts);

			/*
			 * The cost of aggregates in the HAVING qual will be the same
			 * for each child as it is for the parent, so there's no need
			 * to use a translated version of havingQual.
			 */
			get_agg_clause_costs(root, (Node *)root->parse->havingQual,
								 AGGSPLIT_SIMPLE, &aggcosts);
#endif
		}

		/* Get number of grouping columns and possible number of groups */
		numGroupCols = list_length(root->parse->groupClause);
		numGroups = estimate_num_groups(root,
										get_sortgrouplist_exprs(root->parse->groupClause,
																fpinfo->grouped_tlist),
										input_rows,
#if PG_VERSION_NUM >= 140000
										NULL,
#endif
										NULL);

		/*
		 * Get the retrieved_rows and rows estimates.  If there are HAVING
		 * quals, account for their selectivity.
		 */
		if (root->parse->havingQual)
		{
			/* Factor in the selectivity of the remotely-checked quals */
			retrieved_rows =
				clamp_row_est(numGroups *
							  clauselist_selectivity(root,
													 fpinfo->remote_conds,
													 0,
													 JOIN_INNER,
													 NULL));
			/* Factor in the selectivity of the locally-checked quals */
			rows = clamp_row_est(retrieved_rows * fpinfo->local_conds_sel);
		}
		else
		{
			rows = retrieved_rows = numGroups;
		}

		/* Use width estimate made by the core code. */
		width = foreignrel->reltarget->width;

		/*-----
		 * Startup cost includes:
		 *	  1. Startup cost for underneath input relation, adjusted for
		 *	     tlist replacement by apply_scanjoin_target_to_paths()
		 *	  2. Cost of performing aggregation, per cost_agg()
		 *-----
		 */
		startup_cost = ofpinfo->rel_startup_cost;
		startup_cost += outerrel->reltarget->cost.startup;
		startup_cost += aggcosts.transCost.startup;
		startup_cost += aggcosts.transCost.per_tuple * input_rows;
		startup_cost += aggcosts.finalCost.startup;
		startup_cost += (cpu_operator_cost * numGroupCols) * input_rows;

		/*-----
		 * Run time cost includes:
		 *	  1. Run time cost of underneath input relation, adjusted for
		 *	     tlist replacement by apply_scanjoin_target_to_paths()
		 *	  2. Run time cost of performing aggregation, per cost_agg()
		 *-----
		 */
		run_cost = ofpinfo->rel_total_cost - ofpinfo->rel_startup_cost;
		run_cost += outerrel->reltarget->cost.per_tuple * input_rows;
		run_cost += aggcosts.finalCost.per_tuple * numGroups;
		run_cost += cpu_tuple_cost * numGroups;

		/* Account for the eval cost of HAVING quals, if any */
		if (root->parse->havingQual)
		{
			QualCost remote_cost;

			/* Add in the eval cost of the remotely-checked quals */
			cost_qual_eval(&remote_cost, fpinfo->remote_conds, root);
			startup_cost += remote_cost.startup;
			run_cost += remote_cost.per_tuple * numGroups;
			/* Add in the eval cost of the locally-checked quals */
			startup_cost += fpinfo->local_conds_cost.startup;
			run_cost += fpinfo->local_conds_cost.per_tuple * retrieved_rows;
		}

		/* Add in tlist eval cost for each output row */
		startup_cost += foreignrel->reltarget->cost.startup;
		run_cost += foreignrel->reltarget->cost.per_tuple * rows;
	}
	else
	{
		Cost cpu_per_tuple;

		/* Use rows/width estimates made by set_baserel_size_estimates. */
		rows = foreignrel->rows;
		width = foreignrel->reltarget->width;

		/*
		 * Back into an estimate of the number of retrieved rows.  Just in
		 * case this is nuts, clamp to at most foreignrel->tuples.
		 */
		retrieved_rows = clamp_row_est(rows / fpinfo->local_conds_sel);
		retrieved_rows = Min(retrieved_rows, foreignrel->tuples);

		/*
		 * Cost as though this were a seqscan, which is pessimistic.  We
		 * effectively imagine the local_conds are being evaluated
		 * remotely, too.
		 */
		startup_cost = 0;
		run_cost = 0;
		run_cost += seq_page_cost * foreignrel->pages;

		startup_cost += foreignrel->baserestrictcost.startup;
		cpu_per_tuple = cpu_tuple_cost + foreignrel->baserestrictcost.per_tuple;
		run_cost += cpu_per_tuple * foreignrel->tuples;

		/* Add in tlist eval cost for each output row */
		startup_cost += foreignrel->reltarget->cost.startup;
		run_cost += foreignrel->reltarget->cost.per_tuple * rows;
	}

	/*
	 * Without remote estimates, we have no real way to estimate the cost
	 * of generating sorted output.  It could be free if the query plan
	 * the remote side would have chosen generates properly-sorted output
	 * anyway, but in most cases it will cost something.  Estimate a value
	 * high enough that we won't pick the sorted path when the ordering
	 * isn't locally useful, but low enough that we'll err on the side of
	 * pushing down the ORDER BY clause when it's useful to do so.
	 */
	if (pathkeys != NIL)
	{
		if (IS_UPPER_REL(foreignrel))
		{
			Assert(foreignrel->reloptkind == RELOPT_UPPER_REL &&
				   fpinfo->stage == UPPERREL_GROUP_AGG);
			adjust_foreign_grouping_path_cost(root, pathkeys,
											  retrieved_rows, width,
											  fpextra->limit_tuples,
											  &startup_cost, &run_cost);
		}
		else
		{
			startup_cost *= DEFAULT_FDW_SORT_MULTIPLIER;
			run_cost *= DEFAULT_FDW_SORT_MULTIPLIER;
		}
	}

	total_cost = startup_cost + run_cost;

	/* Adjust the cost estimates if we have LIMIT */
	if (fpextra && fpextra->has_limit)
	{
		adjust_limit_rows_costs(&rows, &startup_cost, &total_cost,
								fpextra->offset_est, fpextra->count_est);
		retrieved_rows = rows;
	}

	/*
	 * If this includes the final sort step, the given target, which will be
	 * applied to the resulting path, might have different expressions from
	 * the foreignrel's reltarget (see make_sort_input_target()); adjust tlist
	 * eval costs.
	 */
	if (fpextra && fpextra->has_final_sort &&
		fpextra->target != foreignrel->reltarget)
	{
		QualCost	oldcost = foreignrel->reltarget->cost;
		QualCost	newcost = fpextra->target->cost;

		startup_cost += newcost.startup - oldcost.startup;
		total_cost += newcost.startup - oldcost.startup;
		total_cost += (newcost.per_tuple - oldcost.per_tuple) * rows;
	}

	/*
	 * Cache the retrieved rows and cost estimates for scans, joins, or
	 * groupings without any parameterization, pathkeys, or additional
	 * post-scan/join-processing steps, before adding the costs for
	 * transferring data from the foreign server.  These estimates are useful
	 * for costing remote joins involving this relation or costing other
	 * remote operations on this relation such as remote sorts and remote
	 * LIMIT restrictions, when the costs can not be obtained from the foreign
	 * server.  This function will be called at least once for every foreign
	 * relation without any parameterization, pathkeys, or additional
	 * post-scan/join-processing steps.
	 */
	if (pathkeys == NIL && param_join_conds == NIL && fpextra == NULL)
	{
		fpinfo->retrieved_rows = retrieved_rows;
		fpinfo->rel_startup_cost = startup_cost;
		fpinfo->rel_total_cost = total_cost;
	}

	/*
	 * Add some additional cost factors to account for connection overhead
	 * (fdw_startup_cost), transferring data across the network
	 * (fdw_tuple_cost per retrieved row), and local manipulation of the data
	 * (cpu_tuple_cost per retrieved row).
	 */
	startup_cost += fpinfo->fdw_startup_cost;
	total_cost += fpinfo->fdw_startup_cost;
	total_cost += fpinfo->fdw_tuple_cost * retrieved_rows;
	total_cost += cpu_tuple_cost * retrieved_rows;

	/*
	 * If we have LIMIT, we should prefer performing the restriction remotely
	 * rather than locally, as the former avoids extra row fetches from the
	 * remote that the latter might cause.  But since the core code doesn't
	 * account for such fetches when estimating the costs of the local
	 * restriction (see create_limit_path()), there would be no difference
	 * between the costs of the local restriction and the costs of the remote
	 * restriction estimated above if we don't use remote estimates (except
	 * for the case where the foreignrel is a grouping relation, the given
	 * pathkeys is not NIL, and the effects of a bounded sort for that rel is
	 * accounted for in costing the remote restriction).  Tweak the costs of
	 * the remote restriction to ensure we'll prefer it if LIMIT is a useful
	 * one.
	 */
	if (fpextra && fpextra->has_limit &&
		fpextra->limit_tuples > 0 &&
		fpextra->limit_tuples < fpinfo->rows)
	{
		Assert(fpinfo->rows > 0);
		total_cost -= (total_cost - startup_cost) * 0.05 *
			(fpinfo->rows - fpextra->limit_tuples) / fpinfo->rows;
	}

	/* Return results. */
	*p_rows = rows;
	*p_width = width;
	*p_startup_cost = startup_cost;
	*p_total_cost = total_cost;
}


/*
 * Adjust the cost estimates of a foreign grouping path to include the cost of
 * generating properly-sorted output.
 */
static void
adjust_foreign_grouping_path_cost(PlannerInfo *root,
								  List *pathkeys,
								  double retrieved_rows,
								  double width,
								  double limit_tuples,
								  Cost *p_startup_cost,
								  Cost *p_run_cost)
{
	/*
	 * If the GROUP BY clause isn't sort-able, the plan chosen by the remote
	 * side is unlikely to generate properly-sorted output, so it would need
	 * an explicit sort; adjust the given costs with cost_sort().  Likewise,
	 * if the GROUP BY clause is sort-able but isn't a superset of the given
	 * pathkeys, adjust the costs with that function.  Otherwise, adjust the
	 * costs by applying the same heuristic as for the scan or join case.
	 */
	if (!grouping_is_sortable(root->parse->groupClause) ||
		!pathkeys_contained_in(pathkeys, root->group_pathkeys))
	{
		Path		sort_path;	/* dummy for result of cost_sort */

		cost_sort(&sort_path,
				  root,
				  pathkeys,
				  *p_startup_cost + *p_run_cost,
				  retrieved_rows,
				  width,
				  0.0,
				  work_mem,
				  limit_tuples);

		*p_startup_cost = sort_path.startup_cost;
		*p_run_cost = sort_path.total_cost - sort_path.startup_cost;
	}
	else
	{
		/*
		 * The default extra cost seems too large for foreign-grouping cases;
		 * add 1/4th of that default.
		 */
		double		sort_multiplier = 1.0 + (DEFAULT_FDW_SORT_MULTIPLIER
											 - 1.0) * 0.25;

		*p_startup_cost *= sort_multiplier;
		*p_run_cost *= sort_multiplier;
	}
}

/*
 * Return true if function name existed in list of function
 */
static bool
exist_in_function_list(char *funcname, const char **funclist)
{
	int			i;

	for (i = 0; funclist[i]; i++)
	{
		if (strcmp(funcname, funclist[i]) == 0)
			return true;
	}
	return false;
}


/*
 * Deparse SELECT statement for given relation into buf.
 *
 * tlist contains the list of desired columns to be fetched from foreign server.
 * For a base relation fpinfo->attrs_used is used to construct SELECT clause,
 * hence the tlist is ignored for a base relation.
 *
 * remote_conds is the list of conditions to be deparsed into the WHERE clause
 * (or, in the case of upper relations, into the HAVING clause).
 *
 * If params_list is not NULL, it receives a list of Params and other-relation
 * Vars used in the clauses; these values must be transmitted to the remote
 * server as parameter values.
 *
 * If params_list is NULL, we're generating the query for EXPLAIN purposes,
 * so Params and other-relation Vars should be replaced by dummy values.
 *
 * pathkeys is the list of pathkeys to order the result by.
 *
 * is_subquery is the flag to indicate whether to deparse the specified
 * relation as a subquery.
 *
 * List of columns selected is returned in retrieved_attrs.
 */
static void
oracleDeparseSelectStmtForRel(StringInfo buf, PlannerInfo *root, RelOptInfo *rel,
						List *tlist, List *remote_conds, bool for_update, List *pathkeys,
						bool has_final_sort, bool has_limit, bool is_subquery,
						List **retrieved_attrs, List **params_list)
{
	struct OracleFdwState *fpinfo = (struct OracleFdwState *)rel->fdw_private;
	List	   *quals;
	ListCell   *cell;
	List	   *columnlist;
	List	   *conditions = rel->baserestrictinfo;
	bool	   in_quote = false;
	int	   index;
	char	   *wherecopy, *p, md5[33], parname[10];
	StringInfoData result;
	deparse_expr_cxt context;

	/*
	 * We handle relations for foreign tables, joins between those and upper
	 * relations.
	 */
	Assert(IS_JOIN_REL(rel) || IS_SIMPLE_REL(rel) || IS_UPPER_REL(rel));

	/* Fill portions of context common to upper, join and base relation */
	context.buf = buf;
	initializeContext(fpinfo, root, rel,
							 IS_UPPER_REL(rel) ? fpinfo->outerrel : rel,
							 &context);

	columnlist = rel->reltarget->exprs;

	/*
	 * oracle fdw warns which column is not existed in the remote table.
	 */
	if (IS_SIMPLE_REL(rel) || IS_UPPER_REL(rel) || IS_JOIN_REL(rel))
	{
		/* find all the columns to include in the select list */

		/* examine each SELECT list entry for Var nodes */
		foreach(cell, columnlist)
		{
			getUsedColumns((Expr *)lfirst(cell), fpinfo->oraTable, rel->relid);
		}

		/* examine each condition for Var nodes */
		foreach(cell, conditions)
		{
			getUsedColumns((Expr *)lfirst(cell), fpinfo->oraTable, rel->relid);
		}
	}

	/* Construct SELECT clause */
	oracleDeparseSelectSql(tlist, is_subquery, retrieved_attrs, &context);

	/*
	 * For upper relations, the WHERE clause is built from the remote
	 * conditions of the underlying scan relation; otherwise, we can use the
	 * supplied list of remote conditions directly.
	 */
	if (IS_UPPER_REL(rel))
	{
		struct OracleFdwState *ofpinfo;

		ofpinfo = (struct OracleFdwState *) fpinfo->outerrel->fdw_private;
		quals = ofpinfo->remote_conds;
	}
	else
		quals = remote_conds;

	/* Construct FROM and WHERE clauses */
	oracleDeparseFromExpr(fpinfo, quals, &context);

	if (IS_UPPER_REL(rel))
	{
		/* Append GROUP BY clause */
		oracleAppendGroupByClause(tlist, &context);

		/* Append HAVING clause */
		if (remote_conds)
		{
			appendStringInfoString(buf, " HAVING ");
			appendConditions(remote_conds, &context);
		}
	}

	/* append ORDER BY clause if all its expressions can be pushed down */
	if (pathkeys)
		oracleAppendOrderByClause(pathkeys, has_final_sort, &context);

	/* append FETCH FIRST n ROWS ONLY if the LIMIT can be pushed down */
	if (has_limit && fpinfo->limit_clause)
		appendStringInfo(buf, " %s", fpinfo->limit_clause);

	/* append FOR UPDATE if if the scan is for a modification */
	if (for_update)
		appendStringInfo(buf, " FOR UPDATE");

	/* get a copy of the where clause without single quoted string literals */
	wherecopy = pstrdup(buf->data);
	for (p=wherecopy; *p!='\0'; ++p)
	{
		if (*p == '\'')
			in_quote = ! in_quote;
		if (in_quote)
			*p = ' ';
	}

	/* remove all parameters that do not actually occur in the query */
	index = 0;
	foreach(cell, fpinfo->params)
	{
		++index;
		snprintf(parname, 10, ":p%d", index);
		if (strstr(wherecopy, parname) == NULL)
		{
			/* set the element to NULL to indicate it's gone */
			lfirst(cell) = NULL;
		}
	}

	pfree(wherecopy);

	/*
	 * Calculate MD5 hash of the query string so far.
	 * This is needed to find the query in Oracle's library cache for EXPLAIN.
	 */
	if (! pg_md5_hash(buf->data, strlen(buf->data), md5))
	{
		ereport(ERROR,
				(errcode(ERRCODE_OUT_OF_MEMORY),
				errmsg("out of memory")));
	}

	/* add comment with MD5 hash to query */
	initStringInfo(&result);
	appendStringInfo(&result, "SELECT /*%s*/ %s", md5, buf->data);
	buf->data = pstrdup(result.data);
	pfree(result.data);
}

/*
 * Construct a simple SELECT statement that retrieves desired columns
 * of the specified foreign table, and append it to "buf".  The output
 * contains just "SELECT ... ".
 *
 * We also create an integer List of the columns being retrieved, which is
 * returned to *retrieved_attrs, unless we deparse the specified relation
 * as a subquery.
 *
 * tlist is the list of desired columns.  is_subquery is the flag to
 * indicate whether to deparse the specified relation as a subquery.
 * Read prologue of deparseSelectStmtForRel() for details.
 */
static void
oracleDeparseSelectSql(List *tlist, bool is_subquery, List **retrieved_attrs,
					   deparse_expr_cxt *context)
{
	StringInfo	buf = context->buf;
	RelOptInfo *foreignrel = context->foreignrel;
	struct OracleFdwState *fdwState = (struct OracleFdwState *) foreignrel->fdw_private;

	if (is_subquery)
	{
		/*
		 * For a relation that is deparsed as a subquery, emit expressions
		 * specified in the relation's reltarget.  Note that since this is for
		 * the subquery, no need to care about *retrieved_attrs.
		 */
		oracleDeparseSubqueryTargetList(context);
	}
	else if (IS_JOIN_REL(foreignrel) || IS_UPPER_REL(foreignrel))
	{
		/*
		 * For a join or upper relation the input tlist gives the list of
		 * columns required to be fetched from the foreign server.
		 */
		oracleDeparseExplicitTargetList(tlist, false, retrieved_attrs, context);
	}
	else
	{
		/*
		 * For a base relation fpinfo->attrs_used gives the list of columns
		 * required to be fetched from the foreign server.
		 */
		RangeTblEntry *rte = planner_rt_fetch(foreignrel->relid, context->root);

		/*
		 * Core code already has some lock on each rel being planned, so we
		 * can use NoLock here.
		 */
		Relation	rel = table_open(rte->relid, NoLock);

		if (tlist != NULL)
		{
			oracleDeparseExplicitTargetList(tlist, false, retrieved_attrs, context);
		}
		else
		{
			oracleDeparseTargetList(context->oraTable, buf, rte, foreignrel->relid, rel, false,
									fdwState->attrs_used, false, retrieved_attrs);
		}
		table_close(rel, NoLock);
	}
}


/*
 * Emit expressions specified in the given relation's reltarget.
 *
 * This is used for deparsing the given relation as a subquery.
 */
static void
oracleDeparseSubqueryTargetList(deparse_expr_cxt *context)
{
	StringInfo	buf = context->buf;
	RelOptInfo *foreignrel = context->foreignrel;
	bool		first;
	ListCell   *lc;

	/* Should only be called in these cases. */
	Assert(IS_SIMPLE_REL(foreignrel) || IS_JOIN_REL(foreignrel));

	first = true;
	foreach(lc, foreignrel->reltarget->exprs)
	{
		Node	   *node = (Node *) lfirst(lc);
		char	*result = NULL;

		if (!first)
			appendStringInfoString(buf, ", ");
		first = false;

		result = deparseExpr((Expr *) node, context);
		appendStringInfoString(buf, result);
	}

	/* Don't generate bad syntax if no expressions */
	if (first)
		appendStringInfoString(buf, "NULL");
}


/*
 * Deparse given targetlist and append it to context->buf.
 *
 * tlist is list of TargetEntry's which in turn contain Var nodes.
 *
 * retrieved_attrs is the list of continuously increasing integers starting
 * from 1. It has same number of entries as tlist.
 *
 * This is used for both SELECT and RETURNING targetlists; the is_returning
 * parameter is true only for a RETURNING targetlist.
 */
static void
oracleDeparseExplicitTargetList(List *tlist,
						  bool is_returning,
						  List **retrieved_attrs,
						  deparse_expr_cxt *context)
{
	ListCell   *lc;
	StringInfo	buf = context->buf;
	int			i = 0;

	*retrieved_attrs = NIL;

	foreach(lc, tlist)
	{
		if (!is_returning)
		{
			TargetEntry *tle = lfirst_node(TargetEntry, lc);
			char	*result = NULL;

			if (i > 0)
				appendStringInfoString(buf, ", ");

			result = deparseExpr((Expr *) tle->expr, context);
			appendStringInfoString(buf, result);
		}

		*retrieved_attrs = lappend_int(*retrieved_attrs, i + 1);
		i++;
	}

	if (i == 0 && !is_returning)
		appendStringInfoString(buf, "NULL");
}



/*
 * Add a RETURNING clause, if needed, to an INSERT/UPDATE/DELETE.
 */
static void
oracleDeparseReturningList(struct oraTable *oraTable,
					 StringInfo buf, RangeTblEntry *rte,
					 Index rtindex, Relation rel,
					 bool trig_after_row,
					 List *withCheckOptionList,
					 List *returningList,
					 List **retrieved_attrs)
{
	Bitmapset  *attrs_used = NULL;

	if (trig_after_row)
	{
		/* whole-row reference acquires all non-system columns */
		attrs_used =
			bms_make_singleton(0 - FirstLowInvalidHeapAttributeNumber);
	}

	if (withCheckOptionList != NIL)
	{
		/*
		 * We need the attrs, non-system and system, mentioned in the local
		 * query's WITH CHECK OPTION list.
		 *
		 * Note: we do this to ensure that WCO constraints will be evaluated
		 * on the data actually inserted/updated on the remote side, which
		 * might differ from the data supplied by the core code, for example
		 * as a result of remote triggers.
		 */
		pull_varattnos((Node *) withCheckOptionList, rtindex,
					   &attrs_used);
	}

	if (returningList != NIL)
	{
		/*
		 * We need the attrs, non-system and system, mentioned in the local
		 * query's RETURNING list.
		 */
		pull_varattnos((Node *) returningList, rtindex,
					   &attrs_used);
	}

	if (attrs_used != NULL)
		oracleDeparseTargetList(oraTable, buf, rte, rtindex, rel, true, attrs_used, false,
						  retrieved_attrs);
	else
		*retrieved_attrs = NIL;
}


/*
 * Emit a target list that retrieves the columns specified in attrs_used.
 * This is used for both SELECT.
 *
 * The tlist text is appended to buf, and we also create an integer List
 * of the columns being retrieved, which is returned to *retrieved_attrs.
 *
 * If qualify_col is true, add relation alias before the column name.
 */
static void
oracleDeparseTargetList(struct oraTable *oraTable, StringInfo buf,
				  RangeTblEntry *rte,
				  Index rtindex,
				  Relation rel,
				  bool is_returning,
				  Bitmapset *attrs_used,
				  bool qualify_col,
				  List **retrieved_attrs)
{
	TupleDesc	tupdesc = RelationGetDescr(rel);
	bool		have_wholerow;
	bool		first;
	int			i;
	int			index = 0; /* col index of oraTable */

	*retrieved_attrs = NIL;

	/* If there's a whole-row reference, we'll need all the columns. */
	have_wholerow = bms_is_member(0 - FirstLowInvalidHeapAttributeNumber,
								  attrs_used);

	first = true;
	index = 0;
	for (i = 1; i <= tupdesc->natts; i++)
	{
		Form_pg_attribute attr = TupleDescAttr(tupdesc, i - 1);

		/* Ignore dropped attributes. */
		if (attr->attisdropped)
			continue;

		/*
		 * save index of attribute in the remote table which has
		 * the same name as attribute in the foreign table.
		 */
		for (index=0; index < oraTable->ncols; ++index)
		{
			if (strcmp(oraTable->cols[index]->pgname, NameStr(attr->attname)) == 0)
				break;
		}

		/* ignore attribute not existed in the remote table */
		if (index >= oraTable->ncols)
			continue;

		/* ignore attribute not used */
		if (oraTable->cols[index]->used == 0)
			continue;

		/* deparse attribute in the target list only */
		if (have_wholerow ||
			bms_is_member(i - FirstLowInvalidHeapAttributeNumber,
						  attrs_used))
		{
			if (!is_returning)
			{
				if (!first)
					appendStringInfoString(buf, ", ");

				first = false;
				oracleDeparseColumnRef(oraTable, buf, rtindex, index, qualify_col);
			}

			*retrieved_attrs = lappend_int(*retrieved_attrs, i);
		}
	}

	/* Don't generate bad syntax if no undropped columns */
	if (first && !is_returning)
		appendStringInfoString(buf, "NULL");
}


/*
 * Construct name to use for given column, and emit it into buf.
 * If it has a column_name FDW option, use that instead of attribute name.
 *
 * If qualify_col is true, qualify column name with the alias of relation.
 */
static void
oracleDeparseColumnRef(struct oraTable *oraTable, StringInfo buf, int varno, int varattno, bool qualify_col)
{
	StringInfoData alias;
	
	initStringInfo(&alias);

	/* varno must not be any of OUTER_VAR, INNER_VAR and INDEX_VAR. */
	Assert(!IS_SPECIAL_VARNO(varno));

	if (qualify_col)
		ADD_REL_QUALIFIER(buf, varno);

	if (oraTable->cols[varattno]->oratype == ORA_TYPE_XMLTYPE)
		appendStringInfo(buf, "(%s%s).getclobval()", alias.data, oraTable->cols[varattno]->name);
	else
		appendStringInfo(buf, "%s%s", alias.data, oraTable->cols[varattno]->name);
}

/*
 * Construct a FROM clause and, if needed, a WHERE clause, and append those to
 * "buf".
 *
 * quals is the list of clauses to be included in the WHERE clause.
 * (These may or may not include RestrictInfo decoration.)
 */
static void
oracleDeparseFromExpr(struct OracleFdwState *fdwState, List *quals, deparse_expr_cxt *context)
{
	StringInfo	buf = context->buf;
	RelOptInfo *scanrel = context->scanrel;
	List	**params_list = context->params_list;

	/* For upper relations, scanrel must be either a joinrel or a baserel */
	Assert(!IS_UPPER_REL(context->foreignrel) ||
		   IS_JOIN_REL(scanrel) || IS_SIMPLE_REL(scanrel));

	/* Construct FROM clause */
	appendStringInfoString(buf, " FROM ");

	context->use_alias = (bms_membership(scanrel->relids) == BMS_MULTIPLE);
	context->ignore_rel = (Index) 0;
	context->ignore_conds = NULL;
	deparseFromExprForRel(buf, scanrel, params_list, context);

	/* Construct WHERE clause */
	if (quals != NIL)
	{
		appendStringInfoString(buf, " WHERE ");
		appendConditions(quals, context);
	}
}


/*
 * Append FROM clause entry for the given relation into buf.
 */
static void
oracleDeparseRangeTblRef(StringInfo buf, RelOptInfo *foreignrel,
				   bool make_subquery, List **params_list, deparse_expr_cxt *context)
{
	struct OracleFdwState *fpinfo = (struct OracleFdwState *) foreignrel->fdw_private;
	PlannerInfo *root = context->root;
	Index ignore_rel = context->ignore_rel;

	/* Should only be called in these cases. */
	Assert(IS_SIMPLE_REL(foreignrel) || IS_JOIN_REL(foreignrel));

	/* If make_subquery is true, deparse the relation as a subquery. */
	if (make_subquery)
	{
		List	   *retrieved_attrs;
		int			ncols;

		/*
		 * The given relation shouldn't contain the target relation, because
		 * this should only happen for input relations for a full join, and
		 * such relations can never contain an UPDATE/DELETE target.
		 */
		Assert(ignore_rel == 0 ||
			   !bms_is_member(ignore_rel, foreignrel->relids));

		/* Deparse the subquery representing the relation. */
		appendStringInfoChar(buf, '(');
		oracleDeparseSelectStmtForRel(buf, root, foreignrel, NIL,
								fpinfo->remote_conds, false, NIL,
								false, false, true,
								&retrieved_attrs, params_list);
		appendStringInfoChar(buf, ')');

		/* Append the relation alias. */
		appendStringInfo(buf, " %s%d", SUBQUERY_REL_ALIAS_PREFIX,
						 fpinfo->relation_index);

		/*
		 * Append the column aliases if needed.  Note that the subquery emits
		 * expressions specified in the relation's reltarget (see
		 * deparseSubqueryTargetList).
		 */
		ncols = list_length(foreignrel->reltarget->exprs);
		if (ncols > 0)
		{
			int			i;

			appendStringInfoChar(buf, '(');
			for (i = 1; i <= ncols; i++)
			{
				if (i > 1)
					appendStringInfoString(buf, ", ");

				appendStringInfo(buf, "%s%d", SUBQUERY_COL_ALIAS_PREFIX, i);
			}
			appendStringInfoChar(buf, ')');
		}
	}
	else
		deparseFromExprForRel(buf, foreignrel, params_list, context);
}

/*
 * Deparse GROUP BY clause.
 */
static void
oracleAppendGroupByClause(List *tlist, deparse_expr_cxt *context)
{
	StringInfo	buf = context->buf;
	Query	   *query = context->root->parse;
	ListCell   *lc;
	bool		first = true;

	/* Nothing to be done, if there's no GROUP BY clause in the query. */
	if (!query->groupClause)
		return;

	appendStringInfoString(buf, " GROUP BY ");

	/*
	 * Queries with grouping sets are not pushed down, so we don't expect
	 * grouping sets here.
	 */
	Assert(!query->groupingSets);

	foreach(lc, query->groupClause)
	{
		SortGroupClause *grp = (SortGroupClause *) lfirst(lc);

		if (!first)
			appendStringInfoString(buf, ", ");
		first = false;

		oracleDeparseSortGroupClause(grp->tleSortGroupRef, tlist, context);
	}
}

/*
 * Appends a sort or group clause.
 *
 * Like get_rule_sortgroupclause(), returns the expression tree, so caller
 * need not find it again.
 */
static Node *
oracleDeparseSortGroupClause(Index ref, List *tlist,
					   deparse_expr_cxt *context)
{
	StringInfo	buf = context->buf;
	TargetEntry *tle;
	Expr	   *expr;
	char *result = NULL;


	tle = get_sortgroupref_tle(ref, tlist);
	expr = tle->expr;

	if (!expr || IsA(expr, Var) || IsA(expr, Const))
	{
		result = deparseExpr(expr, context);
		appendStringInfoString(buf, result);
	}
	else
	{
		/* Always parenthesize the expression. */
		appendStringInfoChar(buf, '(');
		result = deparseExpr(expr, context);
		appendStringInfoString(buf, result);
		appendStringInfoChar(buf, ')');
	}

	return (Node *) expr;
}

/*
 * Force assorted GUC parameters to settings that ensure that we'll output
 * data values in a form that is unambiguous to the remote server.
 *
 * This is rather expensive and annoying to do once per row, but there's
 * little choice if we want to be sure values are transmitted accurately;
 * we can't leave the settings in place between rows for fear of affecting
 * user-visible computations.
 *
 * We use the equivalent of a function SET option to allow the settings to
 * persist only until the caller calls reset_transmission_modes().  If an
 * error is thrown in between, guc.c will take care of undoing the settings.
 *
 * The return value is the nestlevel that must be passed to
 * reset_transmission_modes() to undo things.
 */
static int
set_transmission_modes(void)
{
	int			nestlevel = NewGUCNestLevel();

	/*
	 * The values set here should match what pg_dump does.  See also
	 * configure_remote_session in connection.c.
	 */
	if (DateStyle != USE_ISO_DATES)
		(void) set_config_option("datestyle", "ISO",
								 PGC_USERSET, PGC_S_SESSION,
								 GUC_ACTION_SAVE, true, 0, false);
	if (IntervalStyle != INTSTYLE_POSTGRES)
		(void) set_config_option("intervalstyle", "postgres",
								 PGC_USERSET, PGC_S_SESSION,
								 GUC_ACTION_SAVE, true, 0, false);
	if (extra_float_digits < 3)
		(void) set_config_option("extra_float_digits", "3",
								 PGC_USERSET, PGC_S_SESSION,
								 GUC_ACTION_SAVE, true, 0, false);

	return nestlevel;
}

/*
 * Undo the effects of set_transmission_modes().
 */
static void
reset_transmission_modes(int nestlevel)
{
	AtEOXact_GUC(true, nestlevel);
}


/*
 * Deparse an Aggref node.
 */
static char *
oracleDeparseAggref(Aggref *node, deparse_expr_cxt *context)
{
	StringInfoData	buf;
	const char *proname;

	/* Only basic, non-split aggregation accepted. */
	Assert(node->aggsplit == AGGSPLIT_SIMPLE);

	/* oracle does not support aggregate with FILTER clause */
	if (node->aggfilter != NULL)
		return NULL;

	/* oracle does not support VARIADIC */
	if (node->aggvariadic)
		return NULL;

	initStringInfo(&buf);

	/* Get function name */
	proname = get_func_name(node->aggfnoid);
	appendStringInfoString(&buf, quote_identifier(proname));

	if (!exist_in_function_list(buf.data, OracleSupportedBuiltinAggFunction) &&
		!exist_in_function_list(buf.data, OracleUniqueAggFunction))
		return NULL;

	appendStringInfoChar(&buf, '(');

	/* Add DISTINCT */
	appendStringInfoString(&buf, (node->aggdistinct != NIL) ? "DISTINCT " : "");

	/*
	 * Add WITHIN GROUP (ORDER BY ..)
	 */
	if (AGGKIND_IS_ORDERED_SET(node->aggkind))
	{
		ListCell   *arg;
		bool		first = true;
		char *result;

		Assert(!node->aggvariadic);
		Assert(node->aggorder != NIL);

		foreach(arg, node->aggdirectargs)
		{
			if (!first)
				appendStringInfoString(&buf, ", ");
			first = false;

			result = deparseExpr((Expr *) lfirst(arg), context);
			if (result == NULL)
			{
				pfree(buf.data);
				return NULL;
			}

			appendStringInfoString(&buf, result);
		}

		appendStringInfoString(&buf, ") WITHIN GROUP (ORDER BY ");
		result = oracleAppendAggOrderBy(node->aggorder, node->args, context);

		if (result == NULL)
		{
			pfree(buf.data);
			return NULL;
		}

		appendStringInfoString(&buf, result);
	}
	else
	{
		/* aggstar can be set only in zero-argument aggregates */
		if (node->aggstar)
		{
			appendStringInfoChar(&buf, '*');
		}
		else
		{
			ListCell *arg;
			bool first = true;

			/* Add all the arguments */
			foreach (arg, node->args)
			{
				TargetEntry *tle = (TargetEntry *)lfirst(arg);
				Node *n = (Node *)tle->expr;
				char *result;

				if (tle->resjunk)
					continue;

				if (!first)
					appendStringInfoString(&buf, ", ");
				first = false;

				result = deparseExpr((Expr *)n, context);
				if (result == NULL)
				{
					pfree(buf.data);
					return NULL;
				}

				appendStringInfoString(&buf, result);
			}
		}
	}

	appendStringInfoChar(&buf, ')');
	return buf.data;
}

/*
 * Append ORDER BY within aggregate function.
 */
static char *
oracleAppendAggOrderBy(List *orderList, List *targetList, deparse_expr_cxt *context)
{
	StringInfoData	buf;
	ListCell   *lc;
	bool		first = true;
	StringInfo	saved_context_buf = context->buf;

	initStringInfo(&buf);

	foreach(lc, orderList)
	{
		SortGroupClause *srt = (SortGroupClause *) lfirst(lc);
		Node	   *sortexpr;
		Oid			sortcoltype;
		TypeCacheEntry *typentry;

		if (!first)
			appendStringInfoString(&buf, ", ");
		first = false;

		context->buf = &buf;
		sortexpr = oracleDeparseSortGroupClause(srt->tleSortGroupRef, targetList, context);
		context->buf = saved_context_buf;
		sortcoltype = exprType(sortexpr);
		/* See whether operator is default < or > for datatype */
		typentry = lookup_type_cache(sortcoltype,
									 TYPECACHE_LT_OPR | TYPECACHE_GT_OPR);
		if (srt->sortop == typentry->lt_opr)
			appendStringInfoString(&buf, " ASC");
		else if (srt->sortop == typentry->gt_opr)
			appendStringInfoString(&buf, " DESC");

		if (srt->nulls_first)
			appendStringInfoString(&buf, " NULLS FIRST");
		else
			appendStringInfoString(&buf, " NULLS LAST");
	}

	return buf.data;
}

/*
 * Construct a tuple descriptor for the scan tuples handled by a foreign join.
 */
static TupleDesc
get_tupdesc_for_join_scan_tuples(ForeignScanState *node)
{
	ForeignScan *fsplan = (ForeignScan *) node->ss.ps.plan;
	EState	   *estate = node->ss.ps.state;
	TupleDesc	tupdesc;
	int	i;

	/*
	 * The core code has already set up a scan tuple slot based on
	 * fsplan->fdw_scan_tlist, and this slot's tupdesc is mostly good enough,
	 * but there's one case where it isn't.  If we have any whole-row row
	 * identifier Vars, they may have vartype RECORD, and we need to replace
	 * that with the associated table's actual composite type.  This ensures
	 * that when we read those ROW() expression values from the remote server,
	 * we can convert them to a composite type the local server knows.
	 */
	tupdesc = CreateTupleDescCopy(node->ss.ss_ScanTupleSlot->tts_tupleDescriptor);
	for (i = 0; i < tupdesc->natts; i++)
	{
		Form_pg_attribute att = TupleDescAttr(tupdesc, i);
		Var		   *var;
		RangeTblEntry *rte;
		Oid			reltype;

		/* Nothing to do if it's not a generic RECORD attribute */
		if (att->atttypid != RECORDOID || att->atttypmod >= 0)
			continue;

		/*
		 * If we can't identify the referenced table, do nothing.  This'll
		 * likely lead to failure later, but perhaps we can muddle through.
		 */
		var = (Var *) list_nth_node(TargetEntry, fsplan->fdw_scan_tlist,
									i)->expr;
		if (!IsA(var, Var) || var->varattno != 0)
			continue;
		rte = list_nth(estate->es_range_table, var->varno - 1);
		if (rte->rtekind != RTE_RELATION)
			continue;
		reltype = get_rel_type_id(rte->relid);
		if (!OidIsValid(reltype))
			continue;
		att->atttypid = reltype;
		/* shouldn't need to change anything else */
	}
	return tupdesc;
}

static void
add_paths_with_pathkeys_for_rel(PlannerInfo *root, RelOptInfo *rel,
								Path *epq_path)
{
	List	   *useful_pathkeys_list = NIL; /* List of all pathkeys */
	ListCell   *lc;

	useful_pathkeys_list = get_useful_pathkeys_for_relation(root, rel);

	/* Create one path for each set of pathkeys we found above. */
	foreach(lc, useful_pathkeys_list)
	{
		double		rows;
		int			width;
		Cost		startup_cost;
		Cost		total_cost;
		List	   *useful_pathkeys = lfirst(lc);
		Path	   *sorted_epq_path;

		estimate_path_cost_size(root, rel, NIL, useful_pathkeys, NULL,
								&rows, &width, &startup_cost, &total_cost);

		/*
		 * The EPQ path must be at least as well sorted as the path itself, in
		 * case it gets used as input to a mergejoin.
		 */
		sorted_epq_path = epq_path;
		if (sorted_epq_path != NULL &&
			!pathkeys_contained_in(useful_pathkeys,
								   sorted_epq_path->pathkeys))
			sorted_epq_path = (Path *)
				create_sort_path(root,
								 rel,
								 sorted_epq_path,
								 useful_pathkeys,
								 -1.0);

		if (IS_SIMPLE_REL(rel))
			add_path(rel, (Path *)
					 create_foreignscan_path(root, rel,
											 NULL,
											 rows,
											 startup_cost,
											 total_cost,
											 useful_pathkeys,
											 rel->lateral_relids,
											 sorted_epq_path,
											 NIL));
		else
			add_path(rel, (Path *)
					 create_foreign_join_path(root, rel,
											  NULL,
											  rows,
											  startup_cost,
											  total_cost,
											  useful_pathkeys,
											  rel->lateral_relids,
											  sorted_epq_path,
											  NIL));
	}
}

/*
 * get_useful_pathkeys_for_relation
 *		Determine which orderings of a relation might be useful.
 *
 * Getting data in sorted order can be useful either because the requested
 * order matches the final output ordering for the overall query we're
 * planning, or because it enables an efficient merge join.  Here, we try
 * to figure out which pathkeys to consider.
 */
static List *
get_useful_pathkeys_for_relation(PlannerInfo *root, RelOptInfo *rel)
{
	List	   *useful_pathkeys_list = NIL;
	struct OracleFdwState *fpinfo = (struct OracleFdwState *) rel->fdw_private;
	ListCell   *lc;
	deparse_expr_cxt context;

	/* Initialize context */
	initializeContext(fpinfo, root, rel, rel, &context);

	/*
	 * Pushing the query_pathkeys to the remote server is always worth
	 * considering, because it might let us avoid a local sort.
	 */
	fpinfo->qp_is_pushdown_safe = false;
	if (root->query_pathkeys)
	{
		bool		query_pathkeys_ok = true;

		foreach(lc, root->query_pathkeys)
		{
			PathKey    *pathkey = (PathKey *) lfirst(lc);
			EquivalenceClass *pathkey_ec = pathkey->pk_eclass;
			Expr	   *em_expr = NULL;
			Oid em_type;
			bool can_pushdown;

			/*
			 * The planner and executor don't have any clever strategy for
			 * taking data sorted by a prefix of the query's pathkeys and
			 * getting it to be sorted by all of those pathkeys. We'll just
			 * end up resorting the entire data set.  So, unless we can push
			 * down all of the query pathkeys, forget it.
			 *
			 * deparseExpr would detect volatile expressions as well, but
			 * checking ec_has_volatile here saves some cycles.
			 */
			can_pushdown = !pathkey_ec->ec_has_volatile
					&& ((em_expr = find_em_expr_for_rel(pathkey_ec, rel)) != NULL);

			if (can_pushdown)
			{
				em_type = exprType((Node *)em_expr);

				/* expressions of a type different from this are not safe to push down into ORDER BY clauses */
				if (em_type != INT8OID && em_type != INT2OID && em_type != INT4OID && em_type != OIDOID
						&& em_type != FLOAT4OID && em_type != FLOAT8OID && em_type != NUMERICOID && em_type != DATEOID
						&& em_type != TIMESTAMPOID && em_type != TIMESTAMPTZOID && em_type != INTERVALOID)
					can_pushdown = false;
			}

			if (!can_pushdown ||
				!deparseExpr(em_expr, &context))
			{
				query_pathkeys_ok = false;
				break;
			}
		}

		if (query_pathkeys_ok)
		{
			useful_pathkeys_list = list_make1(list_copy(root->query_pathkeys));
			fpinfo->qp_is_pushdown_safe = true;
		}
	}

	return useful_pathkeys_list;
}

/*
 * Deparse ORDER BY clause according to the given pathkeys for given base
 * relation. From given pathkeys expressions belonging entirely to the given
 * base relation are obtained and deparsed.
 */
static void
oracleAppendOrderByClause(List *pathkeys, bool has_final_sort,
					deparse_expr_cxt *context)
{
	ListCell   *lcell;
	int			nestlevel;
	char	   *delim = " ";
	RelOptInfo *baserel = context->scanrel;
	StringInfo	buf = context->buf;

	/* Make sure any constants in the exprs are printed portably */
	nestlevel = set_transmission_modes();

	appendStringInfoString(buf, " ORDER BY");
	foreach(lcell, pathkeys)
	{
		PathKey    *pathkey = lfirst(lcell);
		Expr	   *em_expr;
		char	   *sort_clause;

		if (has_final_sort)
		{
			/*
			 * By construction, context->foreignrel is the input relation to
			 * the final sort.
			 */
			em_expr = find_em_expr_for_input_target(context->root,
													pathkey->pk_eclass,
													context->foreignrel->reltarget);
		}
		else
			em_expr = find_em_expr_for_rel(pathkey->pk_eclass, baserel);

		Assert(em_expr != NULL);

		appendStringInfoString(buf, delim);
		sort_clause = deparseExpr(em_expr, context);
		appendStringInfoString(buf, sort_clause);

		if (pathkey->pk_strategy == BTLessStrategyNumber)
			appendStringInfoString(buf, " ASC");
		else
			appendStringInfoString(buf, " DESC");

		if (pathkey->pk_nulls_first)
			appendStringInfoString(buf, " NULLS FIRST");
		else
			appendStringInfoString(buf, " NULLS LAST");

		delim = ", ";
	}

	reset_transmission_modes(nestlevel);
}

/*
 * oracleCreateQuery
 * 		Return query.
 */
char
*oracleCreateQuery(char *tablename)
{
	char *query;
	int length;

	/* construct a "SELECT * FROM ..." query to describe columns */
	length = strlen(tablename) + 14;
	query = oracleAlloc(length + 1);
	strcpy(query, "SELECT * FROM ");
	strcat(query, tablename);

	return query;
}

/*
 *	getOraTableFromJoinRel
 *		Get oraTable from JOIN relation.
 */
static struct oraTable
*getOraTableFromJoinRel(Var *variable, RelOptInfo *foreignrel)
{
	struct oraTable *var_table = NULL;
	struct OracleFdwState *joinstate, *outerstate, *innerstate;

	Assert(IS_JOIN_REL(foreignrel));

	joinstate = (struct OracleFdwState *)foreignrel->fdw_private;
	outerstate = (struct OracleFdwState *)joinstate->outerrel->fdw_private;
	innerstate = (struct OracleFdwState *)joinstate->innerrel->fdw_private;

	/* we can't get here if the foreign table has no columns, so this is safe */
	if (variable->varno == outerstate->oraTable->cols[0]->varno && variable->varlevelsup == 0)
		var_table = outerstate->oraTable;
	if (variable->varno == innerstate->oraTable->cols[0]->varno && variable->varlevelsup == 0)
		var_table = innerstate->oraTable;
	
	return var_table;
}

/*
 * This is possible that the name of function in PostgreSQL and oracle differ,
 * so return the oracle equivalent function name.
 */
static char *
oracle_replace_function(char *in)
{
	bool		has_oracle_prefix = false;

	if (strcmp(in, "ceiling") == 0)
		return "ceil";
	
	if (strcmp(in, "char_length") == 0
		|| strcmp(in, "character_length") == 0)
	{
		return "length";
	}
	
	if (strcmp(in, "pow") == 0)
		return "power";

	if (strcmp(in, "octet_length") == 0)
		return "lengthb";

	if (strcmp(in, "position") == 0
		|| strcmp(in, "strpos") == 0)
	{
		return "instr";
	}

	if (strcmp(in, "substring") == 0)
		return "substr";
				
	has_oracle_prefix = starts_with("oracle_", in);

	if (has_oracle_prefix == true &&
		(strcmp(in, "oracle_current_date") == 0 ||
		strcmp(in, "oracle_current_timestamp") == 0 ||
		strcmp(in, "oracle_localtimestamp") == 0 ||
		strcmp(in, "oracle_extract") == 0 ||
		strcmp(in, "oracle_round") == 0))
	{
		in += strlen("oracle_");
	}

	return in;
}

/*
 * Return true if the string (*str) have prefix (*pre)
 */
static bool
starts_with(const char *pre, const char *str)
{
	size_t		lenpre = strlen(pre);
	size_t		lenstr = strlen(str);

	return lenstr < lenpre ? false : strncmp(pre, str, lenpre) == 0;
}

#if PG_VERSION_NUM >= 140000
/*
 * find_modifytable_subplan
 *		Helper routine for oraclePlanDirectModify to find the
 *		ModifyTable subplan node that scans the specified RTI.
 *
 * Returns NULL if the subplan couldn't be identified.  That's not a fatal
 * error condition, we just abandon trying to do the update directly.
 */
static ForeignScan *
find_modifytable_subplan(PlannerInfo *root,
						 ModifyTable *plan,
						 Index rtindex,
						 int subplan_index)
{
	Plan	   *subplan = outerPlan(plan);

	/*
	 * The cases we support are (1) the desired ForeignScan is the immediate
	 * child of ModifyTable, or (2) it is the subplan_index'th child of an
	 * Append node that is the immediate child of ModifyTable.  There is no
	 * point in looking further down, as that would mean that local joins are
	 * involved, so we can't do the update directly.
	 *
	 * There could be a Result atop the Append too, acting to compute the
	 * UPDATE targetlist values.  We ignore that here; the tlist will be
	 * checked by our caller.
	 *
	 * In principle we could examine all the children of the Append, but it's
	 * currently unlikely that the core planner would generate such a plan
	 * with the children out-of-order.  Moreover, such a search risks costing
	 * O(N^2) time when there are a lot of children.
	 */
	if (IsA(subplan, Append))
	{
		Append	   *appendplan = (Append *) subplan;

		if (subplan_index < list_length(appendplan->appendplans))
			subplan = (Plan *) list_nth(appendplan->appendplans, subplan_index);
	}
	else if (IsA(subplan, Result) &&
			 outerPlan(subplan) != NULL &&
			 IsA(outerPlan(subplan), Append))
	{
		Append	   *appendplan = (Append *) outerPlan(subplan);

		if (subplan_index < list_length(appendplan->appendplans))
			subplan = (Plan *) list_nth(appendplan->appendplans, subplan_index);
	}

	/* Now, have we got a ForeignScan on the desired rel? */
	if (IsA(subplan, ForeignScan))
	{
		ForeignScan *fscan = (ForeignScan *) subplan;

		if (bms_is_member(rtindex, fscan->fs_relids))
			return fscan;
	}

	return NULL;
}
#endif

/*
 * build_remote_returning
 *		Build a RETURNING targetlist of a remote query for performing an
 *		UPDATE/DELETE .. RETURNING on a join directly
 */
static List *
build_remote_returning(Index rtindex, Relation rel, List *returningList)
{
	bool		have_wholerow = false;
	List	   *tlist = NIL;
	List	   *vars;
	ListCell   *lc;

	Assert(returningList);

	vars = pull_var_clause((Node *) returningList, PVC_INCLUDE_PLACEHOLDERS);

	/*
	 * If there's a whole-row reference to the target relation, then we'll
	 * need all the columns of the relation.
	 */
	foreach(lc, vars)
	{
		Var		   *var = (Var *) lfirst(lc);

		if (IsA(var, Var) &&
			var->varno == rtindex &&
			var->varattno == InvalidAttrNumber)
		{
			have_wholerow = true;
			break;
		}
	}

	if (have_wholerow)
	{
		TupleDesc	tupdesc = RelationGetDescr(rel);
		int			i;

		for (i = 1; i <= tupdesc->natts; i++)
		{
			Form_pg_attribute attr = TupleDescAttr(tupdesc, i - 1);
			Var		   *var;

			/* Ignore dropped attributes. */
			if (attr->attisdropped)
				continue;

			var = makeVar(rtindex,
						  i,
						  attr->atttypid,
						  attr->atttypmod,
						  attr->attcollation,
						  0);

			tlist = lappend(tlist,
							makeTargetEntry((Expr *) var,
											list_length(tlist) + 1,
											NULL,
											false));
		}
	}

	/* Now add any remaining columns to tlist. */
	foreach(lc, vars)
	{
		Var		   *var = (Var *) lfirst(lc);

		/*
		 * No need for whole-row references to the target relation.  We don't
		 * need system columns other than ctid and oid either, since those are
		 * set locally.
		 */
		if (IsA(var, Var) &&
			var->varno == rtindex &&
			var->varattno <= InvalidAttrNumber &&
			var->varattno != SelfItemPointerAttributeNumber)
			continue;			/* don't need it */

		if (tlist_member((Expr *) var, tlist))
			continue;			/* already got it */

		tlist = lappend(tlist,
						makeTargetEntry((Expr *) var,
										list_length(tlist) + 1,
										NULL,
										false));
	}

	list_free(vars);

	return tlist;
}

/*
 * rebuild_fdw_scan_tlist
 *		Build new fdw_scan_tlist of given foreign-scan plan node from given
 *		tlist
 *
 * There might be columns that the fdw_scan_tlist of the given foreign-scan
 * plan node contains that the given tlist doesn't.  The fdw_scan_tlist would
 * have contained resjunk columns such as 'ctid' of the target relation and
 * 'wholerow' of non-target relations, but the tlist might not contain them,
 * for example.  So, adjust the tlist so it contains all the columns specified
 * in the fdw_scan_tlist; else setrefs.c will get confused.
 */
static void
rebuild_fdw_scan_tlist(ForeignScan *fscan, List *tlist)
{
	List	   *new_tlist = tlist;
	List	   *old_tlist = fscan->fdw_scan_tlist;
	ListCell   *lc;

	foreach(lc, old_tlist)
	{
		TargetEntry *tle = (TargetEntry *) lfirst(lc);

		if (tlist_member(tle->expr, new_tlist))
			continue;			/* already got it */

		new_tlist = lappend(new_tlist,
							makeTargetEntry(tle->expr,
											list_length(new_tlist) + 1,
											NULL,
											false));
	}
	fscan->fdw_scan_tlist = new_tlist;
}


/*
 * oracleDeparseConcat
 *
 * Oracle supports the format of concat function as follows concat(str1, str2).
 * If user wants to put less/more argument(s), we need to deparse concat function.
 * For example, if user uses concat(str1, str2, str3, str4), function pushdown will
 * be concat(str1, concat(str2, concat(str3, str4))). If user uses concat(str1),
 * function pushdown will be concat(str1, NULL).
 */
static char *
oracleDeparseConcat(List *args, deparse_expr_cxt *context)
{
	StringInfoData result;
	char *left, *right = NULL;
	List *remain_items;

	Assert(list_length(args) >= 1);

	initStringInfo(&result);

	left = deparseExpr(linitial(args), context);

	if (left == NULL)
	{
		return NULL;
	}

	if (list_length(args) == 1)
	{
		appendStringInfo(&result, "concat(%s, NULL)", left);
		return result.data;
	}
	else if (list_length(args) == 2)
	{
		right = deparseExpr(lsecond(args), context);
	}
	else
	{
		/* Get the remaining items */
		remain_items = list_copy_tail(args, 1);

		right = oracleDeparseConcat(remain_items, context);
	}

	if (right == NULL)
	{
		pfree(left);
		return NULL;
	}

	appendStringInfo(&result, "concat(%s, %s)", left, right);
	return result.data;
}

/*
 * deparse remote UPDATE statement
 *
 * 'buf' is the output buffer to append the statement to
 * 'rtindex' is the RT index of the associated target relation
 * 'rel' is the relation descriptor for the target relation
 * 'foreignrel' is the RelOptInfo for the target relation or the join relation
 *		containing all base relations in the query
 * 'targetlist' is the tlist of the underlying foreign-scan plan node
 *		(note that this only contains new-value expressions and junk attrs)
 * 'targetAttrs' is the target columns of the UPDATE
 * 'remote_conds' is the qual clauses that must be evaluated remotely
 * '*params_list' is an output list of exprs that will become remote Params
 * 'returningList' is the RETURNING targetlist
 * '*retrieved_attrs' is an output list of integers of columns being retrieved
 *		by RETURNING (if any)
 */
static void
oracleDeparseDirectUpdateSql(StringInfo buf, PlannerInfo *root,
					   Index rtindex, Relation rel,
					   RelOptInfo *foreignrel,
					   List *targetlist,
					   List *targetAttrs,
					   List *remote_conds,
					   List **params_list,
					   List *returningList,
					   List **retrieved_attrs)
{
	deparse_expr_cxt context;
	int			nestlevel;
	bool		first;
	RangeTblEntry *rte = planner_rt_fetch(rtindex, root);
	ListCell   *lc,
			   *lc2;
	struct OracleFdwState *fpinfo = (struct OracleFdwState *) foreignrel->fdw_private;
	int	    index = 0;

	/* Set up context struct for recursion */
	initializeContext(fpinfo, root, foreignrel, foreignrel, &context);
	context.buf = buf;

	appendStringInfo(buf, "UPDATE %s", fpinfo->oraTable->name);
	if (foreignrel->reloptkind == RELOPT_JOINREL)
		appendStringInfo(buf, " %s%d", REL_ALIAS_PREFIX, rtindex);
	appendStringInfoString(buf, " SET ");

	/* Make sure any constants in the exprs are printed portably */
	nestlevel = set_transmission_modes();

	first = true;
#if PG_VERSION_NUM >= 140000
	forboth(lc, targetlist, lc2, targetAttrs)
	{
		TargetEntry *tle = lfirst_node(TargetEntry, lc);
		int			attnum = lfirst_int(lc2);
		char	*result;

		/* update's new-value expressions shouldn't be resjunk */
		Assert(!tle->resjunk);
#else
	foreach(lc, targetAttrs)
	{
		int			attnum = lfirst_int(lc);
		TargetEntry *tle = get_tle_by_resno(targetlist, attnum);
		char	*result;

		if (!tle)
			elog(ERROR, "attribute number %d not found in UPDATE targetlist",
				 attnum);
#endif
		/* find the corresponding oraTable entry */
		while (index < fpinfo->oraTable->ncols
			   && fpinfo->oraTable->cols[index]->pgattnum < attnum)
			++index;

		if (index == fpinfo->oraTable->ncols)
			break;

		/* ignore columns that don't occur in the foreign table */
		if (fpinfo->oraTable->cols[index]->pgtype == 0)
			continue;

		/* check that the data types can be converted */
		checkDataType(
			fpinfo->oraTable->cols[index]->oratype,
			fpinfo->oraTable->cols[index]->scale,
			fpinfo->oraTable->cols[index]->pgtype,
			fpinfo->oraTable->pgname,
			fpinfo->oraTable->cols[index]->pgname
		);

		if (!first)
			appendStringInfoString(buf, ", ");
		first = false;

		oracleDeparseColumnRef(fpinfo->oraTable, buf, rtindex, index, false);
		appendStringInfoString(buf, " = ");
		result = deparseExpr((Expr *) tle->expr, &context);
		appendStringInfoString(buf, result);
	}

	reset_transmission_modes(nestlevel);

	/* throw a meaningful error if nothing is updated */
	if (first)
		ereport(ERROR,
				(errcode(ERRCODE_FDW_UNABLE_TO_CREATE_EXECUTION),
				errmsg("no Oracle column modified by UPDATE"),
				errdetail("The UPDATE statement only changes colums that do not exist in the Oracle table.")));

	if (foreignrel->reloptkind == RELOPT_JOINREL)
	{
		List	   *ignore_conds = NIL;

		context.use_alias = true;
		context.ignore_rel = rtindex;
		context.ignore_conds = &ignore_conds;

		appendStringInfoString(buf, " FROM ");
		deparseFromExprForRel(buf, foreignrel, params_list, &context);
		remote_conds = list_concat(remote_conds, ignore_conds);
	}

	if (remote_conds)
	{
		appendStringInfoString(buf, " WHERE ");
		appendConditions(remote_conds, &context);
	}

	if (foreignrel->reloptkind == RELOPT_JOINREL)
		oracleDeparseExplicitTargetList(returningList, true, retrieved_attrs,
								  &context);
	else
		oracleDeparseReturningList(context.oraTable, buf, rte, rtindex, rel, false,
							 NIL, returningList, retrieved_attrs);
}

/*
 * deparse remote DELETE statement
 *
 * 'buf' is the output buffer to append the statement to
 * 'rtindex' is the RT index of the associated target relation
 * 'rel' is the relation descriptor for the target relation
 * 'foreignrel' is the RelOptInfo for the target relation or the join relation
 *		containing all base relations in the query
 * 'remote_conds' is the qual clauses that must be evaluated remotely
 * '*params_list' is an output list of exprs that will become remote Params
 * 'returningList' is the RETURNING targetlist
 * '*retrieved_attrs' is an output list of integers of columns being retrieved
 *		by RETURNING (if any)
 */
void
oracleDeparseDirectDeleteSql(StringInfo buf, PlannerInfo *root,
					   Index rtindex, Relation rel,
					   RelOptInfo *foreignrel,
					   List *remote_conds,
					   List **params_list,
					   List *returningList,
					   List **retrieved_attrs)
{
	deparse_expr_cxt context;
	struct OracleFdwState *fpinfo = (struct OracleFdwState *) foreignrel->fdw_private;

	/* Set up context struct for recursion */
	initializeContext(fpinfo, root, foreignrel, foreignrel, &context);
	context.buf = buf;

	appendStringInfo(buf, "DELETE FROM %s ", fpinfo->oraTable->name);
	if (foreignrel->reloptkind == RELOPT_JOINREL)
		appendStringInfo(buf, " %s%d", REL_ALIAS_PREFIX, rtindex);

	if (foreignrel->reloptkind == RELOPT_JOINREL)
	{
		List	   *ignore_conds = NIL;

		context.use_alias = true;
		context.ignore_rel = rtindex;
		context.ignore_conds = &ignore_conds;

		appendStringInfoString(buf, " USING ");
		deparseFromExprForRel(buf, foreignrel, params_list, &context);
		remote_conds = list_concat(remote_conds, ignore_conds);
	}

	if (remote_conds)
	{
		appendStringInfoString(buf, " WHERE ");
		appendConditions(remote_conds, &context);
	}

	if (foreignrel->reloptkind == RELOPT_JOINREL)
		oracleDeparseExplicitTargetList(returningList, true, retrieved_attrs,
								  &context);
	else
		oracleDeparseReturningList(context.oraTable, buf, planner_rt_fetch(rtindex, root),
							 rtindex, rel, false,
							 NIL, returningList, retrieved_attrs);
}

/*
 * init_returning_filter
 *
 * Initialize a filter to extract an updated/deleted tuple from a scan tuple.
 */
static void
init_returning_filter(struct OracleFdwState *dmstate,
					  List *fdw_scan_tlist,
					  Index rtindex)
{
	TupleDesc	resultTupType = RelationGetDescr(dmstate->resultRel);
	ListCell   *lc;
	int			i;

	/*
	 * Calculate the mapping between the fdw_scan_tlist's entries and the
	 * result tuple's attributes.
	 *
	 * The "map" is an array of indexes of the result tuple's attributes in
	 * fdw_scan_tlist, i.e., one entry for every attribute of the result
	 * tuple.  We store zero for any attributes that don't have the
	 * corresponding entries in that list, marking that a NULL is needed in
	 * the result tuple.
	 *
	 * Also get the indexes of the entries for ctid and oid if any.
	 */
	dmstate->attnoMap = (AttrNumber *)
		palloc0(resultTupType->natts * sizeof(AttrNumber));

	i = 1;
	foreach(lc, fdw_scan_tlist)
	{
		TargetEntry *tle = (TargetEntry *) lfirst(lc);
		Var		   *var = (Var *) tle->expr;

		Assert(IsA(var, Var));

		/*
		 * If the Var is a column of the target relation to be retrieved from
		 * the foreign server, get the index of the entry.
		 */
		if (var->varno == rtindex &&
			list_member_int(dmstate->retrieved_attrs, i))
		{
			int			attrno = var->varattno;

			/*
			 * We don't retrieve whole-row references to the target
			 * relation either.
			 */
			Assert(attrno > 0);

			dmstate->attnoMap[attrno - 1] = i;
		}
		i++;
	}
}

/*
 * apply_returning_filter
 *
 * Extract and return an updated/deleted tuple from a scan tuple.
 */
static TupleTableSlot *
apply_returning_filter(struct OracleFdwState *dmstate,
					   ResultRelInfo *resultRelInfo,
					   TupleTableSlot *slot,
					   EState *estate)
{
	TupleDesc	resultTupType = RelationGetDescr(dmstate->resultRel);
	TupleTableSlot *resultSlot;
	Datum	   *values;
	bool	   *isnull;
	Datum	   *old_values;
	bool	   *old_isnull;
	int			i;

	/*
	 * Use the return tuple slot as a place to store the result tuple.
	 */
	resultSlot = ExecGetReturningSlot(estate, resultRelInfo);

	/*
	 * Extract all the values of the scan tuple.
	 */
	slot_getallattrs(slot);
	old_values = slot->tts_values;
	old_isnull = slot->tts_isnull;

	/*
	 * Prepare to build the result tuple.
	 */
	ExecClearTuple(resultSlot);
	values = resultSlot->tts_values;
	isnull = resultSlot->tts_isnull;

	/*
	 * Transpose data into proper fields of the result tuple.
	 */
	for (i = 0; i < resultTupType->natts; i++)
	{
		int			j = dmstate->attnoMap[i];

		if (j == 0)
		{
			values[i] = (Datum) 0;
			isnull[i] = true;
		}
		else
		{
			values[i] = old_values[j - 1];
			isnull[i] = old_isnull[j - 1];
		}
	}

	/*
	 * Build the virtual tuple.
	 */
	ExecStoreVirtualTuple(resultSlot);

	/*
	 * And return the result tuple.
	 */
	return resultSlot;
}


/*
 * prepare_query_params
 *
 * Prepare for processing of parameters used in remote query.
 */
static void
prepare_query_params(struct OracleFdwState *fdw_state,
					 PlanState *node,
					 List *fdw_exprs,
					 int numParams)
{
	int			index;
	ListCell    *cell;
	List 		*exec_exprs;
	struct paramDesc *paramDesc;

	Assert(numParams > 0);

	/*
	 * Prepare remote-parameter expressions for evaluation.  (Note: in
	 * practice, we expect that all these expressions will be just Params, so
	 * we could possibly do something more efficient than using the full
	 * expression-eval machinery for this.  But probably there would be little
	 * benefit, and it'd require oracle_fdw to know more than is desirable
	 * about Param evaluation.)
	 */
	exec_exprs = ExecInitExprList(fdw_exprs, node);

	index = 0;
	foreach(cell, exec_exprs)
	{
		ExprState  *expr_state = (ExprState *) lfirst(cell);
		char parname[10];

		index++;
		if (expr_state == NULL)
			continue;

		/* create a new entry in the parameter list */
		paramDesc = (struct paramDesc *)palloc(sizeof(struct paramDesc));
		snprintf(parname, 10, ":p%d", index);
		paramDesc->name = pstrdup(parname);
		paramDesc->type = exprType((Node *)(expr_state->expr));

		if (paramDesc->type == TEXTOID || paramDesc->type == VARCHAROID
				|| paramDesc->type == BPCHAROID || paramDesc->type == CHAROID
				|| paramDesc->type == DATEOID || paramDesc->type == TIMESTAMPOID
				|| paramDesc->type == TIMESTAMPTZOID)
			paramDesc->bindType = BIND_STRING;
		else
			paramDesc->bindType = BIND_NUMBER;

		paramDesc->value = NULL;
		paramDesc->node = expr_state;
		paramDesc->bindh = NULL;
		paramDesc->colnum = -1;
		paramDesc->next = fdw_state->paramList;
		fdw_state->paramList = paramDesc;
	}

	/* add a fake parameter ":now" if that string appears in the query */
	if (strstr(fdw_state->query, ":now") != NULL)
	{
		paramDesc = (struct paramDesc *)palloc(sizeof(struct paramDesc));
		paramDesc->name = pstrdup(":now");
		paramDesc->type = TIMESTAMPTZOID;
		paramDesc->bindType = BIND_STRING;
		paramDesc->value = NULL;
		paramDesc->node = NULL;
		paramDesc->bindh = NULL;
		paramDesc->colnum = -1;
		paramDesc->next = fdw_state->paramList;
		fdw_state->paramList = paramDesc;
	}
}

/*
 * get_returning_data
 *
 * Get the result of a RETURNING clause.
 */
static TupleTableSlot *
get_returning_data(ForeignScanState *node)
{
	struct OracleFdwState *dmstate = (struct OracleFdwState *) node->fdw_state;
	EState	   *estate = node->ss.ps.state;
#if PG_VERSION_NUM >= 140000
	ResultRelInfo *resultRelInfo = node->resultRelInfo;
#else
	ResultRelInfo *resultRelInfo = estate->es_result_relation_info;
#endif
	TupleTableSlot *slot = node->ss.ss_ScanTupleSlot;
	TupleTableSlot *resultSlot;

	Assert(resultRelInfo->ri_projectReturning);

	/* If we didn't get any tuples, must be end of data. */
	if (dmstate->next_tuple >= dmstate->rowcount)
		return ExecClearTuple(slot);

	/* Increment the command es_processed count if necessary. */
	if (dmstate->set_processed)
		estate->es_processed += 1;

	/*
	 * Store a RETURNING tuple.  If has_returning is false, just emit a dummy
	 * tuple.  (has_returning is false when the local query is of the form
	 * "UPDATE/DELETE .. RETURNING 1" for example.)
	 */
	if (!dmstate->has_returning)
	{
		ExecStoreAllNullTuple(slot);
		resultSlot = slot;
	}
	else
	{
		/*
		 * On error, be sure to release the PGresult on the way out.  Callers
		 * do not have PG_TRY blocks to ensure this happens.
		 */
		PG_TRY();
		{
			/* clear slot */
			ExecClearTuple(slot);

			/* convert result to arrays of values and null indicators */
			convertTuple(dmstate, slot->tts_values, slot->tts_isnull, false);

			/* store the virtual tuple */
			ExecStoreVirtualTuple(slot);
		}
		PG_CATCH();
		{
			/* release the Oracle session */
			if (dmstate->session)
			{
				oracleCloseStatement(dmstate->session);
				pfree(dmstate->session);
				dmstate->session = NULL;
			}
			PG_RE_THROW();
		}
		PG_END_TRY();

		/* Get the updated/deleted tuple. */
		if (dmstate->rel)
			resultSlot = slot;
		else
			resultSlot = apply_returning_filter(dmstate, resultRelInfo, slot, estate);
	}
	dmstate->next_tuple++;

	/* Make slot available for evaluation of the local query RETURNING list. */
	resultRelInfo->ri_projectReturning->pi_exprContext->ecxt_scantuple =
		resultSlot;

	return slot;
}

/*
 * execute_dml_stmt
 *
 * Execute a direct UPDATE/DELETE statement.
 */
static void
execute_dml_stmt(ForeignScanState *node)
{
	struct OracleFdwState *dmstate = (struct OracleFdwState *) node->fdw_state;
	int			numParams = dmstate->numParams;
	MemoryContext oldcontext;

	/*
	 * Construct array of query parameter values in text format.
	 */
	if (numParams > 0)
	{
		/*
		 * this code path is not reached because Direct Modification
		 * does not support PARAM_MULTIEXPR as specified by postgres_fdw.
		 *
		 * todo: we may need to fill parameters (refer setSelectParameters)
		 * At the moment, there is no test case for this code path.
		 */
	}

	/* Use context to save returning values and session which will be used in each iteration */
	oldcontext = MemoryContextSwitchTo(dmstate->temp_cxt);

	/*
	 * Get connection to the foreign server.  Connection manager will
	 * establish new connection if necessary.
	 */
	dmstate->session = oracleGetSession(
			dmstate->dbserver,
			dmstate->isolation_level,
			dmstate->user,
			dmstate->password,
			dmstate->nls_lang,
			(int)dmstate->have_nchar,
			dmstate->oraTable->pgname,
			GetCurrentTransactionNestLevel()
		);

	oraclePrepareQuery(dmstate->session, dmstate->query, dmstate->oraTable, dmstate->prefetch);

	dmstate->rowcount = oracleExecuteQuery(dmstate->session, dmstate->oraTable, dmstate->paramList);

	MemoryContextSwitchTo(oldcontext);
}
