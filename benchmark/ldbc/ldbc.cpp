#include "ldbc.hpp"

#include "benchmark_runner.hpp"
#include "compare_result.hpp"
#include "duckdb_benchmark_macro.hpp"

using namespace duckdb;
using namespace std;

#define SF 1

#define LDBC_QUERY_BODY(QNR, ENABLE_RAIS, JO_NAME)                                                                     \
	virtual void Load(DuckDBBenchmarkState *state) {                                                                   \
		ldbc::dbgen(state->conn, SF, ENABLE_RAIS);                                                                     \
	}                                                                                                                  \
	virtual string GetQuery() {                                                                                        \
		return ldbc::get_query(QNR, SF);                                                                               \
	}                                                                                                                  \
	virtual string GetJO() {                                                                                           \
		return ldbc::get_default_jo(JO_NAME);                                                                          \
	}                                                                                                                  \
	virtual string VerifyResult(QueryResult *result) {                                                                 \
		if (!result->success) {                                                                                        \
			return result->error;                                                                                      \
		}                                                                                                              \
		return "";                                                                                                     \
	}                                                                                                                  \
	virtual string BenchmarkInfo() {                                                                                   \
		return StringUtil::Format("LDBC (JOB) Q%d: %s", QNR, ldbc::get_query(QNR, SF).c_str());                        \
	}

// HJ
// Q6, Q39, Q40:  Timeout
// Q10, Q11, Q21: Rows 0
// Q19:           Scalar Function with name array_agg does not exist!
// Q20:           Table with name sg does not exist!
// Q23:           Parser: syntax error at or near "p"
// Q24:           Parser: syntax error at or near "m"
// Q34:           Parser: syntax error at or near "m" [62]
// Q26:           Serialization: Cannot copy BoundSubqueryExpression
// Q31, Q46       Unhandled join type in JoinHashTable
// Q32:           Table Function with name unnest does not exist!
// Q42:           Parser: syntax error at or near "epoch_ms" [760]

DUCKDB_BENCHMARK(LDBCQ001, "[ldbc]")
LDBC_QUERY_BODY(1, false, "EMPTY");
FINISH_BENCHMARK(LDBCQ001)

DUCKDB_BENCHMARK(LDBCQ001A, "[ldbc]")
LDBC_QUERY_BODY(1, true, "EMPTY");
FINISH_BENCHMARK(LDBCQ001A)

DUCKDB_BENCHMARK(LDBCQ002, "[ldbc]")
LDBC_QUERY_BODY(2, false, "EMPTY");
FINISH_BENCHMARK(LDBCQ002)

DUCKDB_BENCHMARK(LDBCQ002A, "[ldbc]")
LDBC_QUERY_BODY(2, true, "EMPTY");
FINISH_BENCHMARK(LDBCQ002A)

DUCKDB_BENCHMARK(LDBCQ003, "[ldbc]")
LDBC_QUERY_BODY(3, "", "Q03");
FINISH_BENCHMARK(LDBCQ003)

DUCKDB_BENCHMARK(LDBCQ003A, "[ldbc]")
LDBC_QUERY_BODY(3, ALL_RAIS, "Q03");
FINISH_BENCHMARK(LDBCQ003A)

DUCKDB_BENCHMARK(LDBCQ004, "[ldbc]")
LDBC_QUERY_BODY(4, false, "EMPTY");
FINISH_BENCHMARK(LDBCQ004)

DUCKDB_BENCHMARK(LDBCQ004A, "[ldbc]")
LDBC_QUERY_BODY(4, true, "EMPTY");
FINISH_BENCHMARK(LDBCQ004A)

DUCKDB_BENCHMARK(LDBCQ005, "[ldbc]")
LDBC_QUERY_BODY(5, false, "EMPTY");
FINISH_BENCHMARK(LDBCQ005)

DUCKDB_BENCHMARK(LDBCQ005A, "[ldbc]")
LDBC_QUERY_BODY(5, true, "EMPTY");
FINISH_BENCHMARK(LDBCQ005A)

DUCKDB_BENCHMARK(LDBCQ006, "[ldbc]")
LDBC_QUERY_BODY(6, false, "EMPTY");
FINISH_BENCHMARK(LDBCQ006)

DUCKDB_BENCHMARK(LDBCQ006A, "[ldbc]")
LDBC_QUERY_BODY(6, true, "EMPTY");
FINISH_BENCHMARK(LDBCQ006A)

DUCKDB_BENCHMARK(LDBCQ007, "[ldbc]")
LDBC_QUERY_BODY(7, false, "EMPTY");
FINISH_BENCHMARK(LDBCQ007)

DUCKDB_BENCHMARK(LDBCQ007A, "[ldbc]")
LDBC_QUERY_BODY(7, true, "EMPTY");
FINISH_BENCHMARK(LDBCQ007A)

DUCKDB_BENCHMARK(LDBCQ008, "[ldbc]")
LDBC_QUERY_BODY(8, "", "Q08");
FINISH_BENCHMARK(LDBCQ008)

DUCKDB_BENCHMARK(LDBCQ008A, "[ldbc]")
LDBC_QUERY_BODY(8, ALL_RAIS, "Q08");
FINISH_BENCHMARK(LDBCQ008A)

DUCKDB_BENCHMARK(LDBCQ009, "[ldbc]")
LDBC_QUERY_BODY(9, false, "EMPTY");
FINISH_BENCHMARK(LDBCQ009)

DUCKDB_BENCHMARK(LDBCQ009A, "[ldbc]")
LDBC_QUERY_BODY(9, true, "EMPTY");
FINISH_BENCHMARK(LDBCQ009A)

DUCKDB_BENCHMARK(LDBCQ010, "[ldbc]")
LDBC_QUERY_BODY(10, false, "EMPTY");
FINISH_BENCHMARK(LDBCQ010)

DUCKDB_BENCHMARK(LDBCQ010A, "[ldbc]")
LDBC_QUERY_BODY(10, true, "EMPTY");
FINISH_BENCHMARK(LDBCQ010A)

DUCKDB_BENCHMARK(LDBCQ011, "[ldbc]")
LDBC_QUERY_BODY(11, false, "EMPTY");
FINISH_BENCHMARK(LDBCQ011)

DUCKDB_BENCHMARK(LDBCQ011A, "[ldbc]")
LDBC_QUERY_BODY(11, true, "EMPTY");
FINISH_BENCHMARK(LDBCQ011A)

DUCKDB_BENCHMARK(LDBCQ012, "[ldbc]")
LDBC_QUERY_BODY(12, false, "EMPTY");
FINISH_BENCHMARK(LDBCQ012)

DUCKDB_BENCHMARK(LDBCQ012A, "[ldbc]")
LDBC_QUERY_BODY(12, true, "EMPTY");
FINISH_BENCHMARK(LDBCQ012A)

DUCKDB_BENCHMARK(LDBCQ013, "[ldbc]")
LDBC_QUERY_BODY(13, false, "EMPTY");
FINISH_BENCHMARK(LDBCQ013)

DUCKDB_BENCHMARK(LDBCQ013A, "[ldbc]")
LDBC_QUERY_BODY(13, true, "EMPTY");
FINISH_BENCHMARK(LDBCQ013A)

DUCKDB_BENCHMARK(LDBCQ014, "[ldbc]")
LDBC_QUERY_BODY(14, false, "EMPTY");
FINISH_BENCHMARK(LDBCQ014)

DUCKDB_BENCHMARK(LDBCQ014A, "[ldbc]")
LDBC_QUERY_BODY(14, true, "EMPTY");
FINISH_BENCHMARK(LDBCQ014A)

DUCKDB_BENCHMARK(LDBCQ015, "[ldbc]")
LDBC_QUERY_BODY(15, false, "EMPTY");
FINISH_BENCHMARK(LDBCQ015)

DUCKDB_BENCHMARK(LDBCQ015A, "[ldbc]")
LDBC_QUERY_BODY(15, true, "EMPTY");
FINISH_BENCHMARK(LDBCQ015A)

DUCKDB_BENCHMARK(LDBCQ016, "[ldbc]")
LDBC_QUERY_BODY(16, false, "EMPTY");
FINISH_BENCHMARK(LDBCQ016)

DUCKDB_BENCHMARK(LDBCQ016A, "[ldbc]")
LDBC_QUERY_BODY(16, true, "EMPTY");
FINISH_BENCHMARK(LDBCQ016A)

DUCKDB_BENCHMARK(LDBCQ017, "[ldbc]")
LDBC_QUERY_BODY(17, false, "EMPTY");
FINISH_BENCHMARK(LDBCQ017)

DUCKDB_BENCHMARK(LDBCQ017A, "[ldbc]")
LDBC_QUERY_BODY(17, true, "EMPTY");
FINISH_BENCHMARK(LDBCQ017A)

DUCKDB_BENCHMARK(LDBCQ018, "[ldbc]")
LDBC_QUERY_BODY(18, false, "EMPTY");
FINISH_BENCHMARK(LDBCQ018)

DUCKDB_BENCHMARK(LDBCQ018A, "[ldbc]")
LDBC_QUERY_BODY(18, true, "EMPTY");
FINISH_BENCHMARK(LDBCQ018A)

DUCKDB_BENCHMARK(LDBCQ019, "[ldbc]")
LDBC_QUERY_BODY(19, false, "EMPTY");
FINISH_BENCHMARK(LDBCQ019)

DUCKDB_BENCHMARK(LDBCQ019A, "[ldbc]")
LDBC_QUERY_BODY(19, true, "EMPTY");
FINISH_BENCHMARK(LDBCQ019A)

DUCKDB_BENCHMARK(LDBCQ020, "[ldbc]")
LDBC_QUERY_BODY(20, false, "EMPTY");
FINISH_BENCHMARK(LDBCQ020)

DUCKDB_BENCHMARK(LDBCQ020A, "[ldbc]")
LDBC_QUERY_BODY(20, true, "EMPTY");
FINISH_BENCHMARK(LDBCQ020A)

DUCKDB_BENCHMARK(LDBCQ021, "[ldbc]")
LDBC_QUERY_BODY(21, false, "EMPTY");
FINISH_BENCHMARK(LDBCQ021)

DUCKDB_BENCHMARK(LDBCQ021A, "[ldbc]")
LDBC_QUERY_BODY(21, true, "EMPTY");
FINISH_BENCHMARK(LDBCQ021A)

DUCKDB_BENCHMARK(LDBCQ022, "[ldbc]")
LDBC_QUERY_BODY(22, false, "EMPTY");
FINISH_BENCHMARK(LDBCQ022)

DUCKDB_BENCHMARK(LDBCQ022A, "[ldbc]")
LDBC_QUERY_BODY(22, true, "EMPTY");
FINISH_BENCHMARK(LDBCQ022A)

DUCKDB_BENCHMARK(LDBCQ023, "[ldbc]")
LDBC_QUERY_BODY(23, "", "Q23");
FINISH_BENCHMARK(LDBCQ023)

DUCKDB_BENCHMARK(LDBCQ023A, "[ldbc]")
LDBC_QUERY_BODY(23, ALL_RAIS, "Q23");
FINISH_BENCHMARK(LDBCQ023A)

DUCKDB_BENCHMARK(LDBCQ024, "[ldbc]")
LDBC_QUERY_BODY(24, "", "Q24");
FINISH_BENCHMARK(LDBCQ024)

DUCKDB_BENCHMARK(LDBCQ024A, "[ldbc]")
LDBC_QUERY_BODY(24, ALL_RAIS, "Q24");
FINISH_BENCHMARK(LDBCQ024A)

DUCKDB_BENCHMARK(LDBCQ025, "[ldbc]")
LDBC_QUERY_BODY(25, "", "Q25");
FINISH_BENCHMARK(LDBCQ025)

DUCKDB_BENCHMARK(LDBCQ025A, "[ldbc]")
LDBC_QUERY_BODY(25, ALL_RAIS, "Q25");
FINISH_BENCHMARK(LDBCQ025A)

DUCKDB_BENCHMARK(LDBCQ026, "[ldbc]")
LDBC_QUERY_BODY(26, false, "EMPTY");
FINISH_BENCHMARK(LDBCQ026)

DUCKDB_BENCHMARK(LDBCQ026A, "[ldbc]")
LDBC_QUERY_BODY(26, true, "EMPTY");
FINISH_BENCHMARK(LDBCQ026A)

DUCKDB_BENCHMARK(LDBCQ027, "[ldbc]")
LDBC_QUERY_BODY(27, "", "Q27");
FINISH_BENCHMARK(LDBCQ027)

DUCKDB_BENCHMARK(LDBCQ027A, "[ldbc]")
LDBC_QUERY_BODY(27, ALL_RAIS, "Q27");
FINISH_BENCHMARK(LDBCQ027A)

DUCKDB_BENCHMARK(LDBCQ028, "[ldbc]")
LDBC_QUERY_BODY(28, false, "EMPTY");
FINISH_BENCHMARK(LDBCQ028)

DUCKDB_BENCHMARK(LDBCQ028A, "[ldbc]")
LDBC_QUERY_BODY(28, true, "EMPTY");
FINISH_BENCHMARK(LDBCQ028A)

DUCKDB_BENCHMARK(LDBCQ029, "[ldbc]")
LDBC_QUERY_BODY(29, false, "EMPTY");
FINISH_BENCHMARK(LDBCQ029)

DUCKDB_BENCHMARK(LDBCQ029A, "[ldbc]")
LDBC_QUERY_BODY(29, true, "EMPTY");
FINISH_BENCHMARK(LDBCQ029A)

DUCKDB_BENCHMARK(LDBCQ030, "[ldbc]")
LDBC_QUERY_BODY(30, false, "EMPTY");
FINISH_BENCHMARK(LDBCQ030)

DUCKDB_BENCHMARK(LDBCQ030A, "[ldbc]")
LDBC_QUERY_BODY(30, true, "EMPTY");
FINISH_BENCHMARK(LDBCQ030A)

DUCKDB_BENCHMARK(LDBCQ031, "[ldbc]")
LDBC_QUERY_BODY(31, false, "EMPTY");
FINISH_BENCHMARK(LDBCQ031)

DUCKDB_BENCHMARK(LDBCQ031A, "[ldbc]")
LDBC_QUERY_BODY(31, true, "EMPTY");
FINISH_BENCHMARK(LDBCQ031A)

DUCKDB_BENCHMARK(LDBCQ032, "[ldbc]")
LDBC_QUERY_BODY(32, false, "EMPTY");
FINISH_BENCHMARK(LDBCQ032)

DUCKDB_BENCHMARK(LDBCQ032A, "[ldbc]")
LDBC_QUERY_BODY(32, true, "EMPTY");
FINISH_BENCHMARK(LDBCQ032A)

DUCKDB_BENCHMARK(LDBCQ033, "[ldbc]")
LDBC_QUERY_BODY(33, "", "Q33");
FINISH_BENCHMARK(LDBCQ033)

DUCKDB_BENCHMARK(LDBCQ033A, "[ldbc]")
LDBC_QUERY_BODY(33, ALL_RAIS, "Q33");
FINISH_BENCHMARK(LDBCQ033A)

DUCKDB_BENCHMARK(LDBCQ034, "[ldbc]")
LDBC_QUERY_BODY(34, false, "EMPTY");
FINISH_BENCHMARK(LDBCQ034)

DUCKDB_BENCHMARK(LDBCQ034A, "[ldbc]")
LDBC_QUERY_BODY(34, true, "EMPTY");
FINISH_BENCHMARK(LDBCQ034A)

DUCKDB_BENCHMARK(LDBCQ035, "[ldbc]")
LDBC_QUERY_BODY(35, false, "EMPTY");
FINISH_BENCHMARK(LDBCQ035)

DUCKDB_BENCHMARK(LDBCQ035A, "[ldbc]")
LDBC_QUERY_BODY(35, true, "EMPTY");
FINISH_BENCHMARK(LDBCQ035A)

DUCKDB_BENCHMARK(LDBCQ036, "[ldbc]")
LDBC_QUERY_BODY(36, false, "EMPTY");
FINISH_BENCHMARK(LDBCQ036)

DUCKDB_BENCHMARK(LDBCQ036A, "[ldbc]")
LDBC_QUERY_BODY(36, true, "EMPTY");
FINISH_BENCHMARK(LDBCQ036A)

DUCKDB_BENCHMARK(LDBCQ037, "[ldbc]")
LDBC_QUERY_BODY(37, false, "EMPTY");
FINISH_BENCHMARK(LDBCQ037)

DUCKDB_BENCHMARK(LDBCQ037A, "[ldbc]")
LDBC_QUERY_BODY(37, true, "EMPTY");
FINISH_BENCHMARK(LDBCQ037A)

DUCKDB_BENCHMARK(LDBCQ038, "[ldbc]")
LDBC_QUERY_BODY(38, false, "EMPTY");
FINISH_BENCHMARK(LDBCQ038)

DUCKDB_BENCHMARK(LDBCQ038A, "[ldbc]")
LDBC_QUERY_BODY(38, true, "EMPTY");
FINISH_BENCHMARK(LDBCQ038A)

DUCKDB_BENCHMARK(LDBCQ039, "[ldbc]")
LDBC_QUERY_BODY(39, false, "EMPTY");
FINISH_BENCHMARK(LDBCQ039)

DUCKDB_BENCHMARK(LDBCQ039A, "[ldbc]")
LDBC_QUERY_BODY(39, true, "EMPTY");
FINISH_BENCHMARK(LDBCQ039A)

DUCKDB_BENCHMARK(LDBCQ040, "[ldbc]")
LDBC_QUERY_BODY(40, false, "EMPTY");
FINISH_BENCHMARK(LDBCQ040)

DUCKDB_BENCHMARK(LDBCQ040A, "[ldbc]")
LDBC_QUERY_BODY(40, true, "EMPTY");
FINISH_BENCHMARK(LDBCQ040A)

DUCKDB_BENCHMARK(LDBCQ041, "[ldbc]")
LDBC_QUERY_BODY(41, false, "EMPTY");
FINISH_BENCHMARK(LDBCQ041)

DUCKDB_BENCHMARK(LDBCQ041A, "[ldbc]")
LDBC_QUERY_BODY(41, true, "EMPTY");
FINISH_BENCHMARK(LDBCQ041A)

DUCKDB_BENCHMARK(LDBCQ042, "[ldbc]")
LDBC_QUERY_BODY(42, false, "EMPTY");
FINISH_BENCHMARK(LDBCQ042)

DUCKDB_BENCHMARK(LDBCQ042A, "[ldbc]")
LDBC_QUERY_BODY(42, true, "EMPTY");
FINISH_BENCHMARK(LDBCQ042A)

DUCKDB_BENCHMARK(LDBCQ043, "[ldbc]")
LDBC_QUERY_BODY(43, false, "EMPTY");
FINISH_BENCHMARK(LDBCQ043)

DUCKDB_BENCHMARK(LDBCQ043A, "[ldbc]")
LDBC_QUERY_BODY(43, true, "EMPTY");
FINISH_BENCHMARK(LDBCQ043A)

DUCKDB_BENCHMARK(LDBCQ044, "[ldbc]")
LDBC_QUERY_BODY(44, false, "EMPTY");
FINISH_BENCHMARK(LDBCQ044)

DUCKDB_BENCHMARK(LDBCQ044A, "[ldbc]")
LDBC_QUERY_BODY(44, true, "EMPTY");
FINISH_BENCHMARK(LDBCQ044A)

DUCKDB_BENCHMARK(LDBCQ045, "[ldbc]")
LDBC_QUERY_BODY(45, "", "Q45");
FINISH_BENCHMARK(LDBCQ045)

DUCKDB_BENCHMARK(LDBCQ045A, "[ldbc]")
LDBC_QUERY_BODY(45, ALL_RAIS, "Q45");
FINISH_BENCHMARK(LDBCQ045A)

DUCKDB_BENCHMARK(LDBCQ046, "[ldbc]")
LDBC_QUERY_BODY(46, false, "EMPTY");
FINISH_BENCHMARK(LDBCQ046)

DUCKDB_BENCHMARK(LDBCQ046A, "[ldbc]")
LDBC_QUERY_BODY(46, true, "EMPTY");
FINISH_BENCHMARK(LDBCQ046A)
