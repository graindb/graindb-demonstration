#include "benchmark_runner.hpp"
#include "compare_result.hpp"
#include "duckdb_benchmark_macro.hpp"
#include "imdb.hpp"

using namespace duckdb;
using namespace std;

// JOB benchmark runner with duckdb-default join order

#define IMDB_QUERY_BODY(QNR, ENABLE_RAIS, JO_NAME)                                                                     \
	virtual void Load(DuckDBBenchmarkState *state) {                                                                   \
		imdb::dbgen(state->conn, ENABLE_RAIS);                                                                         \
	}                                                                                                                  \
	virtual string GetQuery() {                                                                                        \
		return imdb::get_113_query(QNR);                                                                               \
	}                                                                                                                  \
	virtual string GetJO() {                                                                                           \
		return imdb::get_113_default_jo(JO_NAME);                                                                      \
	}                                                                                                                  \
	virtual string VerifyResult(QueryResult *result) {                                                                 \
		if (!result->success) {                                                                                        \
			return result->error;                                                                                      \
		}                                                                                                              \
		return "";                                                                                                     \
	}                                                                                                                  \
	virtual string BenchmarkInfo() {                                                                                   \
		return StringUtil::Format("IMDB_113 (JOB) Q%d: %s", QNR, imdb::get_113_query(QNR).c_str());                    \
	}

// Q1
DUCKDB_BENCHMARK(IMDB_113_DEFAULT_Q001, "[imdb_default]")
IMDB_QUERY_BODY(1, false, "EMPTY");
FINISH_BENCHMARK(IMDB_113_DEFAULT_Q001);

DUCKDB_BENCHMARK(IMDB_113_DEFAULT_Q001A, "[imdb_default]")
IMDB_QUERY_BODY(1, true, "EMPTY");
FINISH_BENCHMARK(IMDB_113_DEFAULT_Q001A);

DUCKDB_BENCHMARK(IMDB_113_DEFAULT_Q002, "[imdb_default]")
IMDB_QUERY_BODY(2, false, "EMPTY");
FINISH_BENCHMARK(IMDB_113_DEFAULT_Q002);

DUCKDB_BENCHMARK(IMDB_113_DEFAULT_Q002A, "[imdb_default]")
IMDB_QUERY_BODY(2, true, "EMPTY");
FINISH_BENCHMARK(IMDB_113_DEFAULT_Q002A);

DUCKDB_BENCHMARK(IMDB_113_DEFAULT_Q003, "[imdb_default]")
IMDB_QUERY_BODY(3, false, "EMPTY");
FINISH_BENCHMARK(IMDB_113_DEFAULT_Q003);

DUCKDB_BENCHMARK(IMDB_113_DEFAULT_Q003A, "[imdb_default]")
IMDB_QUERY_BODY(3, true, "EMPTY");
FINISH_BENCHMARK(IMDB_113_DEFAULT_Q003A);

DUCKDB_BENCHMARK(IMDB_113_DEFAULT_Q004, "[imdb_default]")
IMDB_QUERY_BODY(4, false, "EMPTY");
FINISH_BENCHMARK(IMDB_113_DEFAULT_Q004);

DUCKDB_BENCHMARK(IMDB_113_DEFAULT_Q004A, "[imdb_default]")
IMDB_QUERY_BODY(4, true, "EMPTY");
FINISH_BENCHMARK(IMDB_113_DEFAULT_Q004A);

// Q2
DUCKDB_BENCHMARK(IMDB_113_DEFAULT_Q005, "[imdb_default]")
IMDB_QUERY_BODY(5, false, "EMPTY");
FINISH_BENCHMARK(IMDB_113_DEFAULT_Q005);

DUCKDB_BENCHMARK(IMDB_113_DEFAULT_Q005A, "[imdb_default]")
IMDB_QUERY_BODY(5, true, "EMPTY");
FINISH_BENCHMARK(IMDB_113_DEFAULT_Q005A);

DUCKDB_BENCHMARK(IMDB_113_DEFAULT_Q006, "[imdb_default]")
IMDB_QUERY_BODY(6, false, "EMPTY");
FINISH_BENCHMARK(IMDB_113_DEFAULT_Q006);

DUCKDB_BENCHMARK(IMDB_113_DEFAULT_Q006A, "[imdb_default]")
IMDB_QUERY_BODY(6, true, "EMPTY");
FINISH_BENCHMARK(IMDB_113_DEFAULT_Q006A);

DUCKDB_BENCHMARK(IMDB_113_DEFAULT_Q007, "[imdb_default]")
IMDB_QUERY_BODY(7, false, "EMPTY");
FINISH_BENCHMARK(IMDB_113_DEFAULT_Q007);

DUCKDB_BENCHMARK(IMDB_113_DEFAULT_Q007A, "[imdb_default]")
IMDB_QUERY_BODY(7, true, "EMPTY");
FINISH_BENCHMARK(IMDB_113_DEFAULT_Q007A);

DUCKDB_BENCHMARK(IMDB_113_DEFAULT_Q008, "[imdb_default]")
IMDB_QUERY_BODY(8, false, "EMPTY");
FINISH_BENCHMARK(IMDB_113_DEFAULT_Q008);

DUCKDB_BENCHMARK(IMDB_113_DEFAULT_Q008A, "[imdb_default]")
IMDB_QUERY_BODY(8, true, "EMPTY");
FINISH_BENCHMARK(IMDB_113_DEFAULT_Q008A);

// Q3
DUCKDB_BENCHMARK(IMDB_113_DEFAULT_Q009, "[imdb_default]")
IMDB_QUERY_BODY(9, false, "EMPTY");
FINISH_BENCHMARK(IMDB_113_DEFAULT_Q009);

DUCKDB_BENCHMARK(IMDB_113_DEFAULT_Q009A, "[imdb_default]")
IMDB_QUERY_BODY(9, true, "EMPTY");
FINISH_BENCHMARK(IMDB_113_DEFAULT_Q009A);

DUCKDB_BENCHMARK(IMDB_113_DEFAULT_Q010, "[imdb_default]")
IMDB_QUERY_BODY(10, false, "EMPTY");
FINISH_BENCHMARK(IMDB_113_DEFAULT_Q010);

DUCKDB_BENCHMARK(IMDB_113_DEFAULT_Q010A, "[imdb_default]")
IMDB_QUERY_BODY(10, true, "EMPTY");
FINISH_BENCHMARK(IMDB_113_DEFAULT_Q010A);

DUCKDB_BENCHMARK(IMDB_113_DEFAULT_Q011, "[imdb_default]")
IMDB_QUERY_BODY(11, false, "EMPTY");
FINISH_BENCHMARK(IMDB_113_DEFAULT_Q011);

DUCKDB_BENCHMARK(IMDB_113_DEFAULT_Q011A, "[imdb_default]")
IMDB_QUERY_BODY(11, true, "EMPTY");
FINISH_BENCHMARK(IMDB_113_DEFAULT_Q011A);

// Q4
DUCKDB_BENCHMARK(IMDB_113_DEFAULT_Q012, "[imdb_default]")
IMDB_QUERY_BODY(12, false, "EMPTY");
FINISH_BENCHMARK(IMDB_113_DEFAULT_Q012);

DUCKDB_BENCHMARK(IMDB_113_DEFAULT_Q012A, "[imdb_default]")
IMDB_QUERY_BODY(12, true, "EMPTY");
FINISH_BENCHMARK(IMDB_113_DEFAULT_Q012A);

DUCKDB_BENCHMARK(IMDB_113_DEFAULT_Q013, "[imdb_default]")
IMDB_QUERY_BODY(13, false, "EMPTY");
FINISH_BENCHMARK(IMDB_113_DEFAULT_Q013);

DUCKDB_BENCHMARK(IMDB_113_DEFAULT_Q013A, "[imdb_default]")
IMDB_QUERY_BODY(13, true, "EMPTY");
FINISH_BENCHMARK(IMDB_113_DEFAULT_Q013A);

DUCKDB_BENCHMARK(IMDB_113_DEFAULT_Q014, "[imdb_default]")
IMDB_QUERY_BODY(14, false, "EMPTY");
FINISH_BENCHMARK(IMDB_113_DEFAULT_Q014);

DUCKDB_BENCHMARK(IMDB_113_DEFAULT_Q014A, "[imdb_default]")
IMDB_QUERY_BODY(14, true, "EMPTY");
FINISH_BENCHMARK(IMDB_113_DEFAULT_Q014A);

// Q5
DUCKDB_BENCHMARK(IMDB_113_DEFAULT_Q015, "[imdb_default]")
IMDB_QUERY_BODY(15, false, "EMPTY");
FINISH_BENCHMARK(IMDB_113_DEFAULT_Q015);

DUCKDB_BENCHMARK(IMDB_113_DEFAULT_Q015A, "[imdb_default]")
IMDB_QUERY_BODY(15, true, "EMPTY");
FINISH_BENCHMARK(IMDB_113_DEFAULT_Q015A);

DUCKDB_BENCHMARK(IMDB_113_DEFAULT_Q016, "[imdb_default]")
IMDB_QUERY_BODY(16, false, "EMPTY");
FINISH_BENCHMARK(IMDB_113_DEFAULT_Q016);

DUCKDB_BENCHMARK(IMDB_113_DEFAULT_Q016A, "[imdb_default]")
IMDB_QUERY_BODY(16, true, "EMPTY");
FINISH_BENCHMARK(IMDB_113_DEFAULT_Q016A);

DUCKDB_BENCHMARK(IMDB_113_DEFAULT_Q017, "[imdb_default]")
IMDB_QUERY_BODY(17, false, "EMPTY");
FINISH_BENCHMARK(IMDB_113_DEFAULT_Q017);

DUCKDB_BENCHMARK(IMDB_113_DEFAULT_Q017A, "[imdb_default]")
IMDB_QUERY_BODY(17, true, "EMPTY");
FINISH_BENCHMARK(IMDB_113_DEFAULT_Q017A);

// Q6
DUCKDB_BENCHMARK(IMDB_113_DEFAULT_Q018, "[imdb_default]")
IMDB_QUERY_BODY(18, false, "EMPTY");
FINISH_BENCHMARK(IMDB_113_DEFAULT_Q018);

DUCKDB_BENCHMARK(IMDB_113_DEFAULT_Q018A, "[imdb_default]")
IMDB_QUERY_BODY(18, true, "EMPTY");
FINISH_BENCHMARK(IMDB_113_DEFAULT_Q018A);

DUCKDB_BENCHMARK(IMDB_113_DEFAULT_Q019, "[imdb_default]")
IMDB_QUERY_BODY(19, false, "EMPTY");
FINISH_BENCHMARK(IMDB_113_DEFAULT_Q019);

DUCKDB_BENCHMARK(IMDB_113_DEFAULT_Q019A, "[imdb_default]")
IMDB_QUERY_BODY(19, true, "EMPTY");
FINISH_BENCHMARK(IMDB_113_DEFAULT_Q019A);

DUCKDB_BENCHMARK(IMDB_113_DEFAULT_Q020, "[imdb_default]")
IMDB_QUERY_BODY(20, false, "EMPTY");
FINISH_BENCHMARK(IMDB_113_DEFAULT_Q020);

DUCKDB_BENCHMARK(IMDB_113_DEFAULT_Q020A, "[imdb_default]")
IMDB_QUERY_BODY(20, true, "EMPTY");
FINISH_BENCHMARK(IMDB_113_DEFAULT_Q020A);

DUCKDB_BENCHMARK(IMDB_113_DEFAULT_Q021, "[imdb_default]")
IMDB_QUERY_BODY(21, false, "EMPTY");
FINISH_BENCHMARK(IMDB_113_DEFAULT_Q021);

DUCKDB_BENCHMARK(IMDB_113_DEFAULT_Q021A, "[imdb_default]")
IMDB_QUERY_BODY(21, true, "EMPTY");
FINISH_BENCHMARK(IMDB_113_DEFAULT_Q021A);

DUCKDB_BENCHMARK(IMDB_113_DEFAULT_Q022, "[imdb_default]")
IMDB_QUERY_BODY(22, false, "EMPTY");
FINISH_BENCHMARK(IMDB_113_DEFAULT_Q022);

DUCKDB_BENCHMARK(IMDB_113_DEFAULT_Q022A, "[imdb_default]")
IMDB_QUERY_BODY(22, true, "EMPTY");
FINISH_BENCHMARK(IMDB_113_DEFAULT_Q022A);

DUCKDB_BENCHMARK(IMDB_113_DEFAULT_Q023, "[imdb_default]")
IMDB_QUERY_BODY(23, false, "EMPTY");
FINISH_BENCHMARK(IMDB_113_DEFAULT_Q023);

DUCKDB_BENCHMARK(IMDB_113_DEFAULT_Q023A, "[imdb_default]")
IMDB_QUERY_BODY(23, true, "EMPTY");
FINISH_BENCHMARK(IMDB_113_DEFAULT_Q023A);

// Q7
DUCKDB_BENCHMARK(IMDB_113_DEFAULT_Q024, "[imdb_default]")
IMDB_QUERY_BODY(24, false, "EMPTY");
FINISH_BENCHMARK(IMDB_113_DEFAULT_Q024);

DUCKDB_BENCHMARK(IMDB_113_DEFAULT_Q024A, "[imdb_default]")
IMDB_QUERY_BODY(24, true, "EMPTY");
FINISH_BENCHMARK(IMDB_113_DEFAULT_Q024A);

DUCKDB_BENCHMARK(IMDB_113_DEFAULT_Q025, "[imdb_default]")
IMDB_QUERY_BODY(25, false, "EMPTY");
FINISH_BENCHMARK(IMDB_113_DEFAULT_Q025);

DUCKDB_BENCHMARK(IMDB_113_DEFAULT_Q025A, "[imdb_default]")
IMDB_QUERY_BODY(25, true, "EMPTY");
FINISH_BENCHMARK(IMDB_113_DEFAULT_Q025A);

DUCKDB_BENCHMARK(IMDB_113_DEFAULT_Q026, "[imdb_default]")
IMDB_QUERY_BODY(26, false, "EMPTY");
FINISH_BENCHMARK(IMDB_113_DEFAULT_Q026);

DUCKDB_BENCHMARK(IMDB_113_DEFAULT_Q026A, "[imdb_default]")
IMDB_QUERY_BODY(26, true, "EMPTY");
FINISH_BENCHMARK(IMDB_113_DEFAULT_Q026A);

// Q08
DUCKDB_BENCHMARK(IMDB_113_DEFAULT_Q027, "[imdb_default]")
IMDB_QUERY_BODY(27, false, "EMPTY");
FINISH_BENCHMARK(IMDB_113_DEFAULT_Q027);

DUCKDB_BENCHMARK(IMDB_113_DEFAULT_Q027A, "[imdb_default]")
IMDB_QUERY_BODY(27, true, "EMPTY");
FINISH_BENCHMARK(IMDB_113_DEFAULT_Q027A);

DUCKDB_BENCHMARK(IMDB_113_DEFAULT_Q028, "[imdb_default]")
IMDB_QUERY_BODY(28, false, "EMPTY");
FINISH_BENCHMARK(IMDB_113_DEFAULT_Q028);

DUCKDB_BENCHMARK(IMDB_113_DEFAULT_Q028A, "[imdb_default]")
IMDB_QUERY_BODY(28, true, "EMPTY");
FINISH_BENCHMARK(IMDB_113_DEFAULT_Q028A);

DUCKDB_BENCHMARK(IMDB_113_DEFAULT_Q029, "[imdb_default]")
IMDB_QUERY_BODY(29, false, "EMPTY");
FINISH_BENCHMARK(IMDB_113_DEFAULT_Q029);

DUCKDB_BENCHMARK(IMDB_113_DEFAULT_Q029A, "[imdb_default]")
IMDB_QUERY_BODY(29, true, "EMPTY");
FINISH_BENCHMARK(IMDB_113_DEFAULT_Q029A);

DUCKDB_BENCHMARK(IMDB_113_DEFAULT_Q030, "[imdb_default]")
IMDB_QUERY_BODY(30, false, "EMPTY");
FINISH_BENCHMARK(IMDB_113_DEFAULT_Q030);

DUCKDB_BENCHMARK(IMDB_113_DEFAULT_Q030A, "[imdb_default]")
IMDB_QUERY_BODY(30, true, "EMPTY");
FINISH_BENCHMARK(IMDB_113_DEFAULT_Q030A);

// Q09
DUCKDB_BENCHMARK(IMDB_113_DEFAULT_Q031, "[imdb_default]")
IMDB_QUERY_BODY(31, false, "EMPTY");
FINISH_BENCHMARK(IMDB_113_DEFAULT_Q031);

DUCKDB_BENCHMARK(IMDB_113_DEFAULT_Q031A, "[imdb_default]")
IMDB_QUERY_BODY(31, true, "EMPTY");
FINISH_BENCHMARK(IMDB_113_DEFAULT_Q031A);

DUCKDB_BENCHMARK(IMDB_113_DEFAULT_Q032, "[imdb_default]")
IMDB_QUERY_BODY(32, false, "EMPTY");
FINISH_BENCHMARK(IMDB_113_DEFAULT_Q032);

DUCKDB_BENCHMARK(IMDB_113_DEFAULT_Q032A, "[imdb_default]")
IMDB_QUERY_BODY(32, true, "EMPTY");
FINISH_BENCHMARK(IMDB_113_DEFAULT_Q032A);

DUCKDB_BENCHMARK(IMDB_113_DEFAULT_Q033, "[imdb_default]")
IMDB_QUERY_BODY(33, false, "EMPTY");
FINISH_BENCHMARK(IMDB_113_DEFAULT_Q033);

DUCKDB_BENCHMARK(IMDB_113_DEFAULT_Q033A, "[imdb_default]")
IMDB_QUERY_BODY(33, true, "EMPTY");
FINISH_BENCHMARK(IMDB_113_DEFAULT_Q033A);

DUCKDB_BENCHMARK(IMDB_113_DEFAULT_Q034, "[imdb_default]")
IMDB_QUERY_BODY(34, false, "EMPTY");
FINISH_BENCHMARK(IMDB_113_DEFAULT_Q034);

DUCKDB_BENCHMARK(IMDB_113_DEFAULT_Q034A, "[imdb_default]")
IMDB_QUERY_BODY(34, true, "EMPTY");
FINISH_BENCHMARK(IMDB_113_DEFAULT_Q034A);

// Q10
DUCKDB_BENCHMARK(IMDB_113_DEFAULT_Q035, "[imdb_default]")
IMDB_QUERY_BODY(35, false, "EMPTY");
FINISH_BENCHMARK(IMDB_113_DEFAULT_Q035);

DUCKDB_BENCHMARK(IMDB_113_DEFAULT_Q035A, "[imdb_default]")
IMDB_QUERY_BODY(35, true, "EMPTY");
FINISH_BENCHMARK(IMDB_113_DEFAULT_Q035A);

DUCKDB_BENCHMARK(IMDB_113_DEFAULT_Q036, "[imdb_default]")
IMDB_QUERY_BODY(36, false, "EMPTY");
FINISH_BENCHMARK(IMDB_113_DEFAULT_Q036);

DUCKDB_BENCHMARK(IMDB_113_DEFAULT_Q036A, "[imdb_default]")
IMDB_QUERY_BODY(36, true, "EMPTY");
FINISH_BENCHMARK(IMDB_113_DEFAULT_Q036A);

DUCKDB_BENCHMARK(IMDB_113_DEFAULT_Q037, "[imdb_default]")
IMDB_QUERY_BODY(37, false, "EMPTY");
FINISH_BENCHMARK(IMDB_113_DEFAULT_Q037);

DUCKDB_BENCHMARK(IMDB_113_DEFAULT_Q037A, "[imdb_default]")
IMDB_QUERY_BODY(37, true, "EMPTY");
FINISH_BENCHMARK(IMDB_113_DEFAULT_Q037A);

// Q11
DUCKDB_BENCHMARK(IMDB_113_DEFAULT_Q038, "[imdb_default]")
IMDB_QUERY_BODY(38, false, "EMPTY");
FINISH_BENCHMARK(IMDB_113_DEFAULT_Q038);

DUCKDB_BENCHMARK(IMDB_113_DEFAULT_Q038A, "[imdb_default]")
IMDB_QUERY_BODY(38, true, "EMPTY");
FINISH_BENCHMARK(IMDB_113_DEFAULT_Q038A);

DUCKDB_BENCHMARK(IMDB_113_DEFAULT_Q039, "[imdb_default]")
IMDB_QUERY_BODY(39, false, "EMPTY");
FINISH_BENCHMARK(IMDB_113_DEFAULT_Q039);

DUCKDB_BENCHMARK(IMDB_113_DEFAULT_Q039A, "[imdb_default]")
IMDB_QUERY_BODY(39, true, "EMPTY");
FINISH_BENCHMARK(IMDB_113_DEFAULT_Q039A);

DUCKDB_BENCHMARK(IMDB_113_DEFAULT_Q040, "[imdb_default]")
IMDB_QUERY_BODY(40, false, "EMPTY");
FINISH_BENCHMARK(IMDB_113_DEFAULT_Q040);

DUCKDB_BENCHMARK(IMDB_113_DEFAULT_Q040A, "[imdb_default]")
IMDB_QUERY_BODY(40, true, "EMPTY");
FINISH_BENCHMARK(IMDB_113_DEFAULT_Q040A);

DUCKDB_BENCHMARK(IMDB_113_DEFAULT_Q041, "[imdb_default]")
IMDB_QUERY_BODY(41, false, "EMPTY");
FINISH_BENCHMARK(IMDB_113_DEFAULT_Q041);

DUCKDB_BENCHMARK(IMDB_113_DEFAULT_Q041A, "[imdb_default]")
IMDB_QUERY_BODY(41, true, "EMPTY");
FINISH_BENCHMARK(IMDB_113_DEFAULT_Q041A);

// Q12
DUCKDB_BENCHMARK(IMDB_113_DEFAULT_Q042, "[imdb_default]")
IMDB_QUERY_BODY(42, false, "EMPTY");
FINISH_BENCHMARK(IMDB_113_DEFAULT_Q042);

DUCKDB_BENCHMARK(IMDB_113_DEFAULT_Q042A, "[imdb_default]")
IMDB_QUERY_BODY(42, true, "EMPTY");
FINISH_BENCHMARK(IMDB_113_DEFAULT_Q042A);

DUCKDB_BENCHMARK(IMDB_113_DEFAULT_Q043, "[imdb_default]")
IMDB_QUERY_BODY(43, false, "EMPTY");
FINISH_BENCHMARK(IMDB_113_DEFAULT_Q043);

DUCKDB_BENCHMARK(IMDB_113_DEFAULT_Q043A, "[imdb_default]")
IMDB_QUERY_BODY(43, true, "EMPTY");
FINISH_BENCHMARK(IMDB_113_DEFAULT_Q043A);

DUCKDB_BENCHMARK(IMDB_113_DEFAULT_Q044, "[imdb_default]")
IMDB_QUERY_BODY(44, false, "EMPTY");
FINISH_BENCHMARK(IMDB_113_DEFAULT_Q044);

DUCKDB_BENCHMARK(IMDB_113_DEFAULT_Q044A, "[imdb_default]")
IMDB_QUERY_BODY(44, true, "EMPTY");
FINISH_BENCHMARK(IMDB_113_DEFAULT_Q044A);

// Q13
DUCKDB_BENCHMARK(IMDB_113_DEFAULT_Q045, "[imdb_default]")
IMDB_QUERY_BODY(45, false, "EMPTY");
FINISH_BENCHMARK(IMDB_113_DEFAULT_Q045);

DUCKDB_BENCHMARK(IMDB_113_DEFAULT_Q045A, "[imdb_default]")
IMDB_QUERY_BODY(45, true, "EMPTY");
FINISH_BENCHMARK(IMDB_113_DEFAULT_Q045A);

DUCKDB_BENCHMARK(IMDB_113_DEFAULT_Q046, "[imdb_default]")
IMDB_QUERY_BODY(46, false, "EMPTY");
FINISH_BENCHMARK(IMDB_113_DEFAULT_Q046);

DUCKDB_BENCHMARK(IMDB_113_DEFAULT_Q046A, "[imdb_default]")
IMDB_QUERY_BODY(46, true, "EMPTY");
FINISH_BENCHMARK(IMDB_113_DEFAULT_Q046A);

DUCKDB_BENCHMARK(IMDB_113_DEFAULT_Q047, "[imdb_default]")
IMDB_QUERY_BODY(47, false, "EMPTY");
FINISH_BENCHMARK(IMDB_113_DEFAULT_Q047);

DUCKDB_BENCHMARK(IMDB_113_DEFAULT_Q047A, "[imdb_default]")
IMDB_QUERY_BODY(47, true, "EMPTY");
FINISH_BENCHMARK(IMDB_113_DEFAULT_Q047A);

DUCKDB_BENCHMARK(IMDB_113_DEFAULT_Q048, "[imdb_default]")
IMDB_QUERY_BODY(48, false, "EMPTY");
FINISH_BENCHMARK(IMDB_113_DEFAULT_Q048);

DUCKDB_BENCHMARK(IMDB_113_DEFAULT_Q048A, "[imdb_default]")
IMDB_QUERY_BODY(48, true, "EMPTY");
FINISH_BENCHMARK(IMDB_113_DEFAULT_Q048A);

// Q14
DUCKDB_BENCHMARK(IMDB_113_DEFAULT_Q049, "[imdb_default]")
IMDB_QUERY_BODY(49, false, "EMPTY");
FINISH_BENCHMARK(IMDB_113_DEFAULT_Q049);

DUCKDB_BENCHMARK(IMDB_113_DEFAULT_Q049A, "[imdb_default]")
IMDB_QUERY_BODY(49, true, "EMPTY");
FINISH_BENCHMARK(IMDB_113_DEFAULT_Q049A);

DUCKDB_BENCHMARK(IMDB_113_DEFAULT_Q050, "[imdb_default]")
IMDB_QUERY_BODY(50, false, "EMPTY");
FINISH_BENCHMARK(IMDB_113_DEFAULT_Q050);

DUCKDB_BENCHMARK(IMDB_113_DEFAULT_Q050A, "[imdb_default]")
IMDB_QUERY_BODY(50, true, "EMPTY");
FINISH_BENCHMARK(IMDB_113_DEFAULT_Q050A);

DUCKDB_BENCHMARK(IMDB_113_DEFAULT_Q051, "[imdb_default]")
IMDB_QUERY_BODY(51, false, "EMPTY");
FINISH_BENCHMARK(IMDB_113_DEFAULT_Q051);

DUCKDB_BENCHMARK(IMDB_113_DEFAULT_Q051A, "[imdb_default]")
IMDB_QUERY_BODY(51, true, "EMPTY");
FINISH_BENCHMARK(IMDB_113_DEFAULT_Q051A);

// Q15
DUCKDB_BENCHMARK(IMDB_113_DEFAULT_Q052, "[imdb_default]")
IMDB_QUERY_BODY(52, false, "EMPTY");
FINISH_BENCHMARK(IMDB_113_DEFAULT_Q052);

DUCKDB_BENCHMARK(IMDB_113_DEFAULT_Q052A, "[imdb_default]")
IMDB_QUERY_BODY(52, true, "EMPTY");
FINISH_BENCHMARK(IMDB_113_DEFAULT_Q052A);

DUCKDB_BENCHMARK(IMDB_113_DEFAULT_Q053, "[imdb_default]")
IMDB_QUERY_BODY(53, false, "EMPTY");
FINISH_BENCHMARK(IMDB_113_DEFAULT_Q053);

DUCKDB_BENCHMARK(IMDB_113_DEFAULT_Q053A, "[imdb_default]")
IMDB_QUERY_BODY(53, true, "EMPTY");
FINISH_BENCHMARK(IMDB_113_DEFAULT_Q053A);

DUCKDB_BENCHMARK(IMDB_113_DEFAULT_Q054, "[imdb_default]")
IMDB_QUERY_BODY(54, false, "EMPTY");
FINISH_BENCHMARK(IMDB_113_DEFAULT_Q054);

DUCKDB_BENCHMARK(IMDB_113_DEFAULT_Q054A, "[imdb_default]")
IMDB_QUERY_BODY(54, true, "EMPTY");
FINISH_BENCHMARK(IMDB_113_DEFAULT_Q054A);

DUCKDB_BENCHMARK(IMDB_113_DEFAULT_Q055, "[imdb_default]")
IMDB_QUERY_BODY(55, false, "EMPTY");
FINISH_BENCHMARK(IMDB_113_DEFAULT_Q055);

DUCKDB_BENCHMARK(IMDB_113_DEFAULT_Q055A, "[imdb_default]")
IMDB_QUERY_BODY(55, true, "EMPTY");
FINISH_BENCHMARK(IMDB_113_DEFAULT_Q055A);

// Q16
DUCKDB_BENCHMARK(IMDB_113_DEFAULT_Q056, "[imdb_default]")
IMDB_QUERY_BODY(56, false, "EMPTY");
FINISH_BENCHMARK(IMDB_113_DEFAULT_Q056);

DUCKDB_BENCHMARK(IMDB_113_DEFAULT_Q056A, "[imdb_default]")
IMDB_QUERY_BODY(56, true, "EMPTY");
FINISH_BENCHMARK(IMDB_113_DEFAULT_Q056A);

DUCKDB_BENCHMARK(IMDB_113_DEFAULT_Q057, "[imdb_default]")
IMDB_QUERY_BODY(57, false, "EMPTY");
FINISH_BENCHMARK(IMDB_113_DEFAULT_Q057);

DUCKDB_BENCHMARK(IMDB_113_DEFAULT_Q057A, "[imdb_default]")
IMDB_QUERY_BODY(57, true, "EMPTY");
FINISH_BENCHMARK(IMDB_113_DEFAULT_Q057A);

DUCKDB_BENCHMARK(IMDB_113_DEFAULT_Q058, "[imdb_default]")
IMDB_QUERY_BODY(58, false, "EMPTY");
FINISH_BENCHMARK(IMDB_113_DEFAULT_Q058);

DUCKDB_BENCHMARK(IMDB_113_DEFAULT_Q058A, "[imdb_default]")
IMDB_QUERY_BODY(58, true, "EMPTY");
FINISH_BENCHMARK(IMDB_113_DEFAULT_Q058A);

DUCKDB_BENCHMARK(IMDB_113_DEFAULT_Q059, "[imdb_default]")
IMDB_QUERY_BODY(59, false, "EMPTY");
FINISH_BENCHMARK(IMDB_113_DEFAULT_Q059);

DUCKDB_BENCHMARK(IMDB_113_DEFAULT_Q059A, "[imdb_default]")
IMDB_QUERY_BODY(59, true, "EMPTY");
FINISH_BENCHMARK(IMDB_113_DEFAULT_Q059A);

// Q17
DUCKDB_BENCHMARK(IMDB_113_DEFAULT_Q060, "[imdb_default]")
IMDB_QUERY_BODY(60, false, "EMPTY");
FINISH_BENCHMARK(IMDB_113_DEFAULT_Q060);

DUCKDB_BENCHMARK(IMDB_113_DEFAULT_Q060A, "[imdb_default]")
IMDB_QUERY_BODY(60, true, "EMPTY");
FINISH_BENCHMARK(IMDB_113_DEFAULT_Q060A);

DUCKDB_BENCHMARK(IMDB_113_DEFAULT_Q061, "[imdb_default]")
IMDB_QUERY_BODY(61, false, "EMPTY");
FINISH_BENCHMARK(IMDB_113_DEFAULT_Q061);

DUCKDB_BENCHMARK(IMDB_113_DEFAULT_Q061A, "[imdb_default]")
IMDB_QUERY_BODY(61, true, "EMPTY");
FINISH_BENCHMARK(IMDB_113_DEFAULT_Q061A);

DUCKDB_BENCHMARK(IMDB_113_DEFAULT_Q062, "[imdb_default]")
IMDB_QUERY_BODY(62, false, "EMPTY");
FINISH_BENCHMARK(IMDB_113_DEFAULT_Q062);

DUCKDB_BENCHMARK(IMDB_113_DEFAULT_Q062A, "[imdb_default]")
IMDB_QUERY_BODY(62, true, "EMPTY");
FINISH_BENCHMARK(IMDB_113_DEFAULT_Q062A);

DUCKDB_BENCHMARK(IMDB_113_DEFAULT_Q063, "[imdb_default]")
IMDB_QUERY_BODY(63, false, "EMPTY");
FINISH_BENCHMARK(IMDB_113_DEFAULT_Q063);

DUCKDB_BENCHMARK(IMDB_113_DEFAULT_Q063A, "[imdb_default]")
IMDB_QUERY_BODY(63, true, "EMPTY");
FINISH_BENCHMARK(IMDB_113_DEFAULT_Q063A);

DUCKDB_BENCHMARK(IMDB_113_DEFAULT_Q064, "[imdb_default]")
IMDB_QUERY_BODY(64, false, "EMPTY");
FINISH_BENCHMARK(IMDB_113_DEFAULT_Q064);

DUCKDB_BENCHMARK(IMDB_113_DEFAULT_Q064A, "[imdb_default]")
IMDB_QUERY_BODY(64, true, "EMPTY");
FINISH_BENCHMARK(IMDB_113_DEFAULT_Q064A);

DUCKDB_BENCHMARK(IMDB_113_DEFAULT_Q065, "[imdb_default]")
IMDB_QUERY_BODY(65, false, "EMPTY");
FINISH_BENCHMARK(IMDB_113_DEFAULT_Q065);

DUCKDB_BENCHMARK(IMDB_113_DEFAULT_Q065A, "[imdb_default]")
IMDB_QUERY_BODY(65, true, "EMPTY");
FINISH_BENCHMARK(IMDB_113_DEFAULT_Q065A);

// Q18
DUCKDB_BENCHMARK(IMDB_113_DEFAULT_Q066, "[imdb_default]")
IMDB_QUERY_BODY(66, false, "EMPTY");
FINISH_BENCHMARK(IMDB_113_DEFAULT_Q066);

DUCKDB_BENCHMARK(IMDB_113_DEFAULT_Q066A, "[imdb_default]")
IMDB_QUERY_BODY(66, true, "EMPTY");
FINISH_BENCHMARK(IMDB_113_DEFAULT_Q066A);

DUCKDB_BENCHMARK(IMDB_113_DEFAULT_Q067, "[imdb_default]")
IMDB_QUERY_BODY(67, false, "EMPTY");
FINISH_BENCHMARK(IMDB_113_DEFAULT_Q067);

DUCKDB_BENCHMARK(IMDB_113_DEFAULT_Q067A, "[imdb_default]")
IMDB_QUERY_BODY(67, true, "EMPTY");
FINISH_BENCHMARK(IMDB_113_DEFAULT_Q067A);

DUCKDB_BENCHMARK(IMDB_113_DEFAULT_Q068, "[imdb_default]")
IMDB_QUERY_BODY(68, false, "EMPTY");
FINISH_BENCHMARK(IMDB_113_DEFAULT_Q068);

DUCKDB_BENCHMARK(IMDB_113_DEFAULT_Q068A, "[imdb_default]")
IMDB_QUERY_BODY(68, true, "EMPTY");
FINISH_BENCHMARK(IMDB_113_DEFAULT_Q068A);

// Q19
DUCKDB_BENCHMARK(IMDB_113_DEFAULT_Q069, "[imdb_default]")
IMDB_QUERY_BODY(69, false, "EMPTY");
FINISH_BENCHMARK(IMDB_113_DEFAULT_Q069);

DUCKDB_BENCHMARK(IMDB_113_DEFAULT_Q069A, "[imdb_default]")
IMDB_QUERY_BODY(69, true, "EMPTY");
FINISH_BENCHMARK(IMDB_113_DEFAULT_Q069A);

DUCKDB_BENCHMARK(IMDB_113_DEFAULT_Q070, "[imdb_default]")
IMDB_QUERY_BODY(70, false, "EMPTY");
FINISH_BENCHMARK(IMDB_113_DEFAULT_Q070);

DUCKDB_BENCHMARK(IMDB_113_DEFAULT_Q070A, "[imdb_default]")
IMDB_QUERY_BODY(70, true, "EMPTY");
FINISH_BENCHMARK(IMDB_113_DEFAULT_Q070A);

DUCKDB_BENCHMARK(IMDB_113_DEFAULT_Q071, "[imdb_default]")
IMDB_QUERY_BODY(71, false, "EMPTY");
FINISH_BENCHMARK(IMDB_113_DEFAULT_Q071);

DUCKDB_BENCHMARK(IMDB_113_DEFAULT_Q071A, "[imdb_default]")
IMDB_QUERY_BODY(71, true, "EMPTY");
FINISH_BENCHMARK(IMDB_113_DEFAULT_Q071A);

DUCKDB_BENCHMARK(IMDB_113_DEFAULT_Q072, "[imdb_default]")
IMDB_QUERY_BODY(72, false, "EMPTY");
FINISH_BENCHMARK(IMDB_113_DEFAULT_Q072);

DUCKDB_BENCHMARK(IMDB_113_DEFAULT_Q072A, "[imdb_default]")
IMDB_QUERY_BODY(72, true, "EMPTY");
FINISH_BENCHMARK(IMDB_113_DEFAULT_Q072A);

// Q20
DUCKDB_BENCHMARK(IMDB_113_DEFAULT_Q073, "[imdb_default]")
IMDB_QUERY_BODY(73, false, "EMPTY");
FINISH_BENCHMARK(IMDB_113_DEFAULT_Q073);

DUCKDB_BENCHMARK(IMDB_113_DEFAULT_Q073A, "[imdb_default]")
IMDB_QUERY_BODY(73, true, "EMPTY");
FINISH_BENCHMARK(IMDB_113_DEFAULT_Q073A);

DUCKDB_BENCHMARK(IMDB_113_DEFAULT_Q074, "[imdb_default]")
IMDB_QUERY_BODY(74, false, "EMPTY");
FINISH_BENCHMARK(IMDB_113_DEFAULT_Q074);

DUCKDB_BENCHMARK(IMDB_113_DEFAULT_Q074A, "[imdb_default]")
IMDB_QUERY_BODY(74, true, "EMPTY");
FINISH_BENCHMARK(IMDB_113_DEFAULT_Q074A);

DUCKDB_BENCHMARK(IMDB_113_DEFAULT_Q075, "[imdb_default]")
IMDB_QUERY_BODY(75, false, "EMPTY");
FINISH_BENCHMARK(IMDB_113_DEFAULT_Q075);

DUCKDB_BENCHMARK(IMDB_113_DEFAULT_Q075A, "[imdb_default]")
IMDB_QUERY_BODY(75, true, "EMPTY");
FINISH_BENCHMARK(IMDB_113_DEFAULT_Q075A);

// Q21
DUCKDB_BENCHMARK(IMDB_113_DEFAULT_Q076, "[imdb_default]")
IMDB_QUERY_BODY(76, false, "EMPTY");
FINISH_BENCHMARK(IMDB_113_DEFAULT_Q076);

DUCKDB_BENCHMARK(IMDB_113_DEFAULT_Q076A, "[imdb_default]")
IMDB_QUERY_BODY(76, true, "EMPTY");
FINISH_BENCHMARK(IMDB_113_DEFAULT_Q076A);

DUCKDB_BENCHMARK(IMDB_113_DEFAULT_Q077, "[imdb_default]")
IMDB_QUERY_BODY(77, false, "EMPTY");
FINISH_BENCHMARK(IMDB_113_DEFAULT_Q077);

DUCKDB_BENCHMARK(IMDB_113_DEFAULT_Q077A, "[imdb_default]")
IMDB_QUERY_BODY(77, true, "EMPTY");
FINISH_BENCHMARK(IMDB_113_DEFAULT_Q077A);

DUCKDB_BENCHMARK(IMDB_113_DEFAULT_Q078, "[imdb_default]")
IMDB_QUERY_BODY(78, false, "EMPTY");
FINISH_BENCHMARK(IMDB_113_DEFAULT_Q078);

DUCKDB_BENCHMARK(IMDB_113_DEFAULT_Q078A, "[imdb_default]")
IMDB_QUERY_BODY(78, true, "EMPTY");
FINISH_BENCHMARK(IMDB_113_DEFAULT_Q078A);

// Q22
DUCKDB_BENCHMARK(IMDB_113_DEFAULT_Q079, "[imdb_default]")
IMDB_QUERY_BODY(79, false, "EMPTY");
FINISH_BENCHMARK(IMDB_113_DEFAULT_Q079);

DUCKDB_BENCHMARK(IMDB_113_DEFAULT_Q079A, "[imdb_default]")
IMDB_QUERY_BODY(79, true, "EMPTY");
FINISH_BENCHMARK(IMDB_113_DEFAULT_Q079A);

DUCKDB_BENCHMARK(IMDB_113_DEFAULT_Q080, "[imdb_default]")
IMDB_QUERY_BODY(80, false, "EMPTY");
FINISH_BENCHMARK(IMDB_113_DEFAULT_Q080);

DUCKDB_BENCHMARK(IMDB_113_DEFAULT_Q080A, "[imdb_default]")
IMDB_QUERY_BODY(80, true, "EMPTY");
FINISH_BENCHMARK(IMDB_113_DEFAULT_Q080A);

DUCKDB_BENCHMARK(IMDB_113_DEFAULT_Q081, "[imdb_default]")
IMDB_QUERY_BODY(81, false, "EMPTY");
FINISH_BENCHMARK(IMDB_113_DEFAULT_Q081);

DUCKDB_BENCHMARK(IMDB_113_DEFAULT_Q081A, "[imdb_default]")
IMDB_QUERY_BODY(81, true, "EMPTY");
FINISH_BENCHMARK(IMDB_113_DEFAULT_Q081A);

DUCKDB_BENCHMARK(IMDB_113_DEFAULT_Q082, "[imdb_default]")
IMDB_QUERY_BODY(82, false, "EMPTY");
FINISH_BENCHMARK(IMDB_113_DEFAULT_Q082);

DUCKDB_BENCHMARK(IMDB_113_DEFAULT_Q082A, "[imdb_default]")
IMDB_QUERY_BODY(82, true, "EMPTY");
FINISH_BENCHMARK(IMDB_113_DEFAULT_Q082A);

// Q23
DUCKDB_BENCHMARK(IMDB_113_DEFAULT_Q083, "[imdb_default]")
IMDB_QUERY_BODY(83, false, "EMPTY");
FINISH_BENCHMARK(IMDB_113_DEFAULT_Q083);

DUCKDB_BENCHMARK(IMDB_113_DEFAULT_Q083A, "[imdb_default]")
IMDB_QUERY_BODY(83, true, "EMPTY");
FINISH_BENCHMARK(IMDB_113_DEFAULT_Q083A);

DUCKDB_BENCHMARK(IMDB_113_DEFAULT_Q084, "[imdb_default]")
IMDB_QUERY_BODY(84, false, "EMPTY");
FINISH_BENCHMARK(IMDB_113_DEFAULT_Q084);

DUCKDB_BENCHMARK(IMDB_113_DEFAULT_Q084A, "[imdb_default]")
IMDB_QUERY_BODY(84, true, "EMPTY");
FINISH_BENCHMARK(IMDB_113_DEFAULT_Q084A);

DUCKDB_BENCHMARK(IMDB_113_DEFAULT_Q085, "[imdb_default]")
IMDB_QUERY_BODY(85, false, "EMPTY");
FINISH_BENCHMARK(IMDB_113_DEFAULT_Q085);

DUCKDB_BENCHMARK(IMDB_113_DEFAULT_Q085A, "[imdb_default]")
IMDB_QUERY_BODY(85, true, "EMPTY");
FINISH_BENCHMARK(IMDB_113_DEFAULT_Q085A);

// Q24
DUCKDB_BENCHMARK(IMDB_113_DEFAULT_Q086, "[imdb_default]")
IMDB_QUERY_BODY(86, false, "EMPTY");
FINISH_BENCHMARK(IMDB_113_DEFAULT_Q086);

DUCKDB_BENCHMARK(IMDB_113_DEFAULT_Q086A, "[imdb_default]")
IMDB_QUERY_BODY(86, true, "EMPTY");
FINISH_BENCHMARK(IMDB_113_DEFAULT_Q086A);

DUCKDB_BENCHMARK(IMDB_113_DEFAULT_Q087, "[imdb_default]")
IMDB_QUERY_BODY(87, false, "EMPTY");
FINISH_BENCHMARK(IMDB_113_DEFAULT_Q087);

DUCKDB_BENCHMARK(IMDB_113_DEFAULT_Q087A, "[imdb_default]")
IMDB_QUERY_BODY(87, true, "EMPTY");
FINISH_BENCHMARK(IMDB_113_DEFAULT_Q087A);

// Q25
DUCKDB_BENCHMARK(IMDB_113_DEFAULT_Q088, "[imdb_default]")
IMDB_QUERY_BODY(88, false, "EMPTY");
FINISH_BENCHMARK(IMDB_113_DEFAULT_Q088);

DUCKDB_BENCHMARK(IMDB_113_DEFAULT_Q088A, "[imdb_default]")
IMDB_QUERY_BODY(88, true, "EMPTY");
FINISH_BENCHMARK(IMDB_113_DEFAULT_Q088A);

DUCKDB_BENCHMARK(IMDB_113_DEFAULT_Q089, "[imdb_default]")
IMDB_QUERY_BODY(89, false, "EMPTY");
FINISH_BENCHMARK(IMDB_113_DEFAULT_Q089);

DUCKDB_BENCHMARK(IMDB_113_DEFAULT_Q089A, "[imdb_default]")
IMDB_QUERY_BODY(89, true, "EMPTY");
FINISH_BENCHMARK(IMDB_113_DEFAULT_Q089A);

DUCKDB_BENCHMARK(IMDB_113_DEFAULT_Q090, "[imdb_default]")
IMDB_QUERY_BODY(90, false, "EMPTY");
FINISH_BENCHMARK(IMDB_113_DEFAULT_Q090);

DUCKDB_BENCHMARK(IMDB_113_DEFAULT_Q090A, "[imdb_default]")
IMDB_QUERY_BODY(90, true, "EMPTY");
FINISH_BENCHMARK(IMDB_113_DEFAULT_Q090A);

// Q26
DUCKDB_BENCHMARK(IMDB_113_DEFAULT_Q091, "[imdb_default]")
IMDB_QUERY_BODY(91, false, "EMPTY");
FINISH_BENCHMARK(IMDB_113_DEFAULT_Q091);

DUCKDB_BENCHMARK(IMDB_113_DEFAULT_Q091A, "[imdb_default]")
IMDB_QUERY_BODY(91, true, "EMPTY");
FINISH_BENCHMARK(IMDB_113_DEFAULT_Q091A);

DUCKDB_BENCHMARK(IMDB_113_DEFAULT_Q092, "[imdb_default]")
IMDB_QUERY_BODY(92, false, "EMPTY");
FINISH_BENCHMARK(IMDB_113_DEFAULT_Q092);

DUCKDB_BENCHMARK(IMDB_113_DEFAULT_Q092A, "[imdb_default]")
IMDB_QUERY_BODY(92, true, "EMPTY");
FINISH_BENCHMARK(IMDB_113_DEFAULT_Q092A);

DUCKDB_BENCHMARK(IMDB_113_DEFAULT_Q093, "[imdb_default]")
IMDB_QUERY_BODY(93, false, "EMPTY");
FINISH_BENCHMARK(IMDB_113_DEFAULT_Q093);

DUCKDB_BENCHMARK(IMDB_113_DEFAULT_Q093A, "[imdb_default]")
IMDB_QUERY_BODY(93, true, "EMPTY");
FINISH_BENCHMARK(IMDB_113_DEFAULT_Q093A);

// Q27
DUCKDB_BENCHMARK(IMDB_113_DEFAULT_Q094, "[imdb_default]")
IMDB_QUERY_BODY(94, false, "EMPTY");
FINISH_BENCHMARK(IMDB_113_DEFAULT_Q094);

DUCKDB_BENCHMARK(IMDB_113_DEFAULT_Q094A, "[imdb_default]")
IMDB_QUERY_BODY(94, true, "EMPTY");
FINISH_BENCHMARK(IMDB_113_DEFAULT_Q094A);

DUCKDB_BENCHMARK(IMDB_113_DEFAULT_Q095, "[imdb_default]")
IMDB_QUERY_BODY(95, false, "EMPTY");
FINISH_BENCHMARK(IMDB_113_DEFAULT_Q095);

DUCKDB_BENCHMARK(IMDB_113_DEFAULT_Q095A, "[imdb_default]")
IMDB_QUERY_BODY(95, true, "EMPTY");
FINISH_BENCHMARK(IMDB_113_DEFAULT_Q095A);

DUCKDB_BENCHMARK(IMDB_113_DEFAULT_Q096, "[imdb_default]")
IMDB_QUERY_BODY(96, false, "EMPTY");
FINISH_BENCHMARK(IMDB_113_DEFAULT_Q096);

DUCKDB_BENCHMARK(IMDB_113_DEFAULT_Q096A, "[imdb_default]")
IMDB_QUERY_BODY(96, true, "EMPTY");
FINISH_BENCHMARK(IMDB_113_DEFAULT_Q096A);

// Q28
DUCKDB_BENCHMARK(IMDB_113_DEFAULT_Q097, "[imdb_default]")
IMDB_QUERY_BODY(97, false, "EMPTY");
FINISH_BENCHMARK(IMDB_113_DEFAULT_Q097);

DUCKDB_BENCHMARK(IMDB_113_DEFAULT_Q097A, "[imdb_default]")
IMDB_QUERY_BODY(97, true, "EMPTY");
FINISH_BENCHMARK(IMDB_113_DEFAULT_Q097A);

DUCKDB_BENCHMARK(IMDB_113_DEFAULT_Q098, "[imdb_default]")
IMDB_QUERY_BODY(98, false, "EMPTY");
FINISH_BENCHMARK(IMDB_113_DEFAULT_Q098);

DUCKDB_BENCHMARK(IMDB_113_DEFAULT_Q098A, "[imdb_default]")
IMDB_QUERY_BODY(98, true, "EMPTY");
FINISH_BENCHMARK(IMDB_113_DEFAULT_Q098A);

DUCKDB_BENCHMARK(IMDB_113_DEFAULT_Q099, "[imdb_default]")
IMDB_QUERY_BODY(99, false, "EMPTY");
FINISH_BENCHMARK(IMDB_113_DEFAULT_Q099);

DUCKDB_BENCHMARK(IMDB_113_DEFAULT_Q099A, "[imdb_default]")
IMDB_QUERY_BODY(99, true, "EMPTY");
FINISH_BENCHMARK(IMDB_113_DEFAULT_Q099A);

// Q29
DUCKDB_BENCHMARK(IMDB_113_DEFAULT_Q100, "[imdb_default]")
IMDB_QUERY_BODY(100, false, "EMPTY");
FINISH_BENCHMARK(IMDB_113_DEFAULT_Q100);

DUCKDB_BENCHMARK(IMDB_113_DEFAULT_Q100A, "[imdb_default]")
IMDB_QUERY_BODY(100, true, "EMPTY");
FINISH_BENCHMARK(IMDB_113_DEFAULT_Q100A);

DUCKDB_BENCHMARK(IMDB_113_DEFAULT_Q101, "[imdb_default]")
IMDB_QUERY_BODY(101, false, "EMPTY");
FINISH_BENCHMARK(IMDB_113_DEFAULT_Q101);

DUCKDB_BENCHMARK(IMDB_113_DEFAULT_Q101A, "[imdb_default]")
IMDB_QUERY_BODY(101, true, "EMPTY");
FINISH_BENCHMARK(IMDB_113_DEFAULT_Q101A);

DUCKDB_BENCHMARK(IMDB_113_DEFAULT_Q102, "[imdb_default]")
IMDB_QUERY_BODY(102, false, "EMPTY");
FINISH_BENCHMARK(IMDB_113_DEFAULT_Q102);

DUCKDB_BENCHMARK(IMDB_113_DEFAULT_Q102A, "[imdb_default]")
IMDB_QUERY_BODY(102, true, "EMPTY");
FINISH_BENCHMARK(IMDB_113_DEFAULT_Q102A);

// Q30
DUCKDB_BENCHMARK(IMDB_113_DEFAULT_Q103, "[imdb_default]")
IMDB_QUERY_BODY(103, false, "EMPTY");
FINISH_BENCHMARK(IMDB_113_DEFAULT_Q103);

DUCKDB_BENCHMARK(IMDB_113_DEFAULT_Q103A, "[imdb_default]")
IMDB_QUERY_BODY(103, true, "EMPTY");
FINISH_BENCHMARK(IMDB_113_DEFAULT_Q103A);

DUCKDB_BENCHMARK(IMDB_113_DEFAULT_Q104, "[imdb_default]")
IMDB_QUERY_BODY(104, false, "EMPTY");
FINISH_BENCHMARK(IMDB_113_DEFAULT_Q104);

DUCKDB_BENCHMARK(IMDB_113_DEFAULT_Q104A, "[imdb_default]")
IMDB_QUERY_BODY(104, true, "EMPTY");
FINISH_BENCHMARK(IMDB_113_DEFAULT_Q104A);

DUCKDB_BENCHMARK(IMDB_113_DEFAULT_Q105, "[imdb_default]")
IMDB_QUERY_BODY(105, false, "EMPTY");
FINISH_BENCHMARK(IMDB_113_DEFAULT_Q105);

DUCKDB_BENCHMARK(IMDB_113_DEFAULT_Q105A, "[imdb_default]")
IMDB_QUERY_BODY(105, true, "EMPTY");
FINISH_BENCHMARK(IMDB_113_DEFAULT_Q105A);

// Q31
DUCKDB_BENCHMARK(IMDB_113_DEFAULT_Q106, "[imdb_default]")
IMDB_QUERY_BODY(106, false, "EMPTY");
FINISH_BENCHMARK(IMDB_113_DEFAULT_Q106);

DUCKDB_BENCHMARK(IMDB_113_DEFAULT_Q106A, "[imdb_default]")
IMDB_QUERY_BODY(106, true, "EMPTY");
FINISH_BENCHMARK(IMDB_113_DEFAULT_Q106A);

DUCKDB_BENCHMARK(IMDB_113_DEFAULT_Q107, "[imdb_default]")
IMDB_QUERY_BODY(107, false, "EMPTY");
FINISH_BENCHMARK(IMDB_113_DEFAULT_Q107);

DUCKDB_BENCHMARK(IMDB_113_DEFAULT_Q107A, "[imdb_default]")
IMDB_QUERY_BODY(107, true, "EMPTY");
FINISH_BENCHMARK(IMDB_113_DEFAULT_Q107A);

DUCKDB_BENCHMARK(IMDB_113_DEFAULT_Q108, "[imdb_default]")
IMDB_QUERY_BODY(108, false, "EMPTY");
FINISH_BENCHMARK(IMDB_113_DEFAULT_Q108);

DUCKDB_BENCHMARK(IMDB_113_DEFAULT_Q108A, "[imdb_default]")
IMDB_QUERY_BODY(108, true, "EMPTY");
FINISH_BENCHMARK(IMDB_113_DEFAULT_Q108A);

// Q32
DUCKDB_BENCHMARK(IMDB_113_DEFAULT_Q109, "[imdb_default]")
IMDB_QUERY_BODY(109, false, "EMPTY");
FINISH_BENCHMARK(IMDB_113_DEFAULT_Q109);

DUCKDB_BENCHMARK(IMDB_113_DEFAULT_Q109A, "[imdb_default]")
IMDB_QUERY_BODY(109, true, "EMPTY");
FINISH_BENCHMARK(IMDB_113_DEFAULT_Q109A);

DUCKDB_BENCHMARK(IMDB_113_DEFAULT_Q110, "[imdb_default]")
IMDB_QUERY_BODY(110, false, "EMPTY");
FINISH_BENCHMARK(IMDB_113_DEFAULT_Q110);

DUCKDB_BENCHMARK(IMDB_113_DEFAULT_Q110A, "[imdb_default]")
IMDB_QUERY_BODY(110, true, "EMPTY");
FINISH_BENCHMARK(IMDB_113_DEFAULT_Q110A);

DUCKDB_BENCHMARK(IMDB_113_DEFAULT_Q111, "[imdb_default]")
IMDB_QUERY_BODY(111, false, "EMPTY");
FINISH_BENCHMARK(IMDB_113_DEFAULT_Q111);

DUCKDB_BENCHMARK(IMDB_113_DEFAULT_Q111A, "[imdb_default]")
IMDB_QUERY_BODY(111, true, "EMPTY");
FINISH_BENCHMARK(IMDB_113_DEFAULT_Q111A);

// Q33
DUCKDB_BENCHMARK(IMDB_113_DEFAULT_Q112, "[imdb_default]")
IMDB_QUERY_BODY(112, false, "EMPTY");
FINISH_BENCHMARK(IMDB_113_DEFAULT_Q112);

DUCKDB_BENCHMARK(IMDB_113_DEFAULT_Q112A, "[imdb_default]")
IMDB_QUERY_BODY(112, true, "EMPTY");
FINISH_BENCHMARK(IMDB_113_DEFAULT_Q112A);

DUCKDB_BENCHMARK(IMDB_113_DEFAULT_Q113, "[imdb_default]")
IMDB_QUERY_BODY(113, false, "EMPTY");
FINISH_BENCHMARK(IMDB_113_DEFAULT_Q113);

DUCKDB_BENCHMARK(IMDB_113_DEFAULT_Q113A, "[imdb_default]")
IMDB_QUERY_BODY(113, true, "EMPTY");
FINISH_BENCHMARK(IMDB_113_DEFAULT_Q113A);

// MICRO
// DUCKDB_BENCHMARK(IMDB_113_DEFAULT_Q114, "[imdb_default]")
// IMDB_QUERY_BODY(114, false, "EMPTY");
// FINISH_BENCHMARK(IMDB_113_DEFAULT_Q114);
//
// DUCKDB_BENCHMARK(IMDB_113_DEFAULT_Q114A, "[imdb_default]")
// IMDB_QUERY_BODY(114, true, "EMPTY");
// FINISH_BENCHMARK(IMDB_113_DEFAULT_Q114A);
//
//// micro
// DUCKDB_BENCHMARK(IMDB_113_DEFAULT_Q115, "[imdb_default]")
// IMDB_QUERY_BODY(115, false, "EMPTY");
// FINISH_BENCHMARK(IMDB_113_DEFAULT_Q115);
//
// DUCKDB_BENCHMARK(IMDB_113_DEFAULT_Q115A, "[imdb_default]")
// IMDB_QUERY_BODY(115, true, "EMPTY");
// FINISH_BENCHMARK(IMDB_113_DEFAULT_Q115A);
//
// DUCKDB_BENCHMARK(IMDB_113_DEFAULT_Q116, "[imdb_default]")
// IMDB_QUERY_BODY(116, false, "EMPTY");
// FINISH_BENCHMARK(IMDB_113_DEFAULT_Q116);
//
// DUCKDB_BENCHMARK(IMDB_113_DEFAULT_Q116A, "[imdb_default]")
// IMDB_QUERY_BODY(116, true, "EMPTY");
// FINISH_BENCHMARK(IMDB_113_DEFAULT_Q116A);
//
// DUCKDB_BENCHMARK(IMDB_113_DEFAULT_Q117, "[imdb_default]")
// IMDB_QUERY_BODY(117, false, "EMPTY");
// FINISH_BENCHMARK(IMDB_113_DEFAULT_Q117);
//
// DUCKDB_BENCHMARK(IMDB_113_DEFAULT_Q117A, "[imdb_default]")
// IMDB_QUERY_BODY(117, true, "EMPTY");
// FINISH_BENCHMARK(IMDB_113_DEFAULT_Q117A);
//
// DUCKDB_BENCHMARK(IMDB_113_DEFAULT_Q118, "[imdb_default]")
// IMDB_QUERY_BODY(118, false, "EMPTY");
// FINISH_BENCHMARK(IMDB_113_DEFAULT_Q118);
//
// DUCKDB_BENCHMARK(IMDB_113_DEFAULT_Q118A, "[imdb_default]")
// IMDB_QUERY_BODY(118, true, "EMPTY");
// FINISH_BENCHMARK(IMDB_113_DEFAULT_Q118A);
//
// DUCKDB_BENCHMARK(IMDB_113_DEFAULT_Q119, "[imdb_default]")
// IMDB_QUERY_BODY(119, false, "EMPTY");
// FINISH_BENCHMARK(IMDB_113_DEFAULT_Q119);
//
// DUCKDB_BENCHMARK(IMDB_113_DEFAULT_Q119A, "[imdb_default]")
// IMDB_QUERY_BODY(119, true, "EMPTY");
// FINISH_BENCHMARK(IMDB_113_DEFAULT_Q119A);
//
// DUCKDB_BENCHMARK(IMDB_113_DEFAULT_Q120, "[imdb_default]")
// IMDB_QUERY_BODY(120, false, "EMPTY");
// FINISH_BENCHMARK(IMDB_113_DEFAULT_Q120);
//
// DUCKDB_BENCHMARK(IMDB_113_DEFAULT_Q120A, "[imdb_default]")
// IMDB_QUERY_BODY(120, true, "EMPTY");
// FINISH_BENCHMARK(IMDB_113_DEFAULT_Q120A);
