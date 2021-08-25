#include "dbgen.hpp"
#include "duckdb.hpp"
#include "duckdb/catalog/catalog_entry/schema_catalog_entry.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/planner/planner.hpp"
#include "duckdb/storage/data_table.hpp"
#include "duckdb/transaction/transaction.hpp"
#include "imdb.hpp"
#include "ldbc.hpp"

#include <algorithm>
#include <chrono>
#include <fstream>
#include <iostream>
#include <thread>

#define NUM_RUN_TIMES 5

using namespace std;

struct QueryRunResult {
public:
	QueryRunResult(int query_id, int jos_id, bool enable_rai, idx_t num_tuples, double elapsed_time_ms)
	    : query_id(query_id), jos_id(jos_id), enable_rai(enable_rai), num_tuples(num_tuples),
	      elapsed_time_ms(elapsed_time_ms) {
	}

	int query_id;
	int jos_id;
	bool enable_rai;
	idx_t num_tuples;
	double elapsed_time_ms;
};

class SpectrumRunner {
public:
	SpectrumRunner(string jos_input, int query_id, int start_jos_id, int num_jos)
	    : jos_input(jos_input), query_id(query_id), start_jos_id(start_jos_id), num_jos(num_jos) {
	}

	void Run() {
		cout << "Start running ... params: [" << jos_input << ", " << query_id << ", " << start_jos_id << ", "
		     << num_jos << "]" << endl;
		string query = GetQuery();
		cout << "Query plan text: " << query << endl;
		vector<QueryRunResult> query_results;
		cout << "-------------------------------" << endl;
		cout << "||    QUERY " << query_id << "    ||" << endl;
		cout << "-------------------------------" << endl;
		// Run query without rai
		for (auto i = start_jos_id; i < num_jos; i++) {
			DuckDB db(nullptr);
			Connection conn(db);
			Initialize(conn, false);
			auto query_result = RunQueryWithAJos(conn, query, i, false);
			cout << query_result.jos_id << "," << (query_result.enable_rai ? "T," : "F,") << query_result.num_tuples
			     << "," << query_result.elapsed_time_ms << endl;
			query_results.push_back(query_result);
		}
		// Run query with rai
		for (auto i = start_jos_id; i < num_jos; i++) {
			DuckDB db(nullptr);
			Connection conn(db);
			Initialize(conn, true);
			auto query_result = RunQueryWithAJos(conn, query, i, true);
			cout << query_result.jos_id << "," << (query_result.enable_rai ? "T," : "F,") << query_result.num_tuples
			     << ", " << query_result.elapsed_time_ms << endl;
			query_results.push_back(query_result);
		}
		// Display query results
		// for (auto &result : query_results) {
		// }
	}

private:
	virtual void Initialize(Connection &conn_, bool enableRAI) {
	}

	virtual string GetQuery() {
		return "";
	}

	virtual string GetJos(int jos_id) {
		auto file_system = make_unique<FileSystem>();
		string jo_file;
		if (StringUtil::EndsWith(jos_input, "/")) {
			jo_file = jos_input + to_string(jos_id) + ".json";
		} else {
			jo_file = jos_input + "/" + to_string(jos_id) + ".json";
		}
		if (!file_system->FileExists(jo_file)) {
			cout << "JO file not exists!" << endl;
			return "";
		}
		ifstream ifs(jo_file);
		string jo_json((istreambuf_iterator<char>(ifs)), (istreambuf_iterator<char>()));
		return jo_json;
	}

	QueryRunResult RunQueryWithAJos(Connection &conn, string &query, int jos_id, bool enable_rai) {
		cout << "Start running query with a jos ... " << jos_id << ", " << (enable_rai ? "ENABLE_RAI" : "DISABLE_RAI")
		     << endl;
		string jos = GetJos(jos_id);
		idx_t num_tuples = 0;
		vector<double> elapsed_times;
		// cold run
		// conn.EnableProfiling();
		// conn.EnableExplicitJoinOrder(jos);
		// auto result = conn.Query(query);
		// cout << conn.GetProfilingInformation();
		// hot run
		for (int i = 0; i < NUM_RUN_TIMES; i++) {
			Profiler profiler;
			profiler.Start();
			conn.EnableExplicitJoinOrder(jos);
			auto result = conn.Query(query);
			num_tuples = result->collection.count;
			profiler.End();
			elapsed_times.push_back(profiler.Elapsed());
		}
		if (elapsed_times.size() == 1) {
			return QueryRunResult{query_id, jos_id, enable_rai, num_tuples, elapsed_times[0]};
		} else {
			double elapsed_time = numeric_limits<double>::max();
			for (idx_t k = 1; k < elapsed_times.size(); k++) {
				if (elapsed_times[k] < elapsed_time) {
					elapsed_time = elapsed_times[k];
				}
			}
			return QueryRunResult{query_id, jos_id, enable_rai, num_tuples, elapsed_time};
		}
	}

protected:
	string jos_input;
	int query_id;
	int start_jos_id;
	int num_jos;
};

class JOBSpectrumRunner : public SpectrumRunner {
public:
	JOBSpectrumRunner(string jos_input, int query_id, int start_jos_id, int num_jos)
	    : SpectrumRunner(jos_input, query_id, start_jos_id, num_jos) {
	}

private:
	void Initialize(Connection &conn_, bool enableRAI) override {
		imdb::dbgen(conn_, enableRAI, true);
	}

	string GetQuery() override {
		return imdb::get_113_query(query_id);
	}
};

// Q-1: 1-224
// Q-5: 1-244
// Q-9: 1-40
// Q-12: 1-244
// Q-15: 1-244
// Q-18: 1-244
int main(int argc, char *argv[]) {
	if (argc != 5) {
		cout << "USAGE: <JOS_INPUT_DIR>, <QUERY_ID>, <START_JOS_ID>, <END_JOS_ID (not included)>" << endl;
	}
	string jos_input = argv[1];
	int query_id = stoi(argv[2]);
	int start_jos_id = stoi(argv[3]);
	int num_jos = stoi(argv[4]);
	JOBSpectrumRunner job_spectrum_runner(jos_input, query_id, start_jos_id, num_jos);
	job_spectrum_runner.Run();
}

// ./build/release/examples/spectrum_runner/spectrum_runner /home/g35jin/graindb-optimizer/job/jos/Q01/ 1 1 2
// ./build/release/examples/spectrum_runner/spectrum_runner /home/g35jin/graindb-optimizer/job/jos/Q02/ 5 1 2
// ./build/release/examples/spectrum_runner/spectrum_runner /home/g35jin/graindb-optimizer/job/jos/Q03/ 9 1 41
// ./build/release/examples/spectrum_runner/spectrum_runner /home/g35jin/graindb-optimizer/job/jos/Q04/ 12 1 2
// ./build/release/examples/spectrum_runner/spectrum_runner /home/g35jin/graindb-optimizer/job/jos/Q05/ 15 1 2
// ./build/release/examples/spectrum_runner/spectrum_runner /home/g35jin/graindb-optimizer/job/jos/Q06/ 18 1 2
