//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/planner/planner.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/catalog/catalog_entry/prepared_statement_catalog_entry.hpp"
#include "duckdb/parser/sql_statement.hpp"
#include "duckdb/planner/binder.hpp"
#include "duckdb/planner/logical_operator.hpp"

namespace duckdb {
class ClientContext;

//! The planner creates a logical query plan from the parsed SQL statements
//! using the Binder.
class Planner {
public:
	explicit Planner(ClientContext &context);

	void CreatePlan(unique_ptr<SQLStatement> statement);

	unique_ptr<LogicalOperator> plan;
	vector<string> names;
	vector<SQLType> sql_types;
	unordered_map<idx_t, PreparedValueEntry> value_map;

	Binder binder;
	ClientContext &context;

	bool read_only;
	bool requires_valid_transaction;

private:
	void CreatePlan(SQLStatement &statement);
};
} // namespace duckdb
