//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/planner/tableref/bound_basetableref.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/planner/bound_tableref.hpp"
#include "duckdb/planner/logical_operator.hpp"

namespace duckdb {
class TableCatalogEntry;

//! Represents a TableReference to a base table in the schema
class BoundBaseTableRef : public BoundTableRef {
public:
	BoundBaseTableRef(unique_ptr<LogicalOperator> get) : BoundTableRef(TableReferenceType::BASE_TABLE), get(move(get)) {
	}

	unique_ptr<LogicalOperator> get;
	idx_t table_index;
};
} // namespace duckdb
