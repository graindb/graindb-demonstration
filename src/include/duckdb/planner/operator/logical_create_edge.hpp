//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/planner/operator/logical_create_rai.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/parser/parsed_data/create_edge_info.hpp"
#include "duckdb/planner/logical_operator.hpp"

namespace duckdb {

class LogicalCreateEdge : public LogicalOperator {
public:
	LogicalCreateEdge(SchemaCatalogEntry *schema, unique_ptr<BoundCreateEdgeInfo> info)
	    : LogicalOperator(LogicalOperatorType::CREATE_EDGE), schema(schema), info(move(info)) {
	}

	SchemaCatalogEntry *schema;
	unique_ptr<BoundCreateEdgeInfo> info;

protected:
	void ResolveTypes() override {
		types.push_back(TypeId::INT64);
	}
};
} // namespace duckdb
