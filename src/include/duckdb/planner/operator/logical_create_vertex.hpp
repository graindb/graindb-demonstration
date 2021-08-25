#pragma once

#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/parser/parsed_data/create_vertex_info.hpp"
#include "duckdb/planner/logical_operator.hpp"
#include "duckdb/planner/parsed_data/bound_create_vertex_info.hpp"

namespace duckdb {

class LogicalCreateVertex : public LogicalOperator {

public:
	LogicalCreateVertex(SchemaCatalogEntry *schema, unique_ptr<BoundCreateVertexInfo> info)
	    : LogicalOperator(LogicalOperatorType::CREATE_VERTEX), schema(schema), info(move(info)) {
	}

	SchemaCatalogEntry *schema;
	unique_ptr<BoundCreateVertexInfo> info;

protected:
	void ResolveTypes() override {
		types.push_back(TypeId::INT64);
	}
};
} // namespace duckdb
