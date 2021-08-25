#pragma once
#include "duckdb/catalog/catalog_entry/vertex_catalog_entry.hpp"
#include "duckdb/common/enums/edge_direction.hpp"
#include "duckdb/planner/expression.hpp"
#include "duckdb/planner/parsed_data/bound_create_info.hpp"
#include "duckdb/planner/tableref/bound_basetableref.hpp"

namespace duckdb {

class BoundVertexRef : public BoundTableRef {

public:
	BoundVertexRef(string name, unique_ptr<BoundBaseTableRef> base_table, VertexCatalogEntry *vertex_entry)
	    : BoundTableRef(TableReferenceType::VERTEX), name(move(name)), base_table(move(base_table)),
	      vertex_entry(vertex_entry) {
	}

public:
	string name;
	unique_ptr<BoundBaseTableRef> base_table;
	//! This is only needed for BindCreateEdgeInfo
	VertexCatalogEntry *vertex_entry;
};
} // namespace duckdb
