#pragma once
#include "duckdb/catalog/catalog_entry/vertex_catalog_entry.hpp"
#include "duckdb/common/enums/edge_direction.hpp"
#include "duckdb/planner/expression.hpp"
#include "duckdb/planner/parsed_data/bound_create_info.hpp"
#include "duckdb/planner/tableref/bound_basetableref.hpp"

namespace duckdb {

class BoundEdgeRef : public BoundTableRef {

public:
	BoundEdgeRef(string name, unique_ptr<BoundBaseTableRef> base_table, EdgeCatalogEntry *edge_entry)
	    : BoundTableRef(TableReferenceType::EDGE), name(move(name)), base_table(move(base_table)),
	      edge_entry(edge_entry) {
	}

public:
	string name;
	unique_ptr<BoundBaseTableRef> base_table;
	EdgeCatalogEntry *edge_entry;
};
} // namespace duckdb