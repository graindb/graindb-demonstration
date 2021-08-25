#pragma once

#include "duckdb/planner/bound_tableref.hpp"
#include "duckdb/planner/expression.hpp"
#include "duckdb/planner/parsed_data/bound_create_info.hpp"
#include "duckdb/planner/tableref/bound_basetableref.hpp"

namespace duckdb {

struct BoundCreateVertexInfo : public BoundCreateInfo {

public:
	BoundCreateVertexInfo(unique_ptr<CreateInfo> base, string name, SchemaCatalogEntry *schema,
	                      unique_ptr<BoundBaseTableRef> base_table, TableCatalogEntry *base_table_entry)
	    : BoundCreateInfo(move(base)), name(move(name)), schema(schema), base_table(move(base_table)),
	      base_table_entry(base_table_entry) {
	}

	string name;
	//! The schema to create the edge in
	SchemaCatalogEntry *schema;
	unique_ptr<BoundBaseTableRef> base_table;
	//! Info directly used by catalog entry (duplicated with above ones)
	TableCatalogEntry *base_table_entry;
};
} // namespace duckdb
