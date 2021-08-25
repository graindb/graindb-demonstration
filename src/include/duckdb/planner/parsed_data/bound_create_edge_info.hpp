//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/planner/parsed_data/bound_create_rai_info.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/enums/edge_direction.hpp"
#include "duckdb/planner/expression.hpp"
#include "duckdb/planner/parsed_data/bound_create_info.hpp"
#include "duckdb/planner/tableref/bound_basetableref.hpp"
#include "duckdb/planner/tableref/bound_vertexref.hpp"

namespace duckdb {

class VertexCatalogEntry;

struct BoundCreateEdgeInfo : public BoundCreateInfo {

public:
	BoundCreateEdgeInfo(unique_ptr<CreateInfo> base, string name, SchemaCatalogEntry *schema,
	                    unique_ptr<BoundBaseTableRef> edge_table, EdgeDirection edge_direction,
	                    vector<unique_ptr<BoundVertexRef>> vertices, vector<unique_ptr<Expression>> reference_columns,
	                    TableCatalogEntry *edge_table_entry, vector<VertexCatalogEntry *> vertex_entries,
	                    vector<column_t> vertex_primary_column_ids, vector<column_t> reference_column_ids)
	    : BoundCreateInfo(move(base)), name(move(name)), schema(schema), edge_table(move(edge_table)),
	      edge_direction(edge_direction), vertices(move(vertices)), reference_columns(move(reference_columns)),
	      edge_table_entry(edge_table_entry), vertex_entries(move(vertex_entries)),
	      vertex_primary_column_ids(move(vertex_primary_column_ids)), reference_column_ids(move(reference_column_ids)) {
	}

	string name;
	//! The schema to create the edge in
	SchemaCatalogEntry *schema;
	unique_ptr<BoundBaseTableRef> edge_table;
	EdgeDirection edge_direction;
	//! Src and dst vertex tables
	vector<unique_ptr<BoundVertexRef>> vertices;
	//! Columns in edge table referencing vertex tables
	vector<unique_ptr<Expression>> reference_columns;

	//! Info directly used by catalog entry (duplicated with above ones)
	TableCatalogEntry *edge_table_entry;
	vector<VertexCatalogEntry *> vertex_entries;
	vector<column_t> vertex_primary_column_ids;
	vector<column_t> reference_column_ids;
	vector<string> reference_column_names;

	//! The sub plan to generate join column rowid
	BoundStatement query_node;
};
} // namespace duckdb
