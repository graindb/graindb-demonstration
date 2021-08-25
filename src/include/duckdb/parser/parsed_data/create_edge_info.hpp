//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/parser/parsed_data/create_edge_info.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/enums/edge_direction.hpp"
#include "duckdb/parser/parsed_data/create_info.hpp"
#include "duckdb/parser/tableref/basetableref.hpp"
#include "duckdb/parser/tableref/vertexref.hpp"

namespace duckdb {

struct CreateEdgeInfo : public CreateInfo {
	CreateEdgeInfo() : CreateInfo(CatalogType::EDGE), edge_direction(EdgeDirection::UNDIRECTED) {
	}

	CreateEdgeInfo(string name, unique_ptr<BaseTableRef> edge_table, EdgeDirection edge_direction)
	    : CreateInfo(CatalogType::EDGE), name(move(name)), edge_table(move(edge_table)),
	      edge_direction(edge_direction) {
	}

	string name;
	unique_ptr<BaseTableRef> edge_table;
	EdgeDirection edge_direction;
	//! Src and dst vertex tables
	vector<unique_ptr<VertexRef>> vertices;
	//! Columns in edge table referencing vertex tables
	vector<unique_ptr<ParsedExpression>> reference_columns;
};
} // namespace duckdb
