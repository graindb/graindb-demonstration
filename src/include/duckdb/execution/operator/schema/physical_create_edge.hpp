//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/operator/schema/physical_create_edge.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/execution/physical_operator.hpp"
#include "duckdb/parser/parsed_data/create_edge_info.hpp"
#include "duckdb/storage/data_table.hpp"

namespace duckdb {

class PhysicalCreateEdge : public PhysicalOperator {
public:
	PhysicalCreateEdge(LogicalOperator &op, string name, unique_ptr<BoundCreateEdgeInfo> info)
	    : PhysicalOperator(PhysicalOperatorType::CREATE_EDGE, op.types), name(move(name)), info(move(info)) {
	}

	string name;
	unique_ptr<BoundCreateEdgeInfo> info;

public:
	void GetChunkInternal(ClientContext &context, DataChunk &chunk, PhysicalOperatorState *state_,
	                      SelectionVector *sel = nullptr, Vector *rid_vector = nullptr,
	                      DataChunk *rai_chunk = nullptr) override;
};
} // namespace duckdb
