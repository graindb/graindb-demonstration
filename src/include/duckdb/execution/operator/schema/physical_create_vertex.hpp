#pragma once

#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/execution/physical_operator.hpp"
#include "duckdb/planner/parsed_data/bound_create_vertex_info.hpp"

namespace duckdb {

class PhysicalCreateVertex : public PhysicalOperator {

public:
	PhysicalCreateVertex(LogicalOperator &op, string name, unique_ptr<BoundCreateVertexInfo> info)
	    : PhysicalOperator(PhysicalOperatorType::CREATE_VERTEX, op.types), name(move(name)), info(move(info)) {
	}

	string name;
	unique_ptr<BoundCreateVertexInfo> info;

public:
	void GetChunkInternal(ClientContext &context, DataChunk &chunk, PhysicalOperatorState *state_,
	                      SelectionVector *sel = nullptr, Vector *rid_vector = nullptr,
	                      DataChunk *rai_chunk = nullptr) override;
};
} // namespace duckdb
