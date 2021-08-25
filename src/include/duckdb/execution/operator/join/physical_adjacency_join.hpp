//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/operator/join/physical_adjacency_join.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/types/chunk_collection.hpp"
#include "duckdb/execution/operator/join/physical_comparison_join.hpp"

namespace duckdb {

class PhysicalAdjacencyJoinState : public PhysicalComparisonJoinState {
public:
	PhysicalAdjacencyJoinState(PhysicalOperator *left, PhysicalOperator *right, vector<JoinCondition> &conditions)
	    : PhysicalComparisonJoinState(left, right, conditions), right_chunk_idx(0), right_tuple(0), left_tuple(0),
	      initialized(false) {
	}

	idx_t right_chunk_idx;
	idx_t right_tuple;
	idx_t left_tuple;
	ChunkCollection right_chunks;
	ChunkCollection right_conditions;
	DataChunk left_condition;
	bool initialized;
};

class PhysicalAdjacencyJoin : public PhysicalComparisonJoin {
public:
	PhysicalAdjacencyJoin(ClientContext &context, LogicalOperator &op, unique_ptr<PhysicalOperator> left,
	                      unique_ptr<PhysicalOperator> right, vector<JoinCondition> conditions, JoinType join_type);
	PhysicalAdjacencyJoin(ClientContext &context, LogicalOperator &op, unique_ptr<PhysicalOperator> left,
	                      unique_ptr<PhysicalOperator> right, vector<JoinCondition> conditions, JoinType join_type,
	                      vector<idx_t> &left_projection_map, vector<idx_t> &right_projection_map);

public:
	void GetChunkInternal(ClientContext &context, DataChunk &chunk, PhysicalOperatorState *state_,
	                      SelectionVector *sel = nullptr, Vector *rid_vector = nullptr,
	                      DataChunk *rai_chunk = nullptr) override;
	unique_ptr<PhysicalOperatorState> GetOperatorState() override;

private:
	template <bool JOIN_RAI>
	idx_t PerformJoin(ClientContext &context, DataChunk &right_chunk, DataChunk &right_condition_chunk,
	                  DataChunk &rai_chunk, SelectionVector &lvector, SelectionVector &rvector,
	                  PhysicalAdjacencyJoinState *state);
	static idx_t RefineJoin(Vector &left, Vector &right, SelectionVector &sel, idx_t left_size, idx_t right_size,
	                        SelectionVector &lvector, SelectionVector &rvector, idx_t current_match_count,
	                        ExpressionType comparison_type);

	vector<column_t> left_projection_map;
	vector<idx_t> right_projection_map;
};

} // namespace duckdb
