#pragma once

#include "duckdb/common/constants.hpp"
#include "duckdb/common/types/chunk_collection.hpp"
#include "duckdb/execution/index/rai/rel_adj_index.hpp"
#include "duckdb/execution/operator/join/physical_comparison_join.hpp"
#include "duckdb/execution/physical_operator.hpp"
#include "duckdb/execution/rai_hashtable.hpp"
#include "duckdb/planner/operator/logical_join.hpp"

namespace duckdb {

class PhysicalMergeRAIJoin : public PhysicalComparisonJoin {
public:
	PhysicalMergeRAIJoin(ClientContext &context, LogicalOperator &op, unique_ptr<PhysicalOperator> left,
	                     unique_ptr<PhysicalOperator> right, vector<JoinCondition> cond, JoinType join_type,
	                     vector<idx_t> &left_projection_map, vector<idx_t> &right_projection_map,
	                     idx_t left_cardinality, bool enable_lookup_join);

private:
	unique_ptr<RAIHashTable> hash_table;
	vector<idx_t> right_projection_map;
	idx_t build_cardinality; // built hash table cardinality
	idx_t right_side_size;
	bool enable_lookup_join;
	bool chooseRJ = false;

	// extra info
	bool pass_rows_filter = false;
	bool pass_zone_filter = false;

	// public:
	//	vector<PhysicalOperator *> passing_scan_ops = {nullptr, nullptr};

public:
	void GetChunkInternal(ClientContext &context, DataChunk &chunk, PhysicalOperatorState *state_,
	                      SelectionVector *sel = nullptr, Vector *rid_vector = nullptr,
	                      DataChunk *rai_chunk = nullptr) override;
	unique_ptr<PhysicalOperatorState> GetOperatorState() override;
	string ExtraRenderInformation() const override;

private:
	void AppendHashTable(PhysicalOperatorState *state_, DataChunk &chunk, DataChunk &build_chunk);
	void PassZoneFilter();
	void PassRowsFilter(idx_t rows_size);
	void ProbeHashTable(ClientContext &context, DataChunk &chunk, PhysicalOperatorState *state_);
	void PerformRJoin(ClientContext &context, DataChunk &chunk, PhysicalOperatorState *state_);
	idx_t DoAJoin(ClientContext &context, DataChunk &right_chunk, DataChunk &right_condition_chunk,
	              SelectionVector &lvector, SelectionVector &rvector, PhysicalOperatorState *state);
	void PerformAJoin(ClientContext &context, DataChunk &chunk, PhysicalOperatorState *state_);
};
} // namespace duckdb
