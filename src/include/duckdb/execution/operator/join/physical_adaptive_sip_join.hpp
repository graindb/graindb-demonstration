#pragma once

#include "duckdb/common/constants.hpp"
#include "duckdb/common/types/chunk_collection.hpp"
#include "duckdb/execution/index/rai/rel_adj_index.hpp"
#include "duckdb/execution/operator/join/physical_comparison_join.hpp"
#include "duckdb/execution/physical_operator.hpp"
#include "duckdb/execution/sip_hashtable.hpp"
#include "duckdb/planner/operator/logical_join.hpp"

namespace duckdb {

class PhysicalAdaptiveSIPJoin : public PhysicalComparisonJoin {
public:
	PhysicalAdaptiveSIPJoin(ClientContext &context, LogicalOperator &op, unique_ptr<PhysicalOperator> left,
	                        unique_ptr<PhysicalOperator> right, vector<JoinCondition> cond, JoinType join_type,
	                        vector<idx_t> &left_projection_map, vector<idx_t> &right_projection_map, bool enable_nlaj);

private:
	unique_ptr<SIPHashTable> hash_table;
	vector<idx_t> right_projection_map;
	bool enableNLAJ;
	bool chooseSHJ = true;

public:
	void GetChunkInternal(ClientContext &context, DataChunk &chunk, PhysicalOperatorState *state_,
	                      SelectionVector *sel = nullptr, Vector *rid_vector = nullptr,
	                      DataChunk *rai_chunk = nullptr) override;
	unique_ptr<PhysicalOperatorState> GetOperatorState() override;
	string ExtraRenderInformation() const override;

private:
	void InitializeAList();
	void InitializeZoneFilter();
	void PassZoneFilter();
	void AppendHTBlocks(PhysicalOperatorState *state, DataChunk &chunk, DataChunk &build_chunk);
	void ProbeHashTable(ClientContext &context, DataChunk &chunk, PhysicalOperatorState *state_);
	void PerformSHJoin(ClientContext &context, DataChunk &chunk, PhysicalOperatorState *state_);

	template <bool JOIN_RAI>
	idx_t DoAJoin(ClientContext &context, DataChunk &right_chunk, DataChunk &right_condition_chunk,
	              SelectionVector &lvector, SelectionVector &rvector, PhysicalOperatorState *state);
	void PerformNLAJoin(ClientContext &context, DataChunk &chunk, PhysicalOperatorState *state_);
};

} // namespace duckdb
