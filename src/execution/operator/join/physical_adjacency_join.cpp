#include "duckdb/execution/operator/join/physical_adjacency_join.hpp"

#include "duckdb/common/operator/comparison_operators.hpp"

#include <numeric>

using namespace duckdb;
using namespace std;

struct RefineAInnerJoin {
	template <class T, class OP>
	static idx_t Operation(Vector &left, Vector &right, SelectionVector &sel, idx_t left_size, idx_t right_size,
	                       SelectionVector &lvector, SelectionVector &rvector, idx_t current_match_count) {
		VectorData left_data, right_data;
		left.Orrify(left_size, left_data);
		right.Orrify(right_size, right_data);

		// refine lvector and rvector based on matches of subsequent conditions (in case there are multiple conditions
		// in the join)
		assert(current_match_count > 0);
		auto ldata = (T *)left_data.data;
		auto rdata = (T *)right_data.data;
		idx_t result_count = 0;
		for (idx_t i = 0; i < current_match_count; i++) {
			auto left_idx = left_data.sel->get_index(i);
			auto right_idx = sel.get_index(i);
			// null values should be filtered out before
			if ((*left_data.nullmask)[left_idx] || (*right_data.nullmask)[right_idx]) {
				continue;
			}
			if (OP::Operation(ldata[left_idx], rdata[right_idx])) {
				lvector.set_index(result_count, i);
				rvector.set_index(result_count, i);
				result_count++;
			}
		}
		return result_count;
	}
};

template <class OP>
static idx_t join_inner_operator(Vector &left, Vector &right, SelectionVector &sel, idx_t left_size, idx_t right_size,
                                 SelectionVector &lvector, SelectionVector &rvector, idx_t current_match_count) {
	switch (left.type) {
	case TypeId::BOOL:
	case TypeId::INT8:
		return RefineAInnerJoin::template Operation<int8_t, OP>(left, right, sel, left_size, right_size, lvector,
		                                                        rvector, current_match_count);
	case TypeId::INT16:
		return RefineAInnerJoin::template Operation<int16_t, OP>(left, right, sel, left_size, right_size, lvector,
		                                                         rvector, current_match_count);
	case TypeId::INT32:
		return RefineAInnerJoin::template Operation<int32_t, OP>(left, right, sel, left_size, right_size, lvector,
		                                                         rvector, current_match_count);
	case TypeId::INT64:
		return RefineAInnerJoin::template Operation<int64_t, OP>(left, right, sel, left_size, right_size, lvector,
		                                                         rvector, current_match_count);
	case TypeId::FLOAT:
		return RefineAInnerJoin::template Operation<float, OP>(left, right, sel, left_size, right_size, lvector,
		                                                       rvector, current_match_count);
	case TypeId::DOUBLE:
		return RefineAInnerJoin::template Operation<double, OP>(left, right, sel, left_size, right_size, lvector,
		                                                        rvector, current_match_count);
	case TypeId::VARCHAR:
		return RefineAInnerJoin::template Operation<string_t, OP>(left, right, sel, left_size, right_size, lvector,
		                                                          rvector, current_match_count);
	default:
		throw NotImplementedException("Unimplemented type for join!");
	}
}

PhysicalAdjacencyJoin::PhysicalAdjacencyJoin(ClientContext &context, LogicalOperator &op,
                                             unique_ptr<PhysicalOperator> left, unique_ptr<PhysicalOperator> right,
                                             vector<JoinCondition> cond, JoinType join_type)
    : PhysicalComparisonJoin(op, PhysicalOperatorType::ADJACENCY_JOIN, move(cond), join_type) {
	children.push_back(move(left));
	children.push_back(move(right));
}

PhysicalAdjacencyJoin::PhysicalAdjacencyJoin(ClientContext &context, LogicalOperator &op,
                                             unique_ptr<PhysicalOperator> left, unique_ptr<PhysicalOperator> right,
                                             vector<JoinCondition> cond, JoinType join_type,
                                             vector<idx_t> &left_projection_map_, vector<idx_t> &right_projection_map_)
    : PhysicalComparisonJoin(op, PhysicalOperatorType::ADJACENCY_JOIN, move(cond), join_type),
      left_projection_map(left_projection_map_), right_projection_map(right_projection_map_) {
	children.push_back(move(left));
	children.push_back(move(right));
}

template <bool JOIN_RAI>
idx_t PhysicalAdjacencyJoin::PerformJoin(ClientContext &context, DataChunk &right_chunk,
                                         DataChunk &right_condition_chunk, DataChunk &rai_chunk,
                                         SelectionVector &lvector, SelectionVector &rvector,
                                         PhysicalAdjacencyJoinState *state) {
	auto right_types = right_chunk.GetTypes();
	right_types.push_back(TypeId::INT64);
	//right_types.push_back(TypeId::INT64);
	SelectionVector sel(STANDARD_VECTOR_SIZE);
	for (idx_t i = 0; i < STANDARD_VECTOR_SIZE; i++) {
		sel.set_index(i, i);
	}
	if (JOIN_RAI) {
		rai_chunk.Initialize(right_types);
		auto &rai_info = conditions[0].rais[0];
		rai_info->rai->GetChunk(right_chunk, right_condition_chunk, rai_chunk, state->left_tuple, state->right_tuple,
		                        rai_info->forward);
		children[0]->GetChunk(context, state->child_chunk, state->child_state.get(), &sel,
		                      &rai_chunk.data[rai_chunk.column_count() - 1], &rai_chunk);
	} else {
		rai_chunk.InitializeEmpty(right_types);
		rai_chunk.Reference(right_chunk);
		state->right_chunk_idx++;
		children[0]->GetChunk(context, state->child_chunk, state->child_state.get(), &sel,
		                      &right_condition_chunk.data[0], &rai_chunk);
	}
	idx_t match_count = state->child_chunk.size();
	if (match_count == 0) {
		return 0;
	}
	rai_chunk.Slice(sel, match_count);
	//	right_condition_chunk.Slice(lvector, match_count);
	if (conditions.size() > 1) {
		lvector.Initialize();
		rvector.Initialize();
		state->lhs_executor.Execute(state->child_chunk, state->left_condition);
		for (idx_t i = 1; i < conditions.size(); i++) {
			if (match_count == 0) {
				continue;
			}
			Vector &left = state->left_condition.data[i];
			Vector &right = right_condition_chunk.data[i];
			match_count = RefineJoin(left, right, sel, state->left_condition.size(), rai_chunk.size(), lvector, rvector,
			                         match_count, conditions[i].comparison);
		}
	}

	return match_count;
}

idx_t PhysicalAdjacencyJoin::RefineJoin(Vector &left, Vector &right, SelectionVector &sel, idx_t left_size,
                                        idx_t right_size, SelectionVector &lvector, SelectionVector &rvector,
                                        idx_t current_match_count, ExpressionType comparison_type) {
	assert(left.type == right.type);
	switch (comparison_type) {
	case ExpressionType::COMPARE_EQUAL:
		return join_inner_operator<duckdb::Equals>(left, right, sel, left_size, right_size, lvector, rvector,
		                                           current_match_count);
	case ExpressionType::COMPARE_NOTEQUAL:
		return join_inner_operator<duckdb::NotEquals>(left, right, sel, left_size, right_size, lvector, rvector,
		                                              current_match_count);
	case ExpressionType::COMPARE_LESSTHAN:
		return join_inner_operator<duckdb::LessThan>(left, right, sel, left_size, right_size, lvector, rvector,
		                                             current_match_count);
	case ExpressionType::COMPARE_GREATERTHAN:
		return join_inner_operator<duckdb::GreaterThan>(left, right, sel, left_size, right_size, lvector, rvector,
		                                                current_match_count);
	case ExpressionType::COMPARE_LESSTHANOREQUALTO:
		return join_inner_operator<duckdb::LessThanEquals>(left, right, sel, left_size, right_size, lvector, rvector,
		                                                   current_match_count);
	case ExpressionType::COMPARE_GREATERTHANOREQUALTO:
		return join_inner_operator<duckdb::GreaterThanEquals>(left, right, sel, left_size, right_size, lvector, rvector,
		                                                      current_match_count);
	default:
		throw NotImplementedException("Unimplemented comparison type for join!");
	}
}

void PhysicalAdjacencyJoin::GetChunkInternal(ClientContext &context, DataChunk &chunk, PhysicalOperatorState *state_,
                                             SelectionVector *sel, Vector *rid_vector, DataChunk *rai_chunk_) {
	// get state and transaction var
	auto state = reinterpret_cast<PhysicalAdjacencyJoinState *>(state_);

	// fully materialize right chunks
	if (state->right_chunks.column_count() == 0) {
		vector<TypeId> condition_types;
		for (auto &cond : conditions) {
			assert(cond.left->type == cond.right->type);
			condition_types.push_back(cond.right->return_type);
		}
		auto types = children[1]->GetTypes();
		DataChunk new_chunk, right_condition;
		new_chunk.Initialize(types);
		right_condition.Initialize(condition_types);
		state->left_condition.Initialize(condition_types);
		// get all right chunks
		auto right_state = children[1]->GetOperatorState();
		while (true) {
			children[1]->GetChunk(context, new_chunk, right_state.get(), sel, rid_vector, rai_chunk_);
			if (new_chunk.size() == 0) {
				break;
			}
			state->rhs_executor.Execute(new_chunk, right_condition);
			state->right_chunks.Append(new_chunk);
			state->right_conditions.Append(right_condition);
		}

		if (state->right_chunks.count == 0) {
			return;
		}
		state->initialized = true;
	}

	if (state->right_chunks.count == 0) {
		return;
	}
	if (state->initialized && (state->right_chunk_idx == state->right_chunks.chunks.size())) {
		return;
	}
	// get left chunk
	do {
		if (state->right_chunk_idx < state->right_conditions.chunks.size() &&
		    state->right_tuple >= state->right_conditions.chunks[state->right_chunk_idx]->size()) {
			state->right_chunk_idx++;
			state->right_tuple = 0;
			state->left_tuple = 0;
		}
		if (state->right_chunk_idx >= state->right_conditions.chunks.size()) {
			return;
		}
		DataChunk rai_chunk, new_right_chunk;
		idx_t match_count;
		SelectionVector lvector(STANDARD_VECTOR_SIZE);
		SelectionVector rvector(STANDARD_VECTOR_SIZE);
		for (idx_t i = 0; i < STANDARD_VECTOR_SIZE; i++) {
			lvector.set_index(i, i);
			rvector.set_index(i, i);
		}
		auto &right_condition_chunk = *state->right_conditions.chunks[state->right_chunk_idx];
		auto &right_chunk = *state->right_chunks.chunks[state->right_chunk_idx];
		if (conditions[0].rais.size() != 0) {
			match_count =
			    PerformJoin<true>(context, right_chunk, right_condition_chunk, rai_chunk, lvector, rvector, state);
		} else {
			match_count =
			    PerformJoin<false>(context, right_chunk, right_condition_chunk, rai_chunk, lvector, rvector, state);
		}
		auto left_column_count = state->child_chunk.column_count();
		chunk.Slice(state->child_chunk, FlatVector::IncrementalSelectionVector, match_count);
		if (right_projection_map.size() == 0) {
			chunk.Slice(rai_chunk, rvector, match_count, left_column_count, chunk.column_count() - left_column_count);
		} else {
			assert(right_projection_map.size() + left_column_count <= chunk.column_count());
			chunk.Slice(rai_chunk, rvector, match_count, left_column_count, right_projection_map);
		}
		chunk.SetCardinality(match_count);
	} while (chunk.size() == 0);
}

unique_ptr<PhysicalOperatorState> PhysicalAdjacencyJoin::GetOperatorState() {
	return make_unique<PhysicalAdjacencyJoinState>(children[0].get(), children[1].get(), conditions);
}
