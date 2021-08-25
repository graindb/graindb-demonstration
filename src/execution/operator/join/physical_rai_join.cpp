#include "duckdb/execution/operator/join/physical_rai_join.hpp"

#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/execution/index/rai/rel_adj_index.hpp"
#include "duckdb/storage/buffer_manager.hpp"
#include "duckdb/storage/storage_manager.hpp"

#include <iostream>

using namespace duckdb;
using namespace std;

class PhysicalRAIJoinState : public PhysicalComparisonJoinState {
public:
	PhysicalRAIJoinState(PhysicalOperator *left, PhysicalOperator *right, vector<JoinCondition> &conditions)
	    : PhysicalComparisonJoinState(left, right, conditions), initialized(false), right_chunk_idx(0), right_tuple(0),
	      left_tuple(0), join_rai(true), max_join_output_size(0) {
	}

	bool initialized;
	DataChunk cached_chunk;
	DataChunk join_keys;
	ChunkCollection right_chunks;
	// state for RJoin
	unique_ptr<RAIHashTable::RAIScanStructure> scan_structure;
	// state for AJoin
	DataChunk right_condition_chunk;
	DataChunk rai_chunk;
	idx_t right_chunk_idx;
	idx_t right_tuple;
	idx_t left_tuple;
	bool join_rai;

	idx_t max_join_output_size;
};

PhysicalRAIJoin::PhysicalRAIJoin(ClientContext &context, LogicalOperator &op, unique_ptr<PhysicalOperator> left,
                                 unique_ptr<PhysicalOperator> right, vector<JoinCondition> cond, JoinType join_type,
                                 vector<idx_t> &left_projection_map, vector<idx_t> &right_projection_map,
                                 idx_t build_cardinality_, bool enable_lookup_join)
    : PhysicalComparisonJoin(op, PhysicalOperatorType::RAI_JOIN, move(cond), join_type),
      right_projection_map(right_projection_map), build_cardinality(build_cardinality_), right_side_size(0),
      enable_lookup_join(enable_lookup_join) {
	children.push_back(move(left));
	children.push_back(move(right));

	assert(left_projection_map.size() == 0);

	hash_table = make_unique<RAIHashTable>(BufferManager::GetBufferManager(context), conditions,
	                                       LogicalOperator::MapTypes(children[1]->GetTypes(), right_projection_map),
	                                       type, build_cardinality);
}

static idx_t GetExactChunkJoinSize(RAIInfo &rai_info, DataChunk &join_keys) {
	idx_t join_size = 0;
	VectorData source_data;
	idx_t count = join_keys.size();
	join_keys.data[0].Orrify(count, source_data);
	auto source = (int64_t *)source_data.data;
	auto &alist =
	    rai_info.forward ? rai_info.rai->alist->compact_forward_list : rai_info.rai->alist->compact_backward_list;
	if (source_data.nullmask->any()) {
		for (idx_t i = 0; i < count; i++) {
			auto idx = source_data.sel->get_index(i);
			if ((*source_data.nullmask)[idx] == false) {
				join_size += alist.sizes[source[idx]];
			}
		}
	} else {
		for (idx_t i = 0; i < count; i++) {
			auto idx = source_data.sel->get_index(i);
			join_size += alist.sizes[source[idx]];
		}
	}
	return join_size;
}

/*
 * RJoin
 */
void PhysicalRAIJoin::PassRowsFilterWithAList(RAIInfo &rai_info, idx_t rows_size) {
	// init rows filter
	shared_ptr<rows_vector> rows_filters[2] = {nullptr, nullptr};
	rows_filters[0] = make_shared<rows_vector>(rows_size);
	if (rai_info.passing_tables[1]) {
		rows_filters[1] = make_shared<rows_vector>(rows_size);
	}
	auto rows_filter_size = rai_info.rai->GetRowsFilter(hash_table->GetHashmapBuffer(), build_cardinality,
	                                                    rows_filters[0], rows_filters[1], rai_info.forward);
	rows_filters[0]->resize(rows_filter_size);
	sort(rows_filters[0]->begin(), rows_filters[0]->end());
	rows_filters[0]->erase(unique(rows_filters[0]->begin(), rows_filters[0]->end()), rows_filters[0]->end());
	children[0]->PushdownRowsFilter(rai_info.passing_tables[0], rows_filters[0], rows_filter_size);
	// push down rows filters
	if (rai_info.passing_tables[1] > 0) {
		rows_filters[1]->resize(rows_filter_size);
		sort(rows_filters[1]->begin(), rows_filters[1]->end());
		rows_filters[1]->erase(unique(rows_filters[1]->begin(), rows_filters[1]->end()), rows_filters[1]->end());
		children[0]->PushdownRowsFilter(rai_info.passing_tables[1], rows_filters[1], rows_filter_size);
	}
}

void PhysicalRAIJoin::PassZoneFilterWithAList(RAIInfo &rai_info) {
	// init zone filter
	auto zone_filter_size = rai_info.left_cardinalities[0] / STANDARD_VECTOR_SIZE + 1;
	rai_info.row_bitmask = make_shared<bitmask_vector>(rai_info.left_cardinalities[0]);
	if (rai_info.passing_tables[1] != 0) {
		auto extra_zone_filter_size = rai_info.left_cardinalities[1] / STANDARD_VECTOR_SIZE + 1;
		rai_info.extra_row_bitmask = make_shared<bitmask_vector>(rai_info.left_cardinalities[1]);
	}
	rai_info.rai->GetZoneFilter(hash_table->GetHashmapBuffer(), build_cardinality, rai_info.row_bitmask,
	                            rai_info.extra_row_bitmask, rai_info.forward);
	// push down zone filters
	children[0]->PushdownZoneFilter(rai_info.passing_tables[0], rai_info.row_bitmask, rai_info.zone_bitmask);
	if (rai_info.passing_tables[1] > 0) {
		children[0]->PushdownZoneFilter(rai_info.passing_tables[1], rai_info.extra_row_bitmask,
		                                rai_info.extra_zone_bitmask);
	}
}

void PhysicalRAIJoin::PassRowsFilterWithoutAList(RAIInfo &rai_info, idx_t rows_size) {
	auto rows_filter = make_shared<rows_vector>(rows_size);
	auto rows_filter_size = hash_table->GetRowsFilter(*rows_filter);
	// push down rows filters
	children[0]->PushdownRowsFilter(rai_info.passing_tables[0], rows_filter, rows_filter_size);
}

void PhysicalRAIJoin::PassZoneFilterWithoutAList(RAIInfo &rai_info) {
	// init zone filter
	auto zone_filter_size = rai_info.left_cardinalities[0] / STANDARD_VECTOR_SIZE + 1;
	rai_info.row_bitmask = make_shared<bitmask_vector>(rai_info.left_cardinalities[0]);
	hash_table->GetZoneFilter(*rai_info.row_bitmask);
	// push down zone filters
	children[0]->PushdownZoneFilter(rai_info.passing_tables[0], rai_info.row_bitmask, rai_info.zone_bitmask);
}

void PhysicalRAIJoin::PassRowsFilter(idx_t rows_size) {
	auto &rai_info = conditions[0].rais[0];
	switch (rai_info->rai_type) {
	case RAIType::SELF:
	case RAIType::EDGE_SOURCE: {
		PassRowsFilterWithAList(*rai_info, rows_size);
		break;
	}
	case RAIType::EDGE_TARGET: {
		if (rai_info->rai->rai_direction == EdgeDirection::UNDIRECTED) {
			rai_info->forward = false;
			PassRowsFilterWithAList(*rai_info, rows_size);
		}
		break;
	}
	case RAIType::SOURCE_EDGE:
	case RAIType::TARGET_EDGE: {
		PassRowsFilterWithoutAList(*rai_info, rows_size);
		break;
	}
	default:
		break;
	}
}

void PhysicalRAIJoin::PassZoneFilter() {
	auto &rai_info = conditions[0].rais[0];
	switch (rai_info->rai_type) {
	case RAIType::SELF:
	case RAIType::EDGE_SOURCE: {
		PassZoneFilterWithAList(*rai_info);
		break;
	}
	case RAIType::EDGE_TARGET: {
		if (rai_info->rai->rai_direction == EdgeDirection::UNDIRECTED) {
			rai_info->forward = false;
			PassZoneFilterWithAList(*rai_info);
		}
		break;
	}
	case RAIType::SOURCE_EDGE:
	case RAIType::TARGET_EDGE: {
		PassZoneFilterWithoutAList(*rai_info);
		break;
	}
	default:
		break;
	}
}

void PhysicalRAIJoin::AppendHashTable(PhysicalOperatorState *state_, DataChunk &chunk, DataChunk &build_chunk) {
	auto state = reinterpret_cast<PhysicalRAIJoinState *>(state_);
	state->rhs_executor.Execute(chunk, state->join_keys);
	if (hash_table->initialized == false) {
		hash_table->Initialize();
	}
	if (right_projection_map.size() > 0) {
		build_chunk.Reset();
		build_chunk.SetCardinality(chunk);
		for (idx_t i = 0; i < right_projection_map.size(); i++) {
			build_chunk.data[i].Reference(chunk.data[right_projection_map[i]]);
		}
		hash_table->Build(state->join_keys, build_chunk);
	} else {
		hash_table->Build(state->join_keys, chunk);
	}
}

void PhysicalRAIJoin::ProbeHashTable(ClientContext &context, DataChunk &chunk, PhysicalOperatorState *state_) {
	auto state = reinterpret_cast<PhysicalRAIJoinState *>(state_);
	if (state->child_chunk.size() > 0 && state->scan_structure) {
		// still have elements remaining from the previous probe (i.e. we got
		// >1024 elements in the previous probe)
		state->scan_structure->Next(state->join_keys, state->child_chunk, chunk);
		if (chunk.size() > 0) {
			return;
		}
		state->scan_structure = nullptr;
	}

	// probe the HT
	do {
		// fetch the chunk from the left side
		children[0]->GetChunk(context, state->child_chunk, state->child_state.get());
		if (state->child_chunk.size() == 0) {
			return;
		}
		// remove any selection vectors
		if (hash_table->size() == 0) {
			// empty hash table, special case
			if (hash_table->join_type == JoinType::ANTI) {
				// anti join with empty hash table, NOP join
				// return the input
				assert(chunk.column_count() == state->child_chunk.column_count());
				chunk.Reference(state->child_chunk);
				return;
			} else if (hash_table->join_type == JoinType::MARK) {
				// MARK join with empty hash table
				assert(hash_table->join_type == JoinType::MARK);
				assert(chunk.column_count() == state->child_chunk.column_count() + 1);
				auto &result_vector = chunk.data.back();
				assert(result_vector.type == TypeId::BOOL);
				// for every data vector, we just reference the child chunk
				chunk.SetCardinality(state->child_chunk);
				for (idx_t i = 0; i < state->child_chunk.column_count(); i++) {
					chunk.data[i].Reference(state->child_chunk.data[i]);
				}
				// for the MARK vector:
				// if the HT has no NULL values (i.e. empty result set), return a vector that has false for every input
				// entry if the HT has NULL values (i.e. result set had values, but all were NULL), return a vector that
				// has NULL for every input entry
				if (!hash_table->has_null) {
					auto bool_result = FlatVector::GetData<bool>(result_vector);
					for (idx_t i = 0; i < chunk.size(); i++) {
						bool_result[i] = false;
					}
				} else {
					FlatVector::Nullmask(result_vector).set();
				}
				return;
			} else if (hash_table->join_type == JoinType::LEFT || hash_table->join_type == JoinType::OUTER ||
			           hash_table->join_type == JoinType::SINGLE) {
				// LEFT/FULL OUTER/SINGLE join and build side is empty
				// for the LHS we reference the data
				chunk.SetCardinality(state->child_chunk.size());
				for (idx_t i = 0; i < state->child_chunk.column_count(); i++) {
					chunk.data[i].Reference(state->child_chunk.data[i]);
				}
				// for the RHS
				for (idx_t k = state->child_chunk.column_count(); k < chunk.column_count(); k++) {
					chunk.data[k].vector_type = VectorType::CONSTANT_VECTOR;
					ConstantVector::SetNull(chunk.data[k], true);
				}
				return;
			}
		}
		// resolve the join keys for the left chunk
		state->lhs_executor.Execute(state->child_chunk, state->join_keys);

		// perform the actual probe
		state->scan_structure = hash_table->Probe(state->join_keys);
		state->scan_structure->Next(state->join_keys, state->child_chunk, chunk);
	} while (chunk.size() == 0);
}

void PhysicalRAIJoin::PerformRJoin(ClientContext &context, DataChunk &chunk, PhysicalOperatorState *state_) {
	auto state = reinterpret_cast<PhysicalRAIJoinState *>(state_);
	do {
		ProbeHashTable(context, chunk, state);
#if STANDARD_VECTOR_SIZE >= 128
		if (chunk.size() == 0) {
			if (state->cached_chunk.size() > 0) {
				// finished probing but cached data remains, return cached chunk
				chunk.Reference(state->cached_chunk);
				state->cached_chunk.Reset();
			}
			return;
		} else if (chunk.size() < 64) {
			// small chunk: add it to chunk cache and continue
			state->cached_chunk.Append(chunk);
			if (state->cached_chunk.size() >= (STANDARD_VECTOR_SIZE - 64)) {
				// chunk cache full: return it
				chunk.Reference(state->cached_chunk);
				state->cached_chunk.Reset();
				return;
			} else {
				// chunk cache not full: probe again
				chunk.Reset();
			}
		} else {
			return;
		}
#else
		return;
#endif
	} while (true);
}

/*
 * AJoin
 */
struct RefineInnerJoin {
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
static idx_t inner_join_operator(Vector &left, Vector &right, SelectionVector &sel, idx_t left_size, idx_t right_size,
                                 SelectionVector &lvector, SelectionVector &rvector, idx_t current_match_count) {
	switch (left.type) {
	case TypeId::BOOL:
	case TypeId::INT8:
		return RefineInnerJoin::template Operation<int8_t, OP>(left, right, sel, left_size, right_size, lvector,
		                                                       rvector, current_match_count);
	case TypeId::INT16:
		return RefineInnerJoin::template Operation<int16_t, OP>(left, right, sel, left_size, right_size, lvector,
		                                                        rvector, current_match_count);
	case TypeId::INT32:
		return RefineInnerJoin::template Operation<int32_t, OP>(left, right, sel, left_size, right_size, lvector,
		                                                        rvector, current_match_count);
	case TypeId::INT64:
		return RefineInnerJoin::template Operation<int64_t, OP>(left, right, sel, left_size, right_size, lvector,
		                                                        rvector, current_match_count);
	case TypeId::FLOAT:
		return RefineInnerJoin::template Operation<float, OP>(left, right, sel, left_size, right_size, lvector, rvector,
		                                                      current_match_count);
	case TypeId::DOUBLE:
		return RefineInnerJoin::template Operation<double, OP>(left, right, sel, left_size, right_size, lvector,
		                                                       rvector, current_match_count);
	case TypeId::VARCHAR:
		return RefineInnerJoin::template Operation<string_t, OP>(left, right, sel, left_size, right_size, lvector,
		                                                         rvector, current_match_count);
	default:
		throw NotImplementedException("Unimplemented type for join!");
	}
}

static idx_t RefineJoin(Vector &left, Vector &right, SelectionVector &sel, idx_t left_size, idx_t right_size,
                        SelectionVector &lvector, SelectionVector &rvector, idx_t current_match_count,
                        ExpressionType comparison_type) {
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

template <bool JOIN_RAI>
idx_t PhysicalRAIJoin::DoAJoin(ClientContext &context, DataChunk &right_chunk, DataChunk &right_condition_chunk,
                               SelectionVector &lvector, SelectionVector &rvector, PhysicalOperatorState *state_) {
	auto state = reinterpret_cast<PhysicalRAIJoinState *>(state_);
	SelectionVector sel;
	sel.InitializeCopy((sel_t *)FlatVector::incremental_vector);
	if (JOIN_RAI) {
		auto &rai_info = conditions[0].rais[0];
		state->rai_chunk.rai_forward = rai_info->forward;
		rai_info->rai->GetChunk(right_chunk, right_condition_chunk, state->rai_chunk, state->left_tuple,
		                        state->right_tuple, rai_info->forward);
		children[0]->GetChunk(context, state->child_chunk, state->child_state.get(), &sel,
		                      &state->rai_chunk.data[state->rai_chunk.column_count() - 2], &state->rai_chunk);
	} else {
		state->rai_chunk.Reference(right_chunk);
		state->right_chunk_idx++;
		children[0]->GetChunk(context, state->child_chunk, state->child_state.get(), &sel,
		                      &right_condition_chunk.data[0], &state->rai_chunk);
	}
	idx_t match_count = state->child_chunk.size();
	if (match_count == 0) {
		return 0;
	}
	state->rai_chunk.Slice(sel, match_count);
	//	right_condition_chunk.Slice(lvector, match_count);
	if (conditions.size() > 1) {
		lvector.Initialize();
		rvector.Initialize();
		state->lhs_executor.Execute(state->child_chunk, state->join_keys);
		for (idx_t i = 1; i < conditions.size(); i++) {
			if (match_count == 0) {
				continue;
			}
			Vector &left = state->join_keys.data[i];
			Vector &right = right_condition_chunk.data[i];
			match_count = RefineJoin(left, right, sel, state->join_keys.size(), state->rai_chunk.size(), lvector,
			                         rvector, match_count, conditions[i].comparison);
		}
	}

	return match_count;
}

void PhysicalRAIJoin::PerformAJoin(ClientContext &context, DataChunk &chunk, PhysicalOperatorState *state_) {
	auto state = reinterpret_cast<PhysicalRAIJoinState *>(state_);
	if (state->initialized && (state->right_chunk_idx == state->right_chunks.chunks.size())) {
		return;
	}
	// get left chunk
	do {
		if (state->right_chunk_idx < state->right_chunks.chunks.size() &&
		    state->right_tuple >= state->right_chunks.chunks[state->right_chunk_idx]->size()) {
			state->right_chunk_idx++;
			state->right_tuple = 0;
			state->left_tuple = 0;
		}
		if (state->right_chunk_idx >= state->right_chunks.chunks.size()) {
			return;
		}
		idx_t match_count;
		SelectionVector lvector;
		SelectionVector rvector;
		lvector.InitializeCopy((sel_t *)FlatVector::incremental_vector);
		rvector.InitializeCopy((sel_t *)FlatVector::incremental_vector);
		auto &right_chunk = *state->right_chunks.chunks[state->right_chunk_idx];
		state->rhs_executor.Execute(right_chunk, state->right_condition_chunk);
		if (state->join_rai) {
			match_count = DoAJoin<true>(context, right_chunk, state->right_condition_chunk, lvector, rvector, state);
		} else {
			match_count = DoAJoin<false>(context, right_chunk, state->right_condition_chunk, lvector, rvector, state);
		}
		auto left_column_count = state->child_chunk.column_count();
		chunk.Slice(state->child_chunk, FlatVector::IncrementalSelectionVector, match_count);
		if (right_projection_map.size() == 0) {
			chunk.Slice(state->rai_chunk, rvector, match_count, left_column_count,
			            chunk.column_count() - left_column_count);
		} else {
			assert(right_projection_map.size() + left_column_count <= chunk.column_count());
			chunk.Slice(state->rai_chunk, rvector, match_count, left_column_count, right_projection_map);
		}
		chunk.SetCardinality(match_count);
	} while (chunk.size() == 0);
}

void PhysicalRAIJoin::GetChunkInternal(ClientContext &context, DataChunk &chunk, PhysicalOperatorState *state_,
                                       SelectionVector *sel, Vector *rid_vector, DataChunk *rai_chunk) {
#if ENABLE_RID_HT
	auto state = reinterpret_cast<PhysicalRAIJoinState *>(state_);
	if (!state->initialized) {
		state->cached_chunk.Initialize(types);
		// build the HT
		auto right_state = children[1]->GetOperatorState();
		auto types = children[1]->GetTypes();
		auto rai_types = types;
		rai_types.push_back(TypeId::INT64);
		rai_types.push_back(TypeId::INT64);
		DataChunk right_chunk, build_chunk;
		right_chunk.Initialize(types);
		if (right_projection_map.size() > 0) {
			build_chunk.InitializeEmpty(hash_table->build_types);
		}
		state->join_keys.InitializeEmpty(hash_table->condition_types);
		state->right_condition_chunk.InitializeEmpty(hash_table->condition_types);
		state->rai_chunk.Initialize(rai_types);
		auto &rai_info = conditions[0].rais[0];
		chooseRJ = true;
		while (true) {
			// get the child chunk
			children[1]->GetChunk(context, right_chunk, right_state.get());
			if (right_chunk.size() == 0) {
				break;
			}
			right_side_size += right_chunk.size();
			AppendHashTable(state_, right_chunk, build_chunk);
		}

		state->initialized = true;
		if (hash_table->size() == 0 && state->right_chunks.count == 0 &&
		    (hash_table->join_type == JoinType::INNER || hash_table->join_type == JoinType::SEMI)) {
			// empty hash table with INNER or SEMI join means empty result set
			return;
		}
	}

	chooseRJ = true;
	PerformRJoin(context, chunk, state);
#elif ENABLE_LOOKUP_JOIN
	auto state = reinterpret_cast<PhysicalRAIJoinState *>(state_);
	if (!state->initialized) {
		state->cached_chunk.Initialize(types);
		// build the HT
		auto right_state = children[1]->GetOperatorState();
		auto types = children[1]->GetTypes();
		auto rai_types = types;
		rai_types.push_back(TypeId::INT64);
		rai_types.push_back(TypeId::INT64);
		DataChunk right_chunk, build_chunk;
		right_chunk.Initialize(types);
		if (right_projection_map.size() > 0) {
			build_chunk.InitializeEmpty(hash_table->build_types);
		}
		state->join_keys.InitializeEmpty(hash_table->condition_types);
		state->right_condition_chunk.InitializeEmpty(hash_table->condition_types);
		state->rai_chunk.Initialize(rai_types);
		auto &rai_info = conditions[0].rais[0];
		bool enable_alist = rai_info->rai_type >= RAIType::EDGE_SOURCE ||
		                    ((rai_info->forward == false) && rai_info->rai_type == RAIType::EDGE_TARGET);
		chooseRJ = !enable_lookup_join;
		state->join_rai = enable_alist;
		if (chooseRJ) {
			while (true) {
				// get the child chunk
				children[1]->GetChunk(context, right_chunk, right_state.get());
				if (right_chunk.size() == 0) {
					break;
				}
				AppendHashTable(state_, right_chunk, build_chunk);
			}
		} else {
			while (true) {
				// get the child chunk
				children[1]->GetChunk(context, right_chunk, right_state.get());
				if (right_chunk.size() == 0) {
					break;
				}
				state->right_chunks.Append(right_chunk);
			}
		}

		state->initialized = true;
		if (hash_table->size() == 0 && state->right_chunks.count == 0 &&
		    (hash_table->join_type == JoinType::INNER || hash_table->join_type == JoinType::SEMI)) {
			// empty hash table with INNER or SEMI join means empty result set
			return;
		}
	}

	if (chooseRJ) {
		PerformRJoin(context, chunk, state);
	} else {
		PerformAJoin(context, chunk, state);
	}
#elif ENABLE_LOOKUP_FILTER
	auto state = reinterpret_cast<PhysicalRAIJoinState *>(state_);
	if (!state->initialized) {
		state->cached_chunk.Initialize(types);
		// build the HT
		auto right_state = children[1]->GetOperatorState();
		auto types = children[1]->GetTypes();
		auto rai_types = types;
		rai_types.push_back(TypeId::INT64);
		rai_types.push_back(TypeId::INT64);
		DataChunk right_chunk, build_chunk;
		right_chunk.Initialize(types);
		if (right_projection_map.size() > 0) {
			build_chunk.InitializeEmpty(hash_table->build_types);
		}
		state->join_keys.InitializeEmpty(hash_table->condition_types);
		state->right_condition_chunk.InitializeEmpty(hash_table->condition_types);
		state->rai_chunk.Initialize(rai_types);
		auto &rai_info = conditions[0].rais[0];
		bool enable_alist = rai_info->rai_type >= RAIType::EDGE_SOURCE ||
		                    ((rai_info->forward == false) && rai_info->rai_type == RAIType::EDGE_TARGET);

		while (true) {
			// get the child chunk
			children[1]->GetChunk(context, right_chunk, right_state.get());
			if (right_chunk.size() == 0) {
				break;
			}
			right_side_size += right_chunk.size();
			if (enable_alist) {
				state->rhs_executor.Execute(right_chunk, state->join_keys);
				state->max_join_output_size += GetExactChunkJoinSize(*rai_info, state->join_keys);
			} else {
				state->max_join_output_size += right_chunk.size();
			}
			AppendHashTable(state_, right_chunk, build_chunk);
		}

		if (hash_table->size() > 0 && rai_info->passing_tables[0] > 0) {
			PassRowsFilter(state->max_join_output_size);
		}
		pass_rows_filter = true;
		state->initialized = true;
		if (hash_table->size() == 0 && state->right_chunks.count == 0 &&
		    (hash_table->join_type == JoinType::INNER || hash_table->join_type == JoinType::SEMI)) {
			// empty hash table with INNER or SEMI join means empty result set
			return;
		}
	}

	chooseRJ = true;
	PerformRJoin(context, chunk, state);
#elif ENABLE_ZONE_FILTER
	auto state = reinterpret_cast<PhysicalRAIJoinState *>(state_);
	if (!state->initialized) {
		state->cached_chunk.Initialize(types);
		// build the HT
		auto right_state = children[1]->GetOperatorState();
		auto types = children[1]->GetTypes();
		auto rai_types = types;
		rai_types.push_back(TypeId::INT64);
		rai_types.push_back(TypeId::INT64);
		DataChunk right_chunk, build_chunk;
		right_chunk.Initialize(types);
		if (right_projection_map.size() > 0) {
			build_chunk.InitializeEmpty(hash_table->build_types);
		}
		state->join_keys.InitializeEmpty(hash_table->condition_types);
		state->right_condition_chunk.InitializeEmpty(hash_table->condition_types);
		state->rai_chunk.Initialize(rai_types);
		auto &rai_info = conditions[0].rais[0];
		while (true) {
			// get the child chunk
			children[1]->GetChunk(context, right_chunk, right_state.get());
			if (right_chunk.size() == 0) {
				break;
			}
			AppendHashTable(state_, right_chunk, build_chunk);
		}

		if (hash_table->size() > 0 && rai_info->passing_tables[0] > 0) {
			PassZoneFilter();
		}
		pass_zone_filter = true;
		state->initialized = true;
		if (hash_table->size() == 0 && state->right_chunks.count == 0 &&
		    (hash_table->join_type == JoinType::INNER || hash_table->join_type == JoinType::SEMI)) {
			// empty hash table with INNER or SEMI join means empty result set
			return;
		}
	}

	chooseRJ = true;
	PerformRJoin(context, chunk, state);
#elif ENABLE_ADAPTIVE_FILTER
	auto state = reinterpret_cast<PhysicalRAIJoinState *>(state_);
	if (!state->initialized) {
		state->cached_chunk.Initialize(types);
		// build the HT
		auto right_state = children[1]->GetOperatorState();
		auto types = children[1]->GetTypes();
		auto rai_types = types;
		rai_types.push_back(TypeId::INT64);
		rai_types.push_back(TypeId::INT64);
		DataChunk right_chunk, build_chunk;
		right_chunk.Initialize(types);
		if (right_projection_map.size() > 0) {
			build_chunk.InitializeEmpty(hash_table->build_types);
		}
		state->join_keys.InitializeEmpty(hash_table->condition_types);
		state->right_condition_chunk.InitializeEmpty(hash_table->condition_types);
		state->rai_chunk.Initialize(rai_types);
		auto &rai_info = conditions[0].rais[0];
		bool enable_alist = rai_info->rai_type >= RAIType::EDGE_SOURCE ||
		                    ((rai_info->forward == false) && rai_info->rai_type == RAIType::EDGE_TARGET);
		while (true) {
			// get the child chunk
			children[1]->GetChunk(context, right_chunk, right_state.get());
			if (right_chunk.size() == 0) {
				break;
			}
			if (enable_alist && state->max_join_output_size <= RAIJoin::MAX_ROWS_SIZE) {
				state->rhs_executor.Execute(right_chunk, state->join_keys);
				state->max_join_output_size += GetExactChunkJoinSize(*rai_info, state->join_keys);
			} else {
				state->max_join_output_size += right_chunk.size();
			}
			AppendHashTable(state_, right_chunk, build_chunk);
		}

		if (state->max_join_output_size > 0 && rai_info->passing_tables[0] > 0) {
			if (state->max_join_output_size <= RAIJoin::MAX_ROWS_SIZE) {
				PassRowsFilter(state->max_join_output_size);
				pass_rows_filter = true;
			} else {
				PassZoneFilter();
				pass_zone_filter = true;
			}
		}
		state->initialized = true;
		if (hash_table->size() == 0 && state->right_chunks.count == 0 &&
		    (hash_table->join_type == JoinType::INNER || hash_table->join_type == JoinType::SEMI)) {
			// empty hash table with INNER or SEMI join means empty result set
			return;
		}
	}

	chooseRJ = true;
	PerformRJoin(context, chunk, state);
#elif ENABLE_ADAPTIVE_JOIN
	auto state = reinterpret_cast<PhysicalRAIJoinState *>(state_);
	if (!state->initialized) {
		state->cached_chunk.Initialize(types);
		// build the HT
		auto right_state = children[1]->GetOperatorState();
		auto types = children[1]->GetTypes();
		auto rai_types = types;
		rai_types.push_back(TypeId::INT64);
		rai_types.push_back(TypeId::INT64);
		DataChunk right_chunk, build_chunk;
		right_chunk.Initialize(types);
		if (right_projection_map.size() > 0) {
			build_chunk.InitializeEmpty(hash_table->build_types);
		}
		state->join_keys.InitializeEmpty(hash_table->condition_types);
		state->right_condition_chunk.InitializeEmpty(hash_table->condition_types);
		state->rai_chunk.Initialize(rai_types);
		auto &rai_info = conditions[0].rais[0];
		bool enable_alist = rai_info->rai_type >= RAIType::EDGE_SOURCE ||
		                    ((rai_info->forward == false) && rai_info->rai_type == RAIType::EDGE_TARGET);
		while (true) {
			// get the child chunk
			children[1]->GetChunk(context, right_chunk, right_state.get());
			if (right_chunk.size() == 0) {
				break;
			}
			right_side_size += right_chunk.size();
			if (state->max_join_output_size <= RAIJoin::MAX_ROWS_SIZE) {
				state->right_chunks.Append(right_chunk);
				if (enable_alist) { // if rai_type is EDGE_SOURCE or SELF or backward EDGE_TARGET
					state->rhs_executor.Execute(right_chunk, state->join_keys);
					state->max_join_output_size += GetExactChunkJoinSize(*rai_info, state->join_keys);
				} else {
					state->max_join_output_size += right_chunk.size();
				}
			} else {
				AppendHashTable(state_, right_chunk, build_chunk);
			}
		}

		double passing_rate = 0.0;
		if (enable_alist) {
			double avg_degree = conditions[0].rais[0]->GetAverageDegree(rai_info->rai_type, rai_info->forward);
			double left_card = (double)conditions[0].rais[0]->left_cardinalities[0];
			passing_rate = (right_side_size * avg_degree) / left_card;
		} else {
			double left_card = (double)conditions[0].rais[0]->left_cardinalities[0];
			passing_rate = right_side_size / left_card;
		}
		chooseRJ = true;
		if (passing_rate <= RAIJoin::MAX_ZONE_RATE && state->max_join_output_size <= RAIJoin::MAX_ROWS_SIZE &&
		    enable_lookup_join) {
			chooseRJ = false;
			state->join_rai = enable_alist;
		}
		if (chooseRJ) {
			for (auto &rchunk : state->right_chunks.chunks) {
				AppendHashTable(state, *rchunk, build_chunk);
			}
			if (state->max_join_output_size > 0 && passing_rate <= RAIJoin::MAX_ZONE_RATE &&
			    rai_info->passing_tables[0] > 0) {
				if (state->max_join_output_size <= RAIJoin::MAX_ROWS_SIZE) {
					PassRowsFilter(state->max_join_output_size);
					pass_rows_filter = true;
				} else {
					PassZoneFilter();
					pass_zone_filter = true;
				}
			}
		}
		state->initialized = true;
		if (hash_table->size() == 0 && state->right_chunks.count == 0 &&
		    (hash_table->join_type == JoinType::INNER || hash_table->join_type == JoinType::SEMI)) {
			// empty hash table with INNER or SEMI join means empty result set
			return;
		}
	}

	if (chooseRJ) {
		PerformRJoin(context, chunk, state);
	} else {
		PerformAJoin(context, chunk, state);
	}
#endif
}

unique_ptr<PhysicalOperatorState> PhysicalRAIJoin::GetOperatorState() {
	return make_unique<PhysicalRAIJoinState>(children[0].get(), children[1].get(), conditions);
}

string PhysicalRAIJoin::ExtraRenderInformation() const {
	string extra_info;
	extra_info = "R_JOIN[";
	extra_info += JoinTypeToString(type);
	extra_info += ", build: ";
	extra_info +=
	    to_string(right_projection_map.size() == 0 ? children[1]->GetTypes().size() : right_projection_map.size());
	extra_info += "]\n";
	for (auto &it : conditions) {
		string op = ExpressionTypeToOperator(it.comparison);
		extra_info += it.left->GetName() + op + it.right->GetName() + "\n";
		for (auto &rai : it.rais) {
			extra_info += rai->ToString() + "\n";
		}
	}
	return extra_info;
}
