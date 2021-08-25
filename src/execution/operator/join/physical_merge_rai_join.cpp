#include "duckdb/execution/operator/join/physical_merge_rai_join.hpp"

#include "duckdb/execution/operator/scan/physical_table_scan.hpp"

using namespace duckdb;
using namespace std;

class PhysicalMergeRAIJoinState : public PhysicalComparisonJoinState {
public:
	PhysicalMergeRAIJoinState(PhysicalOperator *left, PhysicalOperator *right, vector<JoinCondition> &conditions)
	    : PhysicalComparisonJoinState(left, right, conditions), initialized(false), right_chunk_idx(0), right_tuple(0),
	      left_tuple(0), max_join_output_size(0) {
	}

	bool initialized;
	DataChunk cached_chunk;
	DataChunk join_keys;
	ChunkCollection im_chunks;
	// state for RJoin
	unique_ptr<RAIHashTable::RAIScanStructure> scan_structure;
	// state for AJoin
	DataChunk right_condition_chunk;
	idx_t right_chunk_idx;
	idx_t right_tuple;
	idx_t left_tuple;

	idx_t max_join_output_size;
};

PhysicalMergeRAIJoin::PhysicalMergeRAIJoin(ClientContext &context, LogicalOperator &op,
                                           unique_ptr<PhysicalOperator> left, unique_ptr<PhysicalOperator> right,
                                           vector<JoinCondition> cond, JoinType join_type,
                                           vector<idx_t> &left_projection_map, vector<idx_t> &right_projection_map,
                                           idx_t left_cardinality, bool enable_lookup_join)
    : PhysicalComparisonJoin(op, PhysicalOperatorType::MERGE_RAI_JOIN, move(cond), join_type), hash_table(nullptr),
      right_projection_map(right_projection_map), build_cardinality(left_cardinality), right_side_size(0),
      enable_lookup_join(enable_lookup_join) {
	children.push_back(move(left));
	children.push_back(move(right));

	assert(left_projection_map.size() == 0);

	hash_table = make_unique<RAIHashTable>(BufferManager::GetBufferManager(context), conditions,
	                                       LogicalOperator::MapTypes(children[1]->GetTypes(), right_projection_map),
	                                       type, build_cardinality);
}

void PhysicalMergeRAIJoin::AppendHashTable(PhysicalOperatorState *state_, DataChunk &chunk, DataChunk &build_chunk) {
	auto state = reinterpret_cast<PhysicalMergeRAIJoinState *>(state_);
	state->join_keys.SetCardinality(chunk);
	state->join_keys.data[0].Reference(chunk.data[chunk.column_count() - 1]);
	if (hash_table->initialized == false) {
		hash_table->Initialize();
	}
	if (right_projection_map.size() > 0) {
		build_chunk.SetCardinality(chunk);
		for (idx_t i = 0; i < right_projection_map.size(); i++) {
			build_chunk.data[i].Reference(chunk.data[right_projection_map[i]]);
		}
	} else {
		build_chunk.SetCardinality(chunk);
		for (idx_t i = 0; i < build_chunk.column_count(); i++) {
			build_chunk.data[i].Reference(chunk.data[i]);
		}
	}
	hash_table->Build(state->join_keys, build_chunk);
}

void PhysicalMergeRAIJoin::PassZoneFilter() {
	auto &rai_info = conditions[0].rais[0];
	rai_info->row_bitmask = make_shared<bitmask_vector>(rai_info->left_cardinalities[0]);
	hash_table->GetZoneFilter(*rai_info->row_bitmask);
	//	auto scan = reinterpret_cast<PhysicalTableScan *>(passing_scan_ops[0]);
	//	scan->PassZoneFilter(rai_info->passing_tables[0], rai_info->zone_filter);
	children[0]->PushdownZoneFilter(rai_info->passing_tables[0], rai_info->row_bitmask, rai_info->zone_bitmask);
}

void PhysicalMergeRAIJoin::PassRowsFilter(idx_t rows_size) {
	auto &rai_info = conditions[0].rais[0];
	auto rows_filter = make_shared<rows_vector>(rows_size);
	auto rows_filter_size = hash_table->GetRowsFilter(*rows_filter);
	children[0]->PushdownRowsFilter(rai_info->passing_tables[0], rows_filter, rows_filter_size);
}

void PhysicalMergeRAIJoin::ProbeHashTable(ClientContext &context, DataChunk &chunk, PhysicalOperatorState *state_) {
	auto state = reinterpret_cast<PhysicalMergeRAIJoinState *>(state_);
	if (state->child_chunk.size() > 0 && state->scan_structure) {
		// still have elements remaining from the previous probe (i.e. we got
		// >1024 elements in the previous probe)
		// auto ht_next_start = std::chrono::high_resolution_clock::now();
		state->scan_structure->Next(state->join_keys, state->child_chunk, chunk);
		// auto ht_next_end = std::chrono::high_resolution_clock::now();
		// ht_next_time += (ht_next_end - ht_next_start).count();
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
		// auto ht_expression_start = std::chrono::high_resolution_clock::now();
		state->lhs_executor.Execute(state->child_chunk, state->join_keys);

		// perform the actual probe
		// auto ht_probe_start = std::chrono::high_resolution_clock::now();
		state->scan_structure = hash_table->Probe(state->join_keys);
		// auto ht_probe_end = std::chrono::high_resolution_clock::now();
		state->scan_structure->Next(state->join_keys, state->child_chunk, chunk);
		// auto ht_next_end = std::chrono::high_resolution_clock::now();
		// ht_expr_time += (ht_probe_start - ht_expression_start).count();
		// ht_probe_time += (ht_probe_end - ht_probe_start).count();
		// ht_next_time += (ht_next_end - ht_probe_end).count();
	} while (chunk.size() == 0);
}

void PhysicalMergeRAIJoin::PerformRJoin(ClientContext &context, DataChunk &chunk, PhysicalOperatorState *state_) {
	auto state = reinterpret_cast<PhysicalMergeRAIJoinState *>(state_);
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

idx_t PhysicalMergeRAIJoin::DoAJoin(ClientContext &context, DataChunk &right_chunk, DataChunk &right_condition_chunk,
                                    SelectionVector &lvector, SelectionVector &rvector, PhysicalOperatorState *state_) {
	auto state = reinterpret_cast<PhysicalMergeRAIJoinState *>(state_);
	SelectionVector sel(STANDARD_VECTOR_SIZE);
	for (idx_t i = 0; i < STANDARD_VECTOR_SIZE; i++) {
		sel.set_index(i, i);
	}
	state->right_chunk_idx++;
	children[0]->GetChunk(context, state->child_chunk, state->child_state.get(), &sel, &right_condition_chunk.data[0],
	                      &right_chunk);
	idx_t match_count = state->child_chunk.size();
	if (match_count == 0) {
		return 0;
	}
	right_chunk.Slice(sel, match_count);

	return match_count;
}

void PhysicalMergeRAIJoin::PerformAJoin(ClientContext &context, DataChunk &chunk, PhysicalOperatorState *state_) {
	auto state = reinterpret_cast<PhysicalMergeRAIJoinState *>(state_);
	if (state->initialized && (state->right_chunk_idx == state->im_chunks.chunks.size())) {
		return;
	}
	// get left chunk
	do {
		if (state->right_chunk_idx < state->im_chunks.chunks.size() &&
		    state->right_tuple >= state->im_chunks.chunks[state->right_chunk_idx]->size()) {
			state->right_chunk_idx++;
			state->right_tuple = 0;
			state->left_tuple = 0;
		}
		if (state->right_chunk_idx >= state->im_chunks.chunks.size()) {
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
		auto &im_chunk = *state->im_chunks.chunks[state->right_chunk_idx];
		state->right_condition_chunk.data[0].Reference(im_chunk.data[im_chunk.column_count() - 1]);
		match_count = DoAJoin(context, im_chunk, state->right_condition_chunk, lvector, rvector, state);
		auto left_column_count = state->child_chunk.column_count();
		chunk.Slice(state->child_chunk, FlatVector::IncrementalSelectionVector, match_count);
		if (right_projection_map.size() == 0) {
			chunk.Slice(im_chunk, rvector, match_count, left_column_count, chunk.column_count() - left_column_count);
		} else {
			assert(right_projection_map.size() + left_column_count <= chunk.column_count());
			chunk.Slice(im_chunk, rvector, match_count, left_column_count, right_projection_map);
		}
		chunk.SetCardinality(match_count);
	} while (chunk.size() == 0);
}

void PhysicalMergeRAIJoin::GetChunkInternal(ClientContext &context, DataChunk &chunk, PhysicalOperatorState *state_,
                                            SelectionVector *sel, Vector *rid_vector, DataChunk *rai_chunk) {
	auto state = reinterpret_cast<PhysicalMergeRAIJoinState *>(state_);
	if (!state->initialized) {
		state->cached_chunk.Initialize(types);
		// join right with alist
		auto right_state = children[1]->GetOperatorState();
		DataChunk right_chunk, build_chunk, im_chunk; // im_chunk: intermediate_chunk, [right_chunk.columns, tid]
		right_chunk.Initialize(children[1]->GetTypes());
		auto im_types = right_chunk.GetTypes();
		im_types.push_back(TypeId::INT64);
		im_chunk.Initialize(im_types);
		build_chunk.InitializeEmpty(hash_table->build_types);
		state->join_keys.InitializeEmpty(hash_table->condition_types);
		state->right_condition_chunk.InitializeEmpty(hash_table->condition_types);
		auto &rai_info = conditions[0].rais[0];
		while (true) {
			children[1]->GetChunk(context, right_chunk, right_state.get());
			state->rhs_executor.Execute(right_chunk, state->right_condition_chunk);
			state->right_tuple = 0;
			state->left_tuple = 0;
			if (right_chunk.size() == 0) {
				break;
			}
			do {
				rai_info->rai->GetVertexes(right_chunk, state->right_condition_chunk, im_chunk, state->left_tuple,
				                           state->right_tuple, true);
				if (state->max_join_output_size <= RAIJoin::MAX_ROWS_SIZE) {
					state->im_chunks.Append(im_chunk);
				} else {
					AppendHashTable(state_, im_chunk, build_chunk);
				}
				state->max_join_output_size += im_chunk.size();
			} while (state->right_tuple < right_chunk.size());
		}
		state->right_tuple = 0;
		state->left_tuple = 0;
		if (state->max_join_output_size > RAIJoin::MAX_ROWS_SIZE) {
			chooseRJ = true;
			for (auto &rchunk : state->im_chunks.chunks) {
				AppendHashTable(state, *rchunk, build_chunk);
			}
			double left_card = (double)conditions[0].rais[0]->left_cardinalities[0];
			double passing_rate = state->max_join_output_size / left_card;
			if (passing_rate <= RAIJoin::MAX_ZONE_RATE) {
				PassZoneFilter();
			}
		} else {
			if (enable_lookup_join) {
				chooseRJ = false;
			} else if (state->max_join_output_size > 0) {
				chooseRJ = true;
				for (auto &rchunk : state->im_chunks.chunks) {
					AppendHashTable(state, *rchunk, build_chunk);
				}
				PassRowsFilter(state->max_join_output_size);
			}
		}
		state->initialized = true;
		if (hash_table->size() == 0 && state->im_chunks.count == 0 &&
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
}

unique_ptr<PhysicalOperatorState> PhysicalMergeRAIJoin::GetOperatorState() {
	return make_unique<PhysicalMergeRAIJoinState>(children[0].get(), children[1].get(), conditions);
}

string PhysicalMergeRAIJoin::ExtraRenderInformation() const {
	string extra_info = "M_R_JOIN";
	if (chooseRJ) {
		extra_info += "[RJ]";
	} else {
		extra_info += "[AJ]";
	}
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
