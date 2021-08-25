#include "duckdb/execution/operator/join/physical_adaptive_merge_sip_join.hpp"

#include "duckdb/execution/operator/scan/physical_table_scan.hpp"

#include <iostream>

using namespace duckdb;
using namespace std;

class PhysicalAdaptiveMergeSIPJoinState : public PhysicalComparisonJoinState {
public:
	PhysicalAdaptiveMergeSIPJoinState(PhysicalOperator *left, PhysicalOperator *right,
	                                  vector<JoinCondition> &conditions)
	    : PhysicalComparisonJoinState(left, right, conditions), initialized(false), right_chunk_idx(0), right_tuple(0),
	      left_tuple(0), estimate_join_size(0) {
	}

	bool initialized;
	DataChunk cached_chunk;
	DataChunk join_keys;
	ChunkCollection im_chunks;
	// state for SHJoin
	unique_ptr<SIPHashTable::SIPScanStructure> scan_structure;
	// state for NLAJoin
	DataChunk right_condition_chunk;
	idx_t right_chunk_idx;
	idx_t right_tuple;
	idx_t left_tuple;
	// state for adaptive SJ
	idx_t estimate_join_size;
};

PhysicalAdaptiveMergeSIPJoin::PhysicalAdaptiveMergeSIPJoin(
    ClientContext &context, LogicalOperator &op, unique_ptr<PhysicalOperator> left, unique_ptr<PhysicalOperator> right,
    vector<JoinCondition> cond, JoinType join_type, vector<idx_t> &left_projection_map,
    vector<idx_t> &right_projection_map, vector<idx_t> &merge_projection_map, bool enable_nlaj)
    : PhysicalComparisonJoin(op, PhysicalOperatorType::ADAPTIVE_MERGE_SIP_JOIN, move(cond), join_type),
      hash_table(nullptr), right_projection_map(right_projection_map), enableNLAJ(enable_nlaj) {
	children.push_back(move(left));
	children.push_back(move(right));

	assert(left_projection_map.size() == 0);

	hash_table =
	    make_unique<SIPHashTable>(BufferManager::GetBufferManager(context), conditions,
	                              LogicalOperator::MapTypes(children[1]->GetTypes(), right_projection_map), type);
}

/*
 * M_SHJ
 * */
// todo:
//  3. add SIMD to optimize filter generation
void PhysicalAdaptiveMergeSIPJoin::GenerateZoneFilter(Vector &key_vector, idx_t count) {
	auto &rai_info = conditions[0].rais[0];
	VectorData key_data;
	key_vector.Orrify(count, key_data);
	auto *keys = (int64_t *)key_data.data;
	auto key_nullmask = *key_data.nullmask;
	auto &zone_filter = *rai_info->row_bitmask;
	auto &zone_sel = *rai_info->zone_bitmask;

	auto alist = rai_info->compact_list;
	// generate zone filters
	if (alist) {
		// with alist
		if (key_nullmask.any()) {
			if (rai_info->passing_tables[1] == 0) {
				for (idx_t i = 0; i < count; i++) {
					if (key_nullmask[i]) {
						continue;
					}
					idx_t pos = key_data.sel->get_index(i);
					auto offset = alist->offsets[keys[pos]];
					auto size = alist->sizes[keys[pos]];
					for (idx_t j = 0; j < size; j++) {
						auto id = alist->edges.operator[](offset + j);
						zone_filter[id] = true;
						auto zone_id = id / STANDARD_VECTOR_SIZE;
						zone_sel[zone_id] = true;
					}
				}
			} else {
				auto &extra_zone_filter = *rai_info->extra_row_bitmask;
				auto &extra_zone_sel = *rai_info->extra_zone_bitmask;
				for (idx_t i = 0; i < count; i++) {
					if (key_nullmask[i]) {
						continue;
					}
					idx_t pos = key_data.sel->get_index(i);
					auto offset = alist->offsets[keys[pos]];
					auto size = alist->sizes[keys[pos]];
					for (idx_t j = 0; j < size; j++) {
						auto id = alist->edges.operator[](offset + j);
						zone_filter[id] = true;
						auto zone_id = id / STANDARD_VECTOR_SIZE;
						zone_sel[zone_id] = true;
					}
					for (idx_t j = 0; j < size; j++) {
						auto id = alist->vertices.operator[](offset + j);
						extra_zone_filter[id] = true;
						auto zone_id = id / STANDARD_VECTOR_SIZE;
						extra_zone_sel[zone_id] = true;
					}
				}
			}
		} else {
			if (rai_info->passing_tables[1] == 0) {
				for (idx_t i = 0; i < count; i++) {
					idx_t pos = key_data.sel->get_index(i);
					auto offset = alist->offsets[keys[pos]];
					auto size = alist->sizes[keys[pos]];
					for (idx_t j = 0; j < size; j++) {
						auto id = alist->edges.operator[](offset + j);
						zone_filter[id] = true;
						auto zone_id = id / STANDARD_VECTOR_SIZE;
						zone_sel[zone_id] = true;
					}
				}
			} else {
				auto &extra_zone_filter = *rai_info->extra_row_bitmask;
				auto &extra_zone_sel = *rai_info->extra_zone_bitmask;
				for (idx_t i = 0; i < count; i++) {
					idx_t pos = key_data.sel->get_index(i);
					auto offset = alist->offsets[keys[pos]];
					auto size = alist->sizes[keys[pos]];
					for (idx_t j = 0; j < size; j++) {
						auto id = alist->edges.operator[](offset + j);
						zone_filter[id] = true;
						auto zone_id = id / STANDARD_VECTOR_SIZE;
						zone_sel[zone_id] = true;
					}
					for (idx_t j = 0; j < size; j++) {
						auto id = alist->vertices.operator[](offset + j);
						extra_zone_filter[id] = true;
						auto zone_id = id / STANDARD_VECTOR_SIZE;
						extra_zone_sel[zone_id] = true;
					}
				}
			}
		}
	} else {
		// without alist
		if (key_nullmask.any()) {
			for (idx_t i = 0; i < count; i++) {
				if (key_nullmask[i]) {
					continue;
				}
				idx_t pos = key_data.sel->get_index(i);
				zone_filter[keys[pos]] = true;
				auto zone_id = keys[pos] / STANDARD_VECTOR_SIZE;
				zone_sel[zone_id] = true;
			}
		} else {
			for (idx_t i = 0; i < count; i++) {
				idx_t pos = key_data.sel->get_index(i);
				zone_filter[keys[pos]] = true;
				auto zone_id = keys[pos] / STANDARD_VECTOR_SIZE;
				zone_sel[zone_id] = true;
			}
		}
	}
}

void PhysicalAdaptiveMergeSIPJoin::InitializeAList() {
	auto &rai_info = conditions[0].rais[0];
	// determine the alist for usage
	switch (rai_info->rai_type) {
	case RAIType::TARGET_EDGE: {
		rai_info->forward = true;
		rai_info->compact_list = &rai_info->rai->alist->compact_forward_list;
		break;
	}
	case RAIType::SOURCE_EDGE: {
		rai_info->forward = false;
		rai_info->compact_list = &rai_info->rai->alist->compact_backward_list;
		break;
	}
		//	case RAIType::SELF:
		//	case RAIType::EDGE_SOURCE: {
		//		rai_info->alist =
		//		    rai_info->forward ? &rai_info->rai->alist->forward_list : &rai_info->rai->alist->backward_list;
		//		break;
		//	}
		//	case RAIType::EDGE_TARGET: {
		//		if (rai_info->rai->edge_direction == EdgeDirection::UNDIRECTED) {
		//			rai_info->alist = &rai_info->rai->alist->backward_list;
		//		}
		//		break;
		//	}
	default:
		break;
	}
}

void PhysicalAdaptiveMergeSIPJoin::InitializeZoneFilter() {
	auto &rai_info = conditions[0].rais[0];
	auto zone_size = (rai_info->left_cardinalities[0] / STANDARD_VECTOR_SIZE) + 1;
	rai_info->row_bitmask = make_unique<bitmask_vector>(zone_size * STANDARD_VECTOR_SIZE);
	rai_info->zone_bitmask = make_unique<bitmask_vector>(zone_size);
	if (rai_info->passing_tables[1] != 0) {
		auto extra_zone_size = (rai_info->left_cardinalities[1] / STANDARD_VECTOR_SIZE) + 1;
		rai_info->extra_row_bitmask = make_unique<bitmask_vector>(extra_zone_size * STANDARD_VECTOR_SIZE);
		rai_info->extra_zone_bitmask = make_unique<bitmask_vector>(extra_zone_size);
	}
}

void PhysicalAdaptiveMergeSIPJoin::ProbeHashTable(ClientContext &context, DataChunk &chunk,
                                                  PhysicalOperatorState *state_) {
	auto state = reinterpret_cast<PhysicalAdaptiveMergeSIPJoinState *>(state_);
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

void PhysicalAdaptiveMergeSIPJoin::PassZoneFilter() {
	// actually do the pushdown
	auto &rai_info = conditions[0].rais[0];
	children[0]->PushdownZoneFilter(rai_info->passing_tables[0], rai_info->row_bitmask, rai_info->zone_bitmask);
	if (rai_info->passing_tables[1] != 0) {
		children[0]->PushdownZoneFilter(rai_info->passing_tables[1], rai_info->extra_row_bitmask,
		                                rai_info->extra_zone_bitmask);
	}
}

void PhysicalAdaptiveMergeSIPJoin::AppendHTBlocks(PhysicalOperatorState *state_, DataChunk &chunk,
                                                  DataChunk &build_chunk) {
	auto state = reinterpret_cast<PhysicalAdaptiveMergeSIPJoinState *>(state_);
	state->join_keys.SetCardinality(chunk);
	state->join_keys.data[0].Reference(chunk.data[chunk.column_count() - 1]);
	//	state->rhs_executor.Execute(chunk, state->join_keys);
	if (right_projection_map.size() > 0) {
		//		build_chunk.Reset();
		build_chunk.SetCardinality(chunk);
		for (idx_t i = 0; i < right_projection_map.size(); i++) {
			build_chunk.data[i].Reference(chunk.data[right_projection_map[i]]);
		}
	} else {
		//		build_chunk.Reset();
		build_chunk.SetCardinality(chunk);
		for (idx_t i = 0; i < build_chunk.column_count(); i++) {
			build_chunk.data[i].Reference(chunk.data[i]);
		}
	}
	hash_table->Build(state->join_keys, build_chunk);
}

void PhysicalAdaptiveMergeSIPJoin::PerformSHJoin(ClientContext &context, DataChunk &chunk,
                                                 PhysicalOperatorState *state_) {
	auto state = reinterpret_cast<PhysicalAdaptiveMergeSIPJoinState *>(state_);
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
 * M_NLAJ
 * */
idx_t PhysicalAdaptiveMergeSIPJoin::DoLookups(ClientContext &context, DataChunk &right_chunk,
                                              DataChunk &right_condition_chunk, SelectionVector &lvector,
                                              SelectionVector &rvector, PhysicalOperatorState *state_) {
	auto state = reinterpret_cast<PhysicalAdaptiveMergeSIPJoinState *>(state_);
	SelectionVector sel;
	sel.InitializeCopy((sel_t *)FlatVector::incremental_vector);
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

void PhysicalAdaptiveMergeSIPJoin::PerformNLAJoin(ClientContext &context, DataChunk &chunk,
                                                  PhysicalOperatorState *state_) {
	auto state = reinterpret_cast<PhysicalAdaptiveMergeSIPJoinState *>(state_);
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
		match_count = DoLookups(context, im_chunk, state->right_condition_chunk, lvector, rvector, state);
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

void PhysicalAdaptiveMergeSIPJoin::GetChunkInternal(ClientContext &context, DataChunk &chunk,
                                                    PhysicalOperatorState *state_, SelectionVector *sel,
                                                    Vector *rid_vector, DataChunk *rai_chunk) {
	auto state = reinterpret_cast<PhysicalAdaptiveMergeSIPJoinState *>(state_);
	if (!state->initialized) {
		state->cached_chunk.Initialize(types);
		/* join right with alist */
		// initialize data chunks
		idx_t build_side_size = 0;
		auto right_state = children[1]->GetOperatorState();
		DataChunk right_chunk, build_chunk, im_chunk; // im_chunk: intermediate_chunk, [right_chunk.columns, tid]
		right_chunk.Initialize(children[1]->GetTypes());
		auto im_types = right_chunk.GetTypes();
		im_types.push_back(TypeId::INT64);
		im_chunk.Initialize(im_types);
		build_chunk.InitializeEmpty(hash_table->build_types);
		state->join_keys.InitializeEmpty(hash_table->condition_types);
		state->right_condition_chunk.InitializeEmpty(hash_table->condition_types);
		// initialize alist pointer
		InitializeAList();
		auto &rai_info = conditions[0].rais[0];
		while (true) {
			children[1]->GetChunk(context, right_chunk, right_state.get());
			state->rhs_executor.Execute(right_chunk, state->right_condition_chunk);
			state->right_tuple = 0;
			state->left_tuple = 0;
			if (right_chunk.size() == 0) {
				break;
			}
			build_side_size += right_chunk.size();
			do {
				rai_info->rai->GetVertexes(right_chunk, state->right_condition_chunk, im_chunk, state->left_tuple,
				                           state->right_tuple, rai_info->forward);
				if (state->estimate_join_size <= SIPJoin::NLAJ_MAGIC) {
					state->im_chunks.Append(im_chunk);
				} else {
					AppendHTBlocks(state_, im_chunk, build_chunk);
				}
				state->estimate_join_size += im_chunk.size();
			} while (state->right_tuple < right_chunk.size());
		}
		state->right_tuple = 0;
		state->left_tuple = 0;
		if (enableNLAJ && state->estimate_join_size <= SIPJoin::NLAJ_MAGIC) {
			// choose NLAJ
			chooseSHJ = false;
		} else {
			// choose SHJ
			// first finish building HT blocks
			for (auto &c_chunk : state->im_chunks.chunks) {
				AppendHTBlocks(state, *c_chunk, build_chunk);
			}
			// estimate semi-join filter passing ratio
			double avg_degree = rai_info->GetAverageDegree(rai_info->rai_type, rai_info->forward);
			double probe_table_card = (double)rai_info->left_cardinalities[0];
			double filter_passing_ratio = (double)build_side_size * avg_degree / probe_table_card;
			if (filter_passing_ratio <= SIPJoin::SHJ_MAGIC) {
				// if passing ratio is low, generate and pass semi-join filter
				InitializeZoneFilter();
				hash_table->FinalizeWithFilter(*rai_info);
				PassZoneFilter();
			} else {
				// else fall back to regular hash join
				hash_table->Finalize();
			}
		}
		state->initialized = true;

		if (hash_table->size() == 0 && state->im_chunks.count == 0 &&
		    (hash_table->join_type == JoinType::INNER || hash_table->join_type == JoinType::SEMI)) {
			// empty hash table with INNER or SEMI join means empty result set
			return;
		}
	}

	if (chooseSHJ) {
		// perform SHJ
		PerformSHJoin(context, chunk, state);
	} else {
		// perform NLAJ
		PerformNLAJoin(context, chunk, state);
	}
}

unique_ptr<PhysicalOperatorState> PhysicalAdaptiveMergeSIPJoin::GetOperatorState() {
	return make_unique<PhysicalAdaptiveMergeSIPJoinState>(children[0].get(), children[1].get(), conditions);
}

string PhysicalAdaptiveMergeSIPJoin::ExtraRenderInformation() const {
	string extra_info = "ADAPTIVE_MERGE_SIP_JOIN";
	if (chooseSHJ) {
		extra_info += "[SHJ]";
	} else {
		extra_info += "[NLAJ]";
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
