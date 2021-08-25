#include "duckdb/execution/index/rai/rel_adj_index.hpp"

namespace duckdb {

bool RelAdjIndex::Insert(IndexLock &lock, DataChunk &input, Vector &row_identifiers) {
	// Iterate all tuples in the input. Get basic stats: num_source_vertices (both direction), num_edges, num_vertices.
	VectorData src_vertex_vector, dst_vertex_vector;
	input.data[0].Orrify(input.size(), src_vertex_vector);
	input.data[1].Orrify(input.size(), dst_vertex_vector);
	auto src_vertices = (idx_t *)src_vertex_vector.data;
	auto dst_vertices = (idx_t *)dst_vertex_vector.data;
	for (idx_t i = 0; i < input.size(); i++) {
		auto src_pos = src_vertex_vector.sel->get_index(i);
		if ((*src_vertex_vector.nullmask)[src_pos]) {
			continue;
		}
		num_src_vertices++;
		if (src_vertices[src_pos] > max_src_vertex_id) {
			max_src_vertex_id = src_vertices[src_pos];
		}
		auto dst_pos = dst_vertex_vector.sel->get_index(i);
		if ((*dst_vertex_vector.nullmask)[dst_pos]) {
			continue;
		}
		num_dst_vertices++;
		if (dst_vertices[dst_pos] > max_dst_vertex_id) {
			max_dst_vertex_id = dst_vertices[dst_pos];
		}
	}
	DataChunk newDataChunk;
	newDataChunk.data.resize(input.column_count() + 1);
	newDataChunk.Reference(input);
	newDataChunk.data[input.column_count()].Reference(row_identifiers);
	alists_cache.Append(newDataChunk);
	return true;
}

bool RelAdjIndex::FinalizeInsert() {
	assert(alists.size() <= 2);
	// Initialize insert for each alists
	alists[0]->InitializeALists(max_src_vertex_id + 1, num_dst_vertices, num_dst_vertices);
	if (alists.size() == 2) {
		alists[1]->InitializeALists(max_dst_vertex_id + 1, num_src_vertices, num_src_vertices);
	}
	auto fwd_alists_sizes = reinterpret_cast<CSRLists *>(alists[0].get())->GetSizesArray();
	idx_t *bwd_alists_sizes = nullptr;
	if (alists.size() == 2) {
		bwd_alists_sizes = reinterpret_cast<CSRLists *>(alists[1].get())->GetSizesArray();
	}
	// First pass: Iterate all tuples in alists_cache to calculate size for each source_vertex in `alists`.
	for (auto &chunk : alists_cache.chunks) {
		VectorData src_vertex_vector, dst_vertex_vector;
		chunk->data[0].Orrify(chunk->size(), src_vertex_vector);
		chunk->data[1].Orrify(chunk->size(), dst_vertex_vector);
		auto src_vertices = (idx_t *)src_vertex_vector.data;
		auto dst_vertices = (idx_t *)dst_vertex_vector.data;
		for (idx_t i = 0; i < chunk->size(); i++) {
			auto src_pos = src_vertex_vector.sel->get_index(i);
			auto dst_pos = dst_vertex_vector.sel->get_index(i);
			assert(src_pos == dst_pos);
			if ((*src_vertex_vector.nullmask)[src_pos] || (*dst_vertex_vector.nullmask)[dst_pos]) {
				continue;
			}
			fwd_alists_sizes[src_vertices[src_pos]] = fwd_alists_sizes[src_vertices[src_pos]] + 1;
			if (alists.size() == 2) {
				bwd_alists_sizes[dst_vertices[dst_pos]] = bwd_alists_sizes[dst_vertices[dst_pos]] + 1;
			}
		}
	}
	// Update `offsets` based on `sizes` array and reset `sizes` to 0.
	auto fwd_alists_offsets =
	    reinterpret_cast<CSRLists *>(alists[0].get())->GetOffsetsArray(); // size: max_src_vertex_id+1
	auto current_offset = 0;
	for (idx_t i = 0; i < (max_src_vertex_id + 1); i++) {
		fwd_alists_offsets[i] = current_offset;
		current_offset += fwd_alists_sizes[i];
	}
	memset(fwd_alists_sizes, 0, (max_src_vertex_id + 1) * sizeof(idx_t));
	if (alists.size() == 2) {
		auto bwd_alists_offsets = reinterpret_cast<CSRLists *>(alists[1].get())->GetOffsetsArray();
		current_offset = 0;
		for (idx_t i = 0; i < (max_dst_vertex_id + 1); i++) {
			bwd_alists_offsets[i] = current_offset;
			current_offset += bwd_alists_sizes[i];
		}
		memset(bwd_alists_sizes, 0, (max_dst_vertex_id + 1) * sizeof(idx_t));
	}
	// Second pass: Iterate all tuples in alists_cache to insert values and update `sizes` accordingly in alists.
	for (auto &chunk : alists_cache.chunks) {
		VectorData src_vertex_vector, dst_vertex_vector, edge_vector;
		chunk->data[0].Orrify(chunk->size(), src_vertex_vector);
		chunk->data[1].Orrify(chunk->size(), dst_vertex_vector);
		chunk->data[2].Orrify(chunk->size(), edge_vector);
		auto src_vertices = (idx_t *)src_vertex_vector.data;
		auto dst_vertices = (idx_t *)dst_vertex_vector.data;
		auto edges = (idx_t *)edge_vector.data;
		for (idx_t i = 0; i < chunk->size(); i++) {
			auto src_pos = src_vertex_vector.sel->get_index(i);
			auto dst_pos = dst_vertex_vector.sel->get_index(i);
			assert(src_pos == dst_pos);
			if ((*src_vertex_vector.nullmask)[src_pos] || (*dst_vertex_vector.nullmask)[dst_pos]) {
				continue;
			}
			alists[0]->Insert(src_vertices[src_pos], edges[src_pos], dst_vertices[src_pos]);
			if (alists.size() == 2) {
				alists[1]->Insert(dst_vertices[dst_pos], edges[dst_pos], src_vertices[dst_pos]);
			}
		}
	}
	// Clean the alists_cache
	alists_cache.Clear();
	return true;
}

void RelAdjIndex::GetVertexes(DataChunk &right_chunk, DataChunk &rid_chunk, DataChunk &new_chunk, idx_t &left_tuple,
                              idx_t &right_tuple, bool forward) const {
	assert(new_chunk.column_count() == right_chunk.column_count() + 1);
	SelectionVector rvector(STANDARD_VECTOR_SIZE);
	auto *alist = forward ? alists[0].get() : alists[1].get();
	auto matched_count = alist->FetchVertexes(left_tuple, right_tuple, rid_chunk.data[0], rid_chunk.size(), rvector,
	                                          new_chunk.data[right_chunk.column_count()]);
	// slice and construct new_chunk
	new_chunk.Slice(right_chunk, rvector, matched_count);
	new_chunk.SetCardinality(matched_count);
}

} // namespace duckdb
