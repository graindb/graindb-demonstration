#include "duckdb/execution/index/rai/alists.hpp"

#include "duckdb/common/types/vector.hpp"

#include <cassert>
#include <cstring>

using namespace duckdb;
using namespace std;

ListHandle CSRLists::GetAList(idx_t vertex_id) {
	if (vertex_id >= num_source_vertices) {
		return ListHandle{nullptr, nullptr, 0};
	}
	assert(vertex_id < num_source_vertices);
	auto startOffset = offsets[vertex_id];
	auto size = sizes[vertex_id];
	assert(vertex_ids);
	if (edge_ids) {
		return ListHandle{edge_ids.get() + startOffset, vertex_ids.get() + startOffset, size};
	} else {
		return ListHandle{nullptr, vertex_ids.get() + startOffset, size};
	}
}

void CSRLists::InitializeALists(idx_t _num_source_vertices, idx_t _num_edges, idx_t _num_vertices) {
	assert(num_edges == num_vertices);
	offsets = unique_ptr<idx_t[]>(new idx_t[_num_source_vertices]);
	memset(offsets.get(), 0, _num_source_vertices * sizeof(idx_t));
	sizes = unique_ptr<idx_t[]>(new idx_t[_num_source_vertices]);
	memset(sizes.get(), 0, _num_source_vertices * sizeof(idx_t));
	if (keep_edges) {
		edge_ids = unique_ptr<idx_t[]>(new idx_t[_num_edges]);
		memset(edge_ids.get(), 0, _num_edges * sizeof(idx_t));
	}
	vertex_ids = unique_ptr<idx_t[]>(new idx_t[_num_vertices]);
	memset(vertex_ids.get(), 0, _num_vertices * sizeof(idx_t));
	this->num_source_vertices = _num_source_vertices;
	this->num_edges = _num_edges;
	this->num_vertices = _num_vertices;
}

void CSRLists::Insert(idx_t source_vertex_id, idx_t edge_id, idx_t vertex_id) {
	assert(source_vertex_id < num_source_vertices);
	auto start_offset = offsets[source_vertex_id];
	auto offset_in_list = sizes[source_vertex_id];
	if (keep_edges) {
		edge_ids.operator[](start_offset + offset_in_list) = edge_id;
	}
	vertex_ids.operator[](start_offset + offset_in_list) = vertex_id;
	sizes[source_vertex_id] = offset_in_list + 1;
}

bool CSRLists::AppendAList(idx_t source_vertex_id, idx_t *input_edge_ids, idx_t *input_vertex_ids, idx_t size,
                           AListsAppendState &state) {
	assert(state.current_num_vertices + size <= num_vertices);
	assert(input_edge_ids && input_vertex_ids);
	offsets.operator[](source_vertex_id) = state.current_num_vertices;
	sizes.operator[](source_vertex_id) = size;
	if (keep_edges) {
		memcpy(edge_ids.get() + (state.current_num_vertices * sizeof(idx_t)), input_edge_ids, size * sizeof(idx_t));
	}
	memcpy(vertex_ids.get() + (state.current_num_vertices * sizeof(idx_t)), input_vertex_ids, size * sizeof(idx_t));
	state.current_num_vertices += size;
	return true;
}

idx_t CSRLists::FetchVertexes(idx_t &lpos, idx_t &rpos, Vector &rvector, idx_t rsize, SelectionVector &rsel,
                              Vector &r0vector) {
	assert(r0vector.vector_type == VectorType::FLAT_VECTOR);
	if (rpos >= rsize) {
		return 0;
	}

	VectorData right_data;
	rvector.Orrify(rsize, right_data);
	auto rdata = (int64_t *)right_data.data;
	auto r0data = (int64_t *)FlatVector::GetData(r0vector);
	idx_t result_count = 0;

	if (right_data.nullmask->any()) {
		while (rpos < rsize) {
			idx_t right_position = right_data.sel->get_index(rpos);
			if ((*right_data.nullmask)[right_position]) {
				continue;
			}
			auto rid = rdata[right_position];
			auto offset = offsets.operator[](rid);
			auto length = sizes.operator[](rid);
			auto result_size = std::min(length - lpos, STANDARD_VECTOR_SIZE - result_count);
			memcpy(r0data + result_count, vertex_ids.get() + (offset + lpos), result_size * sizeof(int64_t));
			for (idx_t i = 0; i < result_size; i++) {
				rsel.set_index(result_count++, rpos);
			}
			lpos += result_size;
			if (lpos == length) {
				lpos = 0;
				rpos++;
			}
			if (result_count == STANDARD_VECTOR_SIZE) {
				return result_count;
			}
		}
	} else {
		while (rpos < rsize) {
			idx_t right_position = right_data.sel->get_index(rpos);
			auto rid = rdata[right_position];
			auto offset = offsets.operator[](rid);
			auto length = sizes.operator[](rid);
			auto result_size = std::min(length - lpos, STANDARD_VECTOR_SIZE - result_count);
			memcpy(r0data + result_count, vertex_ids.get() + (offset + lpos), result_size * sizeof(int64_t));
			for (idx_t i = 0; i < result_size; i++) {
				rsel.set_index(result_count++, rpos);
			}
			lpos += result_size;
			if (lpos == length) {
				lpos = 0;
				rpos++;
			}
			if (result_count == STANDARD_VECTOR_SIZE) {
				return result_count;
			}
		}
	}
	return result_count;
}
