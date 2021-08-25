#pragma once

#include "duckdb/common/enums/edge_direction.hpp"
#include "duckdb/common/types/vector.hpp"

namespace duckdb {

//! ListHandle is a cursor for accessing a single list in ALists.
struct ListHandle {
	idx_t *edge_list;
	idx_t *vertex_list;
	idx_t numElements;
};

struct AListsAppendState {
	idx_t current_num_vertices = 0;
};

//! ALists stores all lists per label per direction, and it provides reading interface to sequentially read all
//! edge/vertices of a source vertex.
class ALists {
public:
	ALists(string alias, EdgeDirection alists_direction, bool keep_edges)
	    : alias(move(alias)), alists_direction(alists_direction), keep_edges(keep_edges) {
	}

	virtual ~ALists() = default;

public:
	virtual ListHandle GetAList(idx_t source_vertex_id) = 0;
	//! This function initializes the alists by providing basic stats before appending and inserting.
	virtual void InitializeALists(idx_t num_source_vertices, idx_t num_edges, idx_t num_vertices) = 0;
	virtual void Insert(idx_t source_vertex_id, idx_t edge_id, idx_t vertex_id) = 0;
	//! This function should be called after InitializeALists(), and it should append source_vertex_id in ascending
	//! order.
	virtual bool AppendAList(idx_t source_vertex_id, idx_t *edge_ids, idx_t *vertex_ids, idx_t size,
	                         AListsAppendState &state) = 0;

	virtual idx_t FetchVertexes(idx_t &lpos, idx_t &rpos, Vector &rvector, idx_t rsize, SelectionVector &rsel,
	                            Vector &r0vector) = 0;

protected:
	string alias;
	EdgeDirection alists_direction;
	bool keep_edges;
};

class CSRLists : public ALists {
public:
	CSRLists(string alias, EdgeDirection alists_direction, bool keep_edges)
	    : ALists(move(alias), alists_direction, keep_edges), num_source_vertices(0), num_edges(0), num_vertices(0) {
	}

	~CSRLists() = default;

	ListHandle GetAList(idx_t vertex_id) override;
	void InitializeALists(idx_t num_source_vertices, idx_t num_edges, idx_t num_vertices) override;
	void Insert(idx_t source_vertex_id, idx_t edge_id, idx_t vertex_id) override;
	bool AppendAList(idx_t source_vertex_id, idx_t *input_edge_ids, idx_t *input_vertex_ids, idx_t size,
	                 AListsAppendState &state) override;

	idx_t FetchVertexes(idx_t &lpos, idx_t &rpos, Vector &rvector, idx_t rsize, SelectionVector &rsel,
	                    Vector &r0vector) override;

	inline idx_t *GetOffsetsArray() {
		return (idx_t *)offsets.get();
	}
	inline idx_t *GetSizesArray() {
		return (idx_t *)sizes.get();
	}

private:
	unique_ptr<idx_t[]> offsets;
	unique_ptr<idx_t[]> sizes;
	unique_ptr<idx_t[]> edge_ids;
	unique_ptr<idx_t[]> vertex_ids;
	idx_t num_source_vertices; // max_source_vertex_id + 1
	idx_t num_edges;
	idx_t num_vertices;
};
} // namespace duckdb
