#pragma once

#include "duckdb/common/types/chunk_collection.hpp"
#include "duckdb/execution/index/rai/alists.hpp"
#include "duckdb/storage/index.hpp"

namespace duckdb {

class RelAdjIndex : public Index {
public:
	RelAdjIndex(const string &alias, const vector<column_t> &column_ids,
	            vector<unique_ptr<Expression>> unbound_expressions, EdgeDirection edge_direction, bool keep_edges)
	    : Index(IndexType::RAI, column_ids, move(unbound_expressions)), alias(alias), edge_direction(edge_direction),
	      max_src_vertex_id(0), max_dst_vertex_id(0), num_src_vertices(0), num_dst_vertices(0) {
		// Only allow index building on two join index columns
		assert(column_ids.size() == 2);
		alists.push_back(make_unique_base<ALists, CSRLists>(alias + "_fwd_alists", EdgeDirection::FORWARD, keep_edges));
		if (edge_direction == EdgeDirection::UNDIRECTED) {
			alists.push_back(
			    make_unique_base<ALists, CSRLists>(alias + "_bwd_alists", EdgeDirection::BACKWARD, keep_edges));
		}
	}

	~RelAdjIndex() override {
	}

public:
	unique_ptr<IndexScanState> InitializeScanSinglePredicate(Transaction &transaction, vector<column_t> column_ids,
	                                                         Value value, ExpressionType expressionType) override {
		assert(false);
		return nullptr;
	}
	unique_ptr<IndexScanState> InitializeScanTwoPredicates(Transaction &transaction, vector<column_t> column_ids,
	                                                       Value low_value, ExpressionType low_expression_type,
	                                                       Value high_value,
	                                                       ExpressionType high_expression_type) override {
		assert(false);
		return nullptr;
	}
	void Scan(Transaction &transaction, DataTable &table, TableIndexScanState &state, DataChunk &result) override {
		assert(false);
	}
	bool Append(IndexLock &state, DataChunk &entries, Vector &row_identifiers) override {
		assert(false);
		return false;
	}
	void Delete(IndexLock &state, DataChunk &entries, Vector &row_identifiers) override {
		assert(false);
	}
	bool Insert(IndexLock &lock, DataChunk &input, Vector &row_identifiers) override;
	bool FinalizeInsert() override;

	void GetVertexes(DataChunk &right_chunk, DataChunk &rid_chunk, DataChunk &new_chunk, idx_t &left_tuple,
	                 idx_t &right_tuple, bool forward) const;

	inline ALists *GetALists(EdgeDirection direction) {
		assert(direction != EdgeDirection::INVALID && !alists.empty());
		assert(direction == EdgeDirection::FORWARD || (direction == EdgeDirection::BACKWARD && alists.size() == 2));
		return direction == EdgeDirection::FORWARD ? alists[0].get() : alists[1].get();
	}

public:
	string alias;
	EdgeDirection edge_direction;

private:
	//! Cache used for appending tuples into the index. It will be cleared after FinalizeInsert.
	ChunkCollection alists_cache;
	idx_t max_src_vertex_id, max_dst_vertex_id, num_src_vertices, num_dst_vertices;
	vector<unique_ptr<ALists>> alists;
};

//! Left and Right type
enum class RAILRInfo : uint8_t {
	INVALID = 0,
	DEST_EDGE = 2,
	SOURCE_EDGE = 3,
	EDGE_DEST = 5,
	EDGE_SOURCE = 6,
	SELF = 7
};

struct RelAdjIndexInfo {
	explicit RelAdjIndexInfo()
	    : rai(nullptr), rai_lr_info(RAILRInfo::INVALID), forward(true), vertex(nullptr), passing_table(0),
	      cardinality(0), extended_vertex_passing(false) {
	}

	RelAdjIndexInfo(RelAdjIndex *rai, RAILRInfo rai_type, bool forward, TableCatalogEntry *vertex)
	    : rai(rai), rai_lr_info(rai_type), forward(forward), vertex(vertex), passing_table(0), cardinality(0),
	      extended_vertex_passing(false) {
	}

	RelAdjIndex *rai;
	RAILRInfo rai_lr_info;
	bool forward;
	TableCatalogEntry *vertex; // vertex table table_pointer
	//! Index of table to pass
	idx_t passing_table;
	//! Cardinality of table to pass
	idx_t cardinality;
	//! Passing is from vertex to vertex or not
	bool extended_vertex_passing;

	//! Runtime data structures used during join
	shared_ptr<bitmask_vector> row_bitmask;
	shared_ptr<bitmask_vector> zone_bitmask;
	ALists *alists = nullptr; // csr alists storage used during join

	double GetAverageDegree(RAILRInfo rai_type_, bool forward_) const {
		switch (rai_type_) {
			//		case RAILRInfo::SELF: {
			//			return forward_ ? rai->alist->src_avg_degree : rai->alist->dst_avg_degree;
			//		}
			//		case RAILRInfo::EDGE_SOURCE: {
			//			return rai->alist->src_avg_degree;
			//		}
			//		case RAILRInfo::EDGE_DEST: {
			//			return rai->alist->dst_avg_degree;
			//		}
		default:
			return 1.0;
		}
	}

	string ToString() const {
		string result = "[" + rai->alias + ":";
		switch (rai_lr_info) {
		case RAILRInfo::SELF: {
			result += "SELF";
			break;
		}
		case RAILRInfo::EDGE_SOURCE: {
			result += "EDGE_SOURCE";
			break;
		}
		case RAILRInfo::EDGE_DEST: {
			result += "EDGE_DEST";
			break;
		}
		case RAILRInfo::SOURCE_EDGE: {
			result += "SOURCE_EDGE";
			break;
		}
		case RAILRInfo::DEST_EDGE: {
			result += "DEST_EDGE";
			break;
		}
		default: {
			result += "INVALID";
			break;
		}
		}
		result += "(" + std::to_string(passing_table);
		result += ")";
		result += forward ? " FORWARD" : " BACKWARD";
		result += "]";
		return result;
	}
};

} // namespace duckdb
