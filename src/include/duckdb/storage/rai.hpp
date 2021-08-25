//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/storage/rai.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/common/enums/edge_direction.hpp"
#include "duckdb/execution/index/rai/alists.hpp"
#include "duckdb/execution/index/rai/rel_adj_index.hpp"

#include <mutex>

namespace duckdb {

//! Relational Adjacency Index
// class RAI {
// public:
//	RAI(string name, TableCatalogEntry *table, EdgeDirection rai_direction, vector<column_t> column_ids,
//	    vector<TableCatalogEntry *> referenced_tables, vector<column_t> referenced_columns)
//	    : name(move(name)), table(table), rai_direction(rai_direction), column_ids(move(column_ids)),
//	      referenced_tables(move(referenced_tables)), referenced_columns(move(referenced_columns)) {
//	}
//	virtual ~RAI() = default;
//
//	std::mutex lock;
//	string name;
//	TableCatalogEntry *table;
//	EdgeDirection rai_direction;
//	vector<column_t> column_ids;
//	vector<TableCatalogEntry *> referenced_tables;
//	vector<column_t> referenced_columns;
//
//	unique_ptr<ALists> alist;
//
//	//	void GetChunk(DataChunk &right_chunk, DataChunk &rid_chunk, DataChunk &rai_chunk, idx_t &left_tuple,
//	//	              idx_t &right_tuple, bool forward) const;
//	void GetVertexes(DataChunk &right_chunk, DataChunk &rid_chunk, DataChunk &rai_chunk, idx_t &left_tuple,
//	                 idx_t &right_tuple, bool forward) const;
//	// WARNING: Deprecated
//	//	idx_t GetZoneFilter(data_ptr_t *hashmap, idx_t hm_size, shared_ptr<bitmask_vector> &zone_filter,
//	//	                    shared_ptr<bitmask_vector> &extra_zone_filter, bool forward) const;
//	// WARNING: Deprecated
//	//	idx_t GetRowsFilter(data_ptr_t *hashmap, idx_t hm_size, shared_ptr<vector<row_t>> &rows_filter,
//	//	                    shared_ptr<vector<row_t>> &extra_rows_filter, bool forward) const;
//
// public:
//	string ToString() {
//		//! example:: rai: order_rai(o_custkey: Customer.c_custkey, o_orderkey: Lineitem.l_orderkey)
//		//! param:&table, column_ids, vertex_tables, referenced_columns
//		string description;
//		description.append(this->name + "(");
//		for (unsigned long i = 0; i < this->column_ids.size(); i++) {
//			string column_name = this->table->columns[this->column_ids[i]].name;
//			string ref_column_name = this->referenced_tables[i]->columns[referenced_columns[i]].name;
//			description.append(column_name + ": " + this->referenced_tables[i]->name + "." + ref_column_name + ",");
//		}
//		description = description.substr(0, description.length() - 1) + ");";
//		return description;
//	}
//};

struct RAIInfo {
	explicit RAIInfo() : rai(nullptr), rai_type(RAILRInfo::INVALID), forward(true), vertex(nullptr), vertex_id(0) {
	}

	RelAdjIndex *rai;
	RAILRInfo rai_type;
	bool forward;
	TableCatalogEntry *vertex; // vertex table table_pointer
	idx_t vertex_id;           // vertex table table_index
	vector<idx_t> passing_tables = {0, 0};
	vector<idx_t> left_cardinalities = {0, 0};
	shared_ptr<bitmask_vector> row_bitmask;
	shared_ptr<bitmask_vector> zone_bitmask;
	shared_ptr<bitmask_vector> extra_row_bitmask;
	shared_ptr<bitmask_vector> extra_zone_bitmask;
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

	string ToString() {
		string result = "[" + rai->alias + ":";
		switch (rai_type) {
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
		result += "(" + std::to_string(passing_tables[0]);
		if (passing_tables[1] != 0) {
			result += ", " + std::to_string(passing_tables[1]);
		}
		result += ")";
		result += forward ? " FORWARD" : " BACKWARD";
		result += "]";
		return result;
	}
};

} // namespace duckdb
