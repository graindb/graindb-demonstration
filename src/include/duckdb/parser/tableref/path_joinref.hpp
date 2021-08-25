#pragma once

#include "duckdb/common/enums/join_type.hpp"
#include "duckdb/common/unordered_set.hpp"
#include "duckdb/parser/parsed_expression.hpp"
#include "duckdb/parser/tableref.hpp"
#include "duckdb/parser/tableref/edgeref.hpp"
#include "duckdb/parser/tableref/vertexref.hpp"

namespace duckdb {

class PathJoinRef : public TableRef {
public:
	PathJoinRef() : TableRef(TableReferenceType::PATH_PATTERN), length_low(0), length_high(0) {
	}

	unique_ptr<TableRef> source_vertex;
	unique_ptr<EdgeRef> edge;
	unique_ptr<TableRef> destination_vertex;
	idx_t length_low;  // default is 0, which means no recursive
	idx_t length_high; // default is 0, which means no recursive

public:
	bool Equals(const TableRef *other_) const override {
		return false;
	}

	unique_ptr<TableRef> Copy() override {
		return nullptr;
	}

	//! Serializes a blob into a JoinRef
	void Serialize(Serializer &serializer) override {
	}
	//! Deserializes a blob back into a JoinRef
	static unique_ptr<TableRef> Deserialize(Deserializer &source) {
		return nullptr;
	}
};
} // namespace duckdb
