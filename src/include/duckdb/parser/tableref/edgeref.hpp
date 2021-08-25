#pragma once

#include "duckdb/common/enums/edge_direction.hpp"
#include "duckdb/common/serializer.hpp"
#include "duckdb/parser/tableref.hpp"

namespace duckdb {

class EdgeRef : public TableRef {
public:
	EdgeRef() : TableRef(TableReferenceType::EDGE), direction(EdgeDirection::INVALID), length_low(1), length_high(1) {
	}
	~EdgeRef() override = default;

public:
	//! Edge name/label
	string edge_name;
	//! Table name
	string base_table_name;

	EdgeDirection direction;
	idx_t length_low;
	idx_t length_high;
	//! Temp variable names for transformation
	string src_vertex_alias;
	string dst_vertex_alias;

public:
	string ToString() const override {
		return "EDGE(" + alias + ":" + edge_name + ")";
	}

	unique_ptr<EdgeRef> Revert() {
		auto reversion = make_unique<EdgeRef>();
		reversion->alias = alias;
		reversion->edge_name = edge_name;
		reversion->base_table_name = base_table_name;
		reversion->direction = direction == EdgeDirection::FORWARD ? EdgeDirection::BACKWARD : EdgeDirection::FORWARD;
		reversion->length_low = length_high;
		reversion->length_high = length_low;
		reversion->src_vertex_alias = dst_vertex_alias;
		reversion->dst_vertex_alias = src_vertex_alias;
		return reversion;
	}

	bool Equals(const TableRef *other_) const override {
		if (!TableRef::Equals(other_)) {
			return false;
		}
		auto other = (EdgeRef *)other_;
		return other->edge_name == edge_name && other->base_table_name == base_table_name;
	}

	unique_ptr<TableRef> Copy() override {
		auto copy = make_unique<EdgeRef>();
		copy->alias = alias;
		copy->edge_name = edge_name;
		copy->base_table_name = base_table_name;
		copy->direction = direction;
		copy->length_low = length_low;
		copy->length_high = length_high;
		copy->src_vertex_alias = src_vertex_alias;
		copy->dst_vertex_alias = dst_vertex_alias;
		return copy;
	}

	//! Serializes a blob into a EdgeRef
	void Serialize(Serializer &serializer) override {
		TableRef::Serialize(serializer);

		serializer.WriteString(alias);
		serializer.WriteString(edge_name);
		serializer.WriteString(base_table_name);
	}
	//! Deserializes a blob back into a EdgeRef
	static unique_ptr<TableRef> Deserialize(Deserializer &source) {
		auto result = make_unique<EdgeRef>();
		result->alias = source.Read<string>();
		result->edge_name = source.Read<string>();
		result->base_table_name = source.Read<string>();
		return result;
	}
};

} // namespace duckdb
