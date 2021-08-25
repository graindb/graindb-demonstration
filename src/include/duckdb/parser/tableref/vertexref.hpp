#pragma once

#include "duckdb/common/serializer.hpp"
#include "duckdb/parser/tableref.hpp"

namespace duckdb {

class VertexRef : public TableRef {

public:
	VertexRef() : TableRef(TableReferenceType::VERTEX) {
	}
	~VertexRef() override = default;

	//! Vertex label
	string vertex_label;
	//! Table name
	string base_table_name;

public:
	string ToString() const override {
		return "VERTEX(" + alias + ":" + vertex_label + ")";
	}

	bool Equals(const TableRef *other_) const override {
		if (!TableRef::Equals(other_)) {
			return false;
		}
		auto other = (VertexRef *)other_;
		return other->vertex_label == vertex_label && other->base_table_name == base_table_name;
	}

	unique_ptr<TableRef> Copy() override {
		auto copy = make_unique<VertexRef>();
		copy->alias = alias;
		copy->vertex_label = vertex_label;
		copy->base_table_name = base_table_name;
		return copy;
	}

	//! Serializes a blob into a VertexRef
	void Serialize(Serializer &serializer) override {
		TableRef::Serialize(serializer);

		serializer.WriteString(alias);
		serializer.WriteString(vertex_label);
		serializer.WriteString(base_table_name);
	}
	//! Deserializes a blob back into a VertexRef
	static unique_ptr<TableRef> Deserialize(Deserializer &source) {
		auto result = make_unique<VertexRef>();
		result->alias = source.Read<string>();
		result->vertex_label = source.Read<string>();
		result->base_table_name = source.Read<string>();
		return result;
	}
};
} // namespace duckdb