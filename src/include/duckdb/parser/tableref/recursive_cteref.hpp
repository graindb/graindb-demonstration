
#pragma once

#include "duckdb/parser/query_node/recursive_cte_node.hpp"
#include "duckdb/parser/tableref.hpp"

namespace duckdb {

class RecursiveCTERef : public TableRef {

public:
	explicit RecursiveCTERef(unique_ptr<RecursiveCTENode> cte_node)
	    : TableRef(TableReferenceType::REC_CTE), cte_node(move(cte_node)) {
	}

	//! The subquery
	unique_ptr<RecursiveCTENode> cte_node;
	//! Aliases for the column names
	vector<string> column_name_alias;

public:
	bool Equals(const TableRef *other_) const override {
		return false;
	}

	unique_ptr<TableRef> Copy() override {
		auto copy = make_unique<RecursiveCTERef>(unique_ptr_cast<QueryNode, RecursiveCTENode>(cte_node->Copy()));
		copy->column_name_alias = column_name_alias;
		return move(copy);
	}

	//! Serializes a blob into a SubqueryRef
	void Serialize(Serializer &serializer) override {
	}
	//! Deserializes a blob back into a SubqueryRef
	static unique_ptr<TableRef> Deserialize(Deserializer &source) {
		return nullptr;
	}
};
} // namespace duckdb