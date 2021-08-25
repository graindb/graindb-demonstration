//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/parser/tableref/joinref.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/enums/join_type.hpp"
#include "duckdb/common/unordered_set.hpp"
#include "duckdb/parser/parsed_expression.hpp"
#include "duckdb/parser/tableref.hpp"
#include "duckdb/parser/tableref/edgeref.hpp"

namespace duckdb {
//! Represents a JOIN between two expressions
class JoinRef : public TableRef {
public:
	JoinRef() : TableRef(TableReferenceType::JOIN), type(JoinType::INNER) {
	}
	explicit JoinRef(JoinType type) : TableRef(TableReferenceType::JOIN), type(type) {
	}
	JoinRef(unique_ptr<TableRef> left, unique_ptr<TableRef> right, unique_ptr<ParsedExpression> condition,
	        JoinType join_type)
	    : TableRef(TableReferenceType::JOIN), left(move(left)), right(move(right)), condition(move(condition)),
	      type(join_type) {
	}

	//! The left hand side of the join
	unique_ptr<TableRef> left;
	//! The right hand side of the join
	unique_ptr<TableRef> right;
	//! The join condition
	unique_ptr<ParsedExpression> condition;
	//! The join type
	JoinType type;
	//! The set of USING columns (if any)
	vector<string> using_columns;

public:
	bool Equals(const TableRef *other_) const override;

	unique_ptr<TableRef> Copy() override;

	//! Serializes a blob into a JoinRef
	void Serialize(Serializer &serializer) override;
	//! Deserializes a blob back into a JoinRef
	static unique_ptr<TableRef> Deserialize(Deserializer &source);
};
} // namespace duckdb
