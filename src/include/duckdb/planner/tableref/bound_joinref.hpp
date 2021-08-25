//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/planner/tableref/bound_joinref.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/enums/join_type.hpp"
#include "duckdb/planner/bound_tableref.hpp"
#include "duckdb/planner/expression.hpp"

namespace duckdb {

//! Represents a join, including cross product (condition is nullptr)
class BoundJoinRef : public BoundTableRef {
public:
	BoundJoinRef() : BoundTableRef(TableReferenceType::JOIN), type(JoinType::INVALID) {
	}
	explicit BoundJoinRef(TableReferenceType reference_type) : BoundTableRef(reference_type), type(JoinType::INVALID) {
	}

	//! The left hand side of the join
	unique_ptr<BoundTableRef> left;
	//! The right hand side of the join
	unique_ptr<BoundTableRef> right;

	//! Used for non-cross-product joins
	//! The join condition
	unique_ptr<Expression> condition;
	//! The join type
	JoinType type;
};
} // namespace duckdb
