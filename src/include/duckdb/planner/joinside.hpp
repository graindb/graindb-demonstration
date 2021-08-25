//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/planner/joinside.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/unordered_set.hpp"
#include "duckdb/execution/index/rai/rel_adj_index.hpp"
#include "duckdb/planner/expression.hpp"

namespace duckdb {

//! JoinCondition represents a left-right comparison join condition
struct JoinCondition {
public:
	JoinCondition()
	    : left(nullptr), right(nullptr), comparison(ExpressionType::COMPARE_EQUAL), null_values_are_equal(false),
	      rai_info(nullptr) {
	}

	//! Turns the JoinCondition into an expression; note that this destroys the JoinCondition as the expression inherits
	//! the left/right expressions
	static unique_ptr<Expression> CreateExpression(JoinCondition cond);

public:
	unique_ptr<Expression> left;
	unique_ptr<Expression> right;
	ExpressionType comparison;
	//! NULL values are equal for just THIS JoinCondition (instead of the entire join).
	//! This is only supported by the HashJoin and can only be used in equality comparisons.
	bool null_values_are_equal;
	//! RAI info bound to this join condition
	unique_ptr<RelAdjIndexInfo> rai_info;

	BoundColumnRefExpression *GetEdgeColumn() const {
		assert(rai_info);
		switch (rai_info->rai_lr_info) {
		case RAILRInfo::EDGE_SOURCE:
		case RAILRInfo::EDGE_DEST: {
			return reinterpret_cast<BoundColumnRefExpression *>(left.get());
		}
		case RAILRInfo::SOURCE_EDGE:
		case RAILRInfo::DEST_EDGE: {
			return reinterpret_cast<BoundColumnRefExpression *>(right.get());
		}
		case RAILRInfo::SELF: {
			return reinterpret_cast<BoundColumnRefExpression *>(rai_info->forward ? right.get() : left.get());
		}
		default:
			assert(false);
			return nullptr;
		}
	}

	BoundColumnRefExpression *GetVertexColumn() const {
		auto edge_column = GetEdgeColumn();
		assert(edge_column);
		if (edge_column == left.get()) {
			return reinterpret_cast<BoundColumnRefExpression *>(right.get());
		} else {
			return reinterpret_cast<BoundColumnRefExpression *>(left.get());
		}
	}

	string ToString() const {
		string result = ExpressionTypeToString(comparison) + "(" + left->GetName() + ", " + right->GetName() + ")";
		if (rai_info) {
			result += "[";
			result += rai_info->ToString();
			result += "\n";
			result += "]";
		}
		return result;
	}
};

// enum class JoinSide : uint8_t {
// 	NONE, LEFT, RIGHT, BOTH
// };

class JoinSide {
public:
	enum join_value : uint8_t { NONE, LEFT, RIGHT, BOTH };

	JoinSide() = default;
	constexpr JoinSide(join_value val) : value(val) {
	}

	bool operator==(JoinSide a) const {
		return value == a.value;
	}
	bool operator!=(JoinSide a) const {
		return value != a.value;
	}

	static JoinSide CombineJoinSide(JoinSide left, JoinSide right);
	static JoinSide GetJoinSide(idx_t table_binding, unordered_set<idx_t> &left_bindings,
	                            unordered_set<uint64_t> &right_bindings);
	static JoinSide GetJoinSide(Expression &expression, unordered_set<idx_t> &left_bindings,
	                            unordered_set<idx_t> &right_bindings);
	static JoinSide GetJoinSide(unordered_set<idx_t> bindings, unordered_set<idx_t> &left_bindings,
	                            unordered_set<idx_t> &right_bindings);

private:
	join_value value;
};

} // namespace duckdb
