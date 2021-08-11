#include "duckdb/planner/operator/logical_join.hpp"

#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/expression_iterator.hpp"

using namespace duckdb;
using namespace std;

LogicalJoin::LogicalJoin(JoinType join_type, LogicalOperatorType logical_type)
    : LogicalOperator(logical_type), join_type(join_type) {
}

vector<ColumnBinding> LogicalJoin::GetColumnBindings() {
	auto left_bindings = MapBindings(children[0]->GetColumnBindings(), left_projection_map);
	if (join_type == JoinType::SEMI || join_type == JoinType::ANTI) {
		// for SEMI and ANTI join we only project the left hand side
		return left_bindings;
	}
	if (join_type == JoinType::MARK) {
		// for MARK join we project the left hand side plus the MARK column
		left_bindings.push_back(ColumnBinding(mark_index, 0));
		return left_bindings;
	}
	// for other join types we project both the LHS and the RHS
	auto right_bindings = MapBindings(children[1]->GetColumnBindings(), right_projection_map);
	left_bindings.insert(left_bindings.end(), right_bindings.begin(), right_bindings.end());
	return left_bindings;
}

ColumnBinding LogicalJoin::PushdownColumnBinding(ColumnBinding &binding) {
	unordered_set<idx_t> left_tables, right_tables;
	auto left_bindings = children[0]->GetColumnBindings();
	for (auto &lb : left_bindings) {
		left_tables.insert(lb.table_index);
	}
	if (left_tables.find(binding.table_index) != left_tables.end()) {
		auto child_binding = children[0]->PushdownColumnBinding(binding);
		if (child_binding.column_index != INVALID_INDEX) {
			if (left_projection_map.size() != 0) {
				left_projection_map.push_back(child_binding.column_index);
			}
			return child_binding;
		}
	}
	if (join_type == JoinType::SEMI || join_type == JoinType::ANTI || join_type == JoinType::MARK) {
		return ColumnBinding(binding.table_index, INVALID_INDEX);
	}
	auto right_bindings = children[1]->GetColumnBindings();
	for (auto &rb : right_bindings) {
		right_tables.insert(rb.table_index);
	}
	if (right_tables.find(binding.table_index) != right_tables.end()) {
		auto child_binding = children[1]->PushdownColumnBinding(binding);
		if (child_binding.column_index != INVALID_INDEX) {
			if (right_projection_map.size() != 0) {
				right_projection_map.push_back(child_binding.column_index);
			}
			return child_binding;
		}
	}

	return ColumnBinding(binding.table_index, INVALID_INDEX);
}

void LogicalJoin::ResolveTypes() {
	types = MapTypes(children[0]->types, left_projection_map);
	if (join_type == JoinType::SEMI || join_type == JoinType::ANTI) {
		// for SEMI and ANTI join we only project the left hand side
		return;
	}
	if (join_type == JoinType::MARK) {
		// for MARK join we project the left hand side, plus a BOOLEAN column indicating the MARK
		types.push_back(TypeId::BOOL);
		return;
	}
	// for any other join we project both sides
	auto right_types = MapTypes(children[1]->types, right_projection_map);
	types.insert(types.end(), right_types.begin(), right_types.end());
}

void LogicalJoin::GetTableReferences(LogicalOperator &op, unordered_set<idx_t> &bindings) {
	auto column_bindings = op.GetColumnBindings();
	for (auto binding : column_bindings) {
		bindings.insert(binding.table_index);
	}
}

void LogicalJoin::GetExpressionBindings(Expression &expr, unordered_set<idx_t> &bindings) {
	if (expr.type == ExpressionType::BOUND_COLUMN_REF) {
		auto &colref = (BoundColumnRefExpression &)expr;
		assert(colref.depth == 0);
		bindings.insert(colref.binding.table_index);
	}
	ExpressionIterator::EnumerateChildren(expr, [&](Expression &child) { GetExpressionBindings(child, bindings); });
}
