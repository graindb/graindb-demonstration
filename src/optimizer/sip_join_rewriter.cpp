#include "duckdb/optimizer/sip_join_rewriter.hpp"

#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/operator/logical_comparison_join.hpp"
#include "duckdb/storage/data_table.hpp"

namespace duckdb {
using namespace std;

unique_ptr<LogicalOperator> SIPJoinRewriter::Rewrite(unique_ptr<LogicalOperator> op) {
	if (op->type == LogicalOperatorType::CREATE_EDGE) {
		return op;
	}
	VisitOperator(*op);
	return op;
}

static bool IsDirectScan(LogicalOperator &op) {
	if (op.type == LogicalOperatorType::GET) {
		return true;
	}
	if (op.children.size() == 1) {
		return IsDirectScan(*op.children[0]);
	} else {
		return false;
	}
}

static inline void RewriteJoinCondition(column_t edge_column, BoundColumnRefExpression *edge, column_t vertex_column,
                                        BoundColumnRefExpression *vertex, LogicalComparisonJoin &join) {
	// rewrite the rai join condition
	ColumnBinding edge_binding(edge->binding.table_index, edge_column, edge_column, edge->binding.table);
	edge->binding = join.PushdownColumnBinding(edge_binding);
	edge->alias = edge->binding.table->columns[edge_column].name + "_rowid";
	edge->return_type = TypeId::INT64;
	ColumnBinding vertex_binding(vertex->binding.table_index, vertex_column, vertex_column, vertex->binding.table);
	vertex->binding = join.PushdownColumnBinding(vertex_binding);
	vertex->alias = vertex_column == COLUMN_IDENTIFIER_ROW_ID
	                    ? vertex_binding.table->name + "_rowid"
	                    : vertex_binding.table->columns[vertex_column].name + ".rowid";
	vertex->return_type = TypeId::INT64;
	// add join op mark
	join.op_hint = OpHint::SJ;
	join.enable_lookup_join = IsDirectScan(*join.children[0]);
}

bool SIPJoinRewriter::RewriteInternal(LogicalComparisonJoin &join, idx_t join_cond_idx) {
	assert(join_cond_idx < join.conditions.size());
	auto &join_cond = join.conditions[join_cond_idx];
	auto edge_column_expr = join_cond.GetEdgeColumn();
	auto vertex_column_expr = join_cond.GetVertexColumn();
	RewriteJoinCondition(edge_column_expr->binding.column_ordinal, edge_column_expr, COLUMN_IDENTIFIER_ROW_ID,
	                     vertex_column_expr, join);
	swap(join.conditions[0], join_cond);
	return true;
}

void SIPJoinRewriter::VisitOperator(LogicalOperator &op) {
	VisitOperatorChildren(op);
	if (op.type == LogicalOperatorType::COMPARISON_JOIN && (op.op_hint != OpHint::HJ && op.op_hint != OpHint::NLJ)) {
		auto &join = reinterpret_cast<LogicalComparisonJoin &>(op);
		for (auto i = 0u; i < join.conditions.size(); i++) {
			if (join.conditions[i].rai_info != nullptr) {
				if (RewriteInternal(join, i)) {
					return;
				}
			}
		}
	}
}

} // namespace duckdb
