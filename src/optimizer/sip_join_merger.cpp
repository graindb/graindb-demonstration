#include "duckdb/optimizer/sip_join_merger.hpp"

#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/operator/logical_comparison_join.hpp"

namespace duckdb {
using namespace std;

unique_ptr<LogicalOperator> SIPJoinMerger::Rewrite(unique_ptr<LogicalOperator> op) {
	VisitOperator(*op);
	return op;
}

void SIPJoinMerger::VisitOperator(LogicalOperator &op) {
	// traverse recursively through the operator tree bottom-up
	VisitOperatorChildren(op);
	// try merge
	if (op.type == LogicalOperatorType::COMPARISON_JOIN && op.op_hint == OpHint::SJ) {
		auto &join = reinterpret_cast<LogicalComparisonJoin &>(op);
		if (join.conditions.size() == 1) {
			Merge(join);
		}
	}
}

void SIPJoinMerger::Merge(LogicalComparisonJoin &join) {
	auto &rai_info = join.conditions[0].rai_info;
	switch (rai_info->rai_lr_info) {
	case RAILRInfo::EDGE_SOURCE: {
		if (join.children[0]->op_hint == OpHint::SJ) {
			auto left = reinterpret_cast<LogicalComparisonJoin *>(join.children[0].get());
			auto &left_rai_info = left->conditions[0].rai_info;
			if (left_rai_info->rai == rai_info->rai && left->conditions.size() == 1 &&
			    left_rai_info->rai_lr_info == RAILRInfo::DEST_EDGE &&
			    left->children[1]->type == LogicalOperatorType::GET) {
				auto edge_get = reinterpret_cast<LogicalGet *>(left->children[1].get());
				bool is_mergable = edge_get->column_ids.size() <= 2;
				for (auto col : edge_get->column_ids) {
					if (edge_get->table->join_index_columns_map.find(col) ==
					    edge_get->table->join_index_columns_map.end()) {
						is_mergable = false;
						break;
					}
				}
				if (is_mergable) {
					// perform merge: DESTINATION-[EDGE]-SOURCE. left.condition.left=left.condition.right
					auto left_binding = reinterpret_cast<BoundColumnRefExpression *>(left->conditions[0].left.get());
					rai_info->cardinality = left_rai_info->cardinality;
					rai_info->passing_table = left_binding->binding.table_index;
					column_ref_equivalence_cache
					    [reinterpret_cast<BoundColumnRefExpression *>(left->conditions[0].right.get())->binding] =
					        left_binding->binding;
					column_ref_equivalence_cache
					    [reinterpret_cast<BoundColumnRefExpression *>(join.conditions[0].left.get())->binding] =
					        reinterpret_cast<BoundColumnRefExpression *>(join.conditions[0].right.get())->binding;
					join.conditions[0].left = move(left->conditions[0].left);
					join.enable_lookup_join = left->enable_lookup_join;
					join.AddChild(move(left->children[1]));
					join.children[0] = move(left->children[0]);
					join.op_hint = OpHint::MSJ;
				}
			}
		}
		break;
	}
	case RAILRInfo::EDGE_DEST: {
		if (join.children[0]->op_hint == OpHint::SJ) {
			auto left = reinterpret_cast<LogicalComparisonJoin *>(join.children[0].get());
			auto &left_rai_info = left->conditions[0].rai_info;
			if (left_rai_info->rai == rai_info->rai && left->conditions.size() == 1 &&
			    left_rai_info->rai_lr_info == RAILRInfo::SOURCE_EDGE &&
			    left_rai_info->rai->edge_direction == EdgeDirection::UNDIRECTED &&
			    left->children[1]->type == LogicalOperatorType::GET) {
				auto edge_get = reinterpret_cast<LogicalGet *>(left->children[1].get());
				bool is_mergable = edge_get->column_ids.size() <= 2;
				for (auto col : edge_get->column_ids) {
					if (edge_get->table->join_index_columns_map.find(col) ==
					    edge_get->table->join_index_columns_map.end()) {
						is_mergable = false;
						break;
					}
				}
				if (is_mergable) {
					// perform merge: SOURCE-[EDGE]-DESTINATION
					auto left_binding = reinterpret_cast<BoundColumnRefExpression *>(left->conditions[0].left.get());
					rai_info->cardinality = left_rai_info->cardinality;
					rai_info->passing_table = left_binding->binding.table_index;
					column_ref_equivalence_cache
					    [reinterpret_cast<BoundColumnRefExpression *>(left->conditions[0].right.get())->binding] =
					        left_binding->binding;
					column_ref_equivalence_cache
					    [reinterpret_cast<BoundColumnRefExpression *>(join.conditions[0].left.get())->binding] =
					        reinterpret_cast<BoundColumnRefExpression *>(join.conditions[0].right.get())->binding;
					join.conditions[0].left = move(left->conditions[0].left);
					join.enable_lookup_join = left->enable_lookup_join;
					auto &new_left = left->children[0];
					auto &merge_child = left->children[1];
					join.AddChild(move(merge_child));
					join.children[0] = move(new_left);
					join.op_hint = OpHint::MSJ;
				}
			}
		}
		break;
	}
	case RAILRInfo::DEST_EDGE: {
		if (join.children[1]->op_hint == OpHint::SJ) {
			auto right = reinterpret_cast<LogicalComparisonJoin *>(join.children[1].get());
			auto &right_rai_info = right->conditions[0].rai_info;
			if (right_rai_info->rai == rai_info->rai && right->conditions.size() == 1 &&
			    right_rai_info->rai_lr_info == RAILRInfo::EDGE_SOURCE &&
			    right->children[0]->type == LogicalOperatorType::GET) {
				auto edge_get = reinterpret_cast<LogicalGet *>(right->children[0].get());
				bool is_mergable = edge_get->column_ids.size() <= 2;
				for (auto col : edge_get->column_ids) {
					if (edge_get->table->join_index_columns_map.find(col) ==
					    edge_get->table->join_index_columns_map.end()) {
						is_mergable = false;
						break;
					}
				}
				if (is_mergable) {
					// perform merge: DESTINATION-[EDGE]-SOURCE
					auto &new_right = right->children[1];
					auto &merge_child = right->children[0];
					column_ref_equivalence_cache
					    [reinterpret_cast<BoundColumnRefExpression *>(join.conditions[0].right.get())->binding] =
					        reinterpret_cast<BoundColumnRefExpression *>(join.conditions[0].left.get())->binding;
					column_ref_equivalence_cache
					    [reinterpret_cast<BoundColumnRefExpression *>(right->conditions[0].left.get())->binding] =
					        reinterpret_cast<BoundColumnRefExpression *>(right->conditions[0].right.get())->binding;
					join.conditions[0].right = move(right->conditions[0].right);
					join.AddChild(move(merge_child));
					join.children[1] = move(new_right);
					join.op_hint = OpHint::MSJ;
					rai_info->forward = true;
					rai_info->alists = rai_info->rai->GetALists(EdgeDirection::FORWARD);
				}
			}
		}
		break;
	}
	case RAILRInfo::SOURCE_EDGE: {
		if (join.children[1]->op_hint == OpHint::SJ) {
			auto right = reinterpret_cast<LogicalComparisonJoin *>(join.children[1].get());
			auto &right_rai_info = right->conditions[0].rai_info;
			if (right_rai_info->rai == rai_info->rai && right->conditions.size() == 1 &&
			    right_rai_info->rai_lr_info == RAILRInfo::EDGE_DEST &&
			    right_rai_info->rai->edge_direction == EdgeDirection::UNDIRECTED &&
			    right->children[0]->type == LogicalOperatorType::GET) {
				auto edge_get = reinterpret_cast<LogicalGet *>(right->children[0].get());
				bool is_mergable = edge_get->column_ids.size() <= 2;
				for (auto col : edge_get->column_ids) {
					if (edge_get->table->join_index_columns_map.find(col) ==
					    edge_get->table->join_index_columns_map.end()) {
						is_mergable = false;
						break;
					}
				}
				if (is_mergable) {
					// perform merge: SOURCE-[EDGE]-DESTINATION
					auto &new_right = right->children[1];
					auto &merge_child = right->children[0];
					column_ref_equivalence_cache
					    [reinterpret_cast<BoundColumnRefExpression *>(join.conditions[0].right.get())->binding] =
					        reinterpret_cast<BoundColumnRefExpression *>(join.conditions[0].left.get())->binding;
					column_ref_equivalence_cache
					    [reinterpret_cast<BoundColumnRefExpression *>(right->conditions[0].left.get())->binding] =
					        reinterpret_cast<BoundColumnRefExpression *>(right->conditions[0].right.get())->binding;
					join.conditions[0].right = move(right->conditions[0].right);
					join.AddChild(move(merge_child));
					join.children[1] = move(new_right);
					join.op_hint = OpHint::MSJ;
					rai_info->forward = false;
					rai_info->alists = rai_info->rai->GetALists(EdgeDirection::BACKWARD);
				}
			}
		}
		break;
	}
	default:
		return;
	}
}
} // namespace duckdb
