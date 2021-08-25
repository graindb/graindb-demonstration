#include "duckdb/planner/binder.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/operator/list.hpp"
#include "duckdb/planner/operator/logical_expression_get.hpp"
#include "duckdb/planner/query_node/bound_select_node.hpp"

using namespace duckdb;
using namespace std;

unique_ptr<LogicalOperator> Binder::PlanFilter(unique_ptr<Expression> condition, unique_ptr<LogicalOperator> root) {
	PlanSubqueries(&condition, &root);
	auto filter = make_unique<LogicalFilter>(move(condition));
	filter->AddChild(move(root));
	return move(filter);
}

unique_ptr<LogicalOperator> Binder::CreatePlan(BoundSelectNode &statement) {
	unique_ptr<LogicalOperator> root;
	assert(statement.from_table);
	root = CreatePlan(*statement.from_table);
	assert(root);

	if (statement.where_clause) {
		root = PlanFilter(move(statement.where_clause), move(root));
	}

	if (!statement.aggregates.empty() || !statement.groups.empty()) {
		if (!statement.groups.empty()) {
			// visit the groups
			for (auto &group : statement.groups) {
				PlanSubqueries(&group, &root);
			}
		}
		// now visit all aggregate expressions
		for (auto &expr : statement.aggregates) {
			PlanSubqueries(&expr, &root);
		}
		// finally create the aggregate node with the group_index and aggregate_index as obtained from the binder
		auto aggregate =
		    make_unique<LogicalAggregate>(statement.group_index, statement.aggregate_index, move(statement.aggregates));
		aggregate->groups = move(statement.groups);

		aggregate->AddChild(move(root));
		root = move(aggregate);
	}

	if (statement.having) {
		PlanSubqueries(&statement.having, &root);
		auto having = make_unique<LogicalFilter>(move(statement.having));

		having->AddChild(move(root));
		root = move(having);
	}

	if (!statement.windows.empty()) {
		auto win = make_unique<LogicalWindow>(statement.window_index);
		win->expressions = move(statement.windows);
		// visit the window expressions
		for (auto &expr : win->expressions) {
			PlanSubqueries(&expr, &root);
		}
		assert(!win->expressions.empty());
		win->AddChild(move(root));
		root = move(win);
	}

	if (!statement.unnests.empty()) {
		auto unnest = make_unique<LogicalUnnest>(statement.unnest_index);
		unnest->expressions = move(statement.unnests);
		// visit the window expressions
		for (auto &expr : unnest->expressions) {
			PlanSubqueries(&expr, &root);
		}
		assert(!unnest->expressions.empty());
		unnest->AddChild(move(root));
		root = move(unnest);
	}

	for (auto &expr : statement.select_list) {
		PlanSubqueries(&expr, &root);
	}

	// create the projection
	auto proj = make_unique<LogicalProjection>(statement.projection_index, move(statement.select_list));
	auto &projection = *proj;
	proj->AddChild(move(root));
	root = move(proj);

	// finish the plan by handling the elements of the QueryNode
	root = VisitQueryNode(statement, move(root));

	// add a prune node if necessary
	if (statement.need_prune) {
		assert(root);
		vector<unique_ptr<Expression>> prune_expressions;
		for (idx_t i = 0; i < statement.column_count; i++) {
			prune_expressions.push_back(make_unique<BoundColumnRefExpression>(
			    projection.expressions[i]->return_type, ColumnBinding(statement.projection_index, i)));
		}
		auto prune = make_unique<LogicalProjection>(statement.prune_index, move(prune_expressions));
		prune->AddChild(move(root));
		root = move(prune);
	}
	return root;
}
