#include "expression_helper.hpp"

#include "duckdb/optimizer/filter_pushdown.hpp"
#include "duckdb/optimizer/in_clause_rewriter.hpp"
#include "duckdb/optimizer/join_order_optimizer.hpp"
#include "duckdb/optimizer/remove_unused_columns.hpp"
#include "duckdb/optimizer/rule/list.hpp"
#include "duckdb/optimizer/sip_join_merger.hpp"
#include "duckdb/optimizer/sip_join_rewriter.hpp"
#include "duckdb/optimizer/topn_optimizer.hpp"
#include "duckdb/parser/parser.hpp"
#include "duckdb/planner/binder.hpp"
#include "duckdb/planner/expression_iterator.hpp"
#include "duckdb/planner/operator/logical_projection.hpp"
#include "duckdb/planner/planner.hpp"
#include "duckdb/planner/query_node/bound_select_node.hpp"

#include <iostream>

using namespace duckdb;
using namespace std;

ExpressionHelper::ExpressionHelper() : db(nullptr), con(db), rewriter(*con.context) {
	con.Query("BEGIN TRANSACTION");
}

bool ExpressionHelper::VerifyRewrite(const string &input, const string &expected_output, bool silent) {
	auto root = ParseExpression(input);
	auto result = ApplyExpressionRule(move(root));
	auto expected_result = ParseExpression(expected_output);
	bool equals = Expression::Equals(result.get(), expected_result.get());
	if (!equals && !silent) {
		printf("Optimized result does not equal expected result!\n");
		result->Print();
		printf("Expected:\n");
		expected_result->Print();
	}
	return equals;
}

string ExpressionHelper::AddColumns(const string &columns) {
	if (!from_clause.empty()) {
		con.Query("DROP TABLE expression_helper");
	}
	auto result = con.Query("CREATE TABLE expression_helper(" + columns + ")");
	if (!result->success) {
		return result->error;
	}
	from_clause = " FROM expression_helper";
	return string();
}

unique_ptr<Expression> ExpressionHelper::ParseExpression(const string &expression) {
	string query = "SELECT " + expression + from_clause;

	Parser parser;
	parser.ParseQuery(query);
	if (parser.statements.empty() || parser.statements[0]->type != StatementType::SELECT_STATEMENT) {
		return nullptr;
	}
	Binder binder(*con.context);
	auto bound_statement = binder.Bind(*parser.statements[0]);
	assert(bound_statement.plan->type == LogicalOperatorType::PROJECTION);
	return move(bound_statement.plan->expressions[0]);
}

unique_ptr<LogicalOperator> ExpressionHelper::ParseLogicalTree(const string &query) const {
	Parser parser;
	parser.ParseQuery(query);
	if (parser.statements.empty() || parser.statements[0]->type != StatementType::SELECT_STATEMENT) {
		return nullptr;
	}
	Planner planner(*con.context);
	planner.CreatePlan(move(parser.statements[0]));
	return move(planner.plan);
}

unique_ptr<LogicalOperator> ExpressionHelper::ParseAndOptimizeLogicalTree(const string &query) const {
	Parser parser;
	parser.ParseQuery(query);
	if (parser.statements.empty() || parser.statements[0]->type != StatementType::SELECT_STATEMENT) {
		return nullptr;
	}
	Planner planner(*con.context);
	planner.CreatePlan(move(parser.statements[0]));
	auto plan = move(planner.plan);

	Binder binder(*con.context);
	Optimizer optimizer(binder, *con.context);
	FilterPushdown filterPushdown(optimizer);
	plan = filterPushdown.Rewrite(move(plan));
	JoinOrderOptimizer join_order_optimizer(*con.context);
	plan = join_order_optimizer.Optimize(move(plan));
	SIPJoinRewriter sip_join_optimizer;
	plan = sip_join_optimizer.Rewrite(move(plan));
	RemoveUnusedColumns unused(true);
	unused.VisitOperator(*plan);

	return plan;
}

unique_ptr<Expression> ExpressionHelper::ApplyExpressionRule(unique_ptr<Expression> root) {
	// make a logical projection
	vector<unique_ptr<Expression>> expressions;
	expressions.push_back(move(root));
	auto proj = make_unique<LogicalProjection>(0, move(expressions));
	rewriter.Apply(*proj);
	return move(proj->expressions[0]);
}
