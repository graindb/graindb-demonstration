#include "duckdb/catalog/catalog_entry/edge_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/schema_catalog_entry.hpp"
#include "duckdb/parser/expression/columnref_expression.hpp"
#include "duckdb/parser/expression/comparison_expression.hpp"
#include "duckdb/parser/expression/conjunction_expression.hpp"
#include "duckdb/parser/tableref/joinref.hpp"
#include "duckdb/parser/tableref/path_joinref.hpp"
#include "duckdb/planner/binder.hpp"
#include "duckdb/planner/expression_binder/where_binder.hpp"
#include "duckdb/planner/tableref/bound_joinref.hpp"

using namespace duckdb;
using namespace std;

// edge and destination vertex in path element
static unique_ptr<ParsedExpression> GetEdgeVertexJoinExpression(EdgeCatalogEntry *edge, const string &vertex_alias,
                                                                const string &rel_alias, bool is_forward) {
	unique_ptr<ColumnRefExpression> left, right;
	if (is_forward) {
		// edge and destination vertex
		auto edge_join_column = edge->reference_column_names[1];
		right = make_unique<ColumnRefExpression>(edge_join_column, rel_alias);
		assert(edge->vertices[1]->base_table->primary_key_column == edge->vertex_primary_column_ids[1]);
		auto vertex_join_column = edge->vertices[1]->base_table->columns[edge->vertex_primary_column_ids[1]].name;
		left = make_unique<ColumnRefExpression>(vertex_join_column, vertex_alias);
	} else {
		// edge and source vertex
		auto edge_join_column = edge->reference_column_names[0];
		right = make_unique<ColumnRefExpression>(edge_join_column, rel_alias);
		assert(edge->vertices[0]->base_table->primary_key_column == edge->vertex_primary_column_ids[0]);
		auto vertex_join_column = edge->vertices[0]->base_table->columns[edge->vertex_primary_column_ids[0]].name;
		left = make_unique<ColumnRefExpression>(vertex_join_column, vertex_alias);
	}
	return make_unique<ComparisonExpression>(ExpressionType::COMPARE_EQUAL, move(left), move(right));
}

unique_ptr<BoundTableRef> Binder::Bind(PathJoinRef &ref) {
	auto result = make_unique<BoundJoinRef>();
	// Bind Path Join
	result->type = JoinType::INNER;
	WhereBinder where_binder(*this, context);
	auto source_vertex = Bind(*ref.source_vertex);
	auto destination_vertex = Bind(*ref.destination_vertex);
	// Construct sub join
	auto sub_join = make_unique<BoundJoinRef>();
	sub_join->type = JoinType::INNER;
	sub_join->right = move(source_vertex);
	if (sub_join->right->type == TableReferenceType::VERTEX) {
		sub_join->right = move(reinterpret_cast<BoundVertexRef *>(sub_join->right.get())->base_table);
	}
	auto bound_edge = unique_ptr_cast<BoundTableRef, BoundEdgeRef>(Bind(*ref.edge));
	auto sub_join_edge = bound_edge->edge_entry;
	sub_join->left = move(bound_edge->base_table);
	auto sub_join_expr = GetEdgeVertexJoinExpression(sub_join_edge, ref.source_vertex->alias, ref.edge->alias,
	                                                 ref.edge->direction == EdgeDirection::BACKWARD);
	sub_join->condition = where_binder.Bind(sub_join_expr);

	// Construct root join
	result->right = move(sub_join);
	result->left = move(destination_vertex);
	if (result->left->type == TableReferenceType::VERTEX) {
		result->left = move(reinterpret_cast<BoundVertexRef *>(result->left.get())->base_table);
	}
	auto root_join_expr = GetEdgeVertexJoinExpression(sub_join_edge, ref.destination_vertex->alias, ref.edge->alias,
	                                                  ref.edge->direction == EdgeDirection::FORWARD);
	result->condition = where_binder.Bind(root_join_expr);

	return move(result);
}
