#include "duckdb/parser/expression/comparison_expression.hpp"
#include "duckdb/parser/parsed_data/create_edge_info.hpp"
#include "duckdb/parser/tableref/joinref.hpp"
#include "duckdb/planner/binder.hpp"
#include "duckdb/planner/expression/bound_comparison_expression.hpp"
#include "duckdb/planner/expression_binder/edge_binder.hpp"
#include "duckdb/planner/operator/list.hpp"
#include "duckdb/planner/parsed_data/bound_create_edge_info.hpp"
#include "duckdb/planner/tableref/bound_basetableref.hpp"
#include "duckdb/planner/tableref/bound_vertexref.hpp"

namespace duckdb {

static unique_ptr<QueryNode> CreateSubPlanQueryNode(const string &schema, const string &edge_table_name,
                                                    const vector<string> &vertex_table_names,
                                                    const vector<string> &vertex_column_names,
                                                    const vector<string> &reference_column_names) {
	auto query_node = make_unique<SelectNode>();
	// Select list: vector of ColumnRefExpression
	auto src_vertex_alias = vertex_table_names[0] + "_from_";
	auto dst_vertex_alias = vertex_table_names[1] + "_to_";
	query_node->select_list.push_back(make_unique<ColumnRefExpression>(COLUMN_NAME_ROW_ID, src_vertex_alias));
	query_node->select_list.push_back(make_unique<ColumnRefExpression>(COLUMN_NAME_ROW_ID, dst_vertex_alias));
	// From table ref: JoinRef
	auto src_table_ref = make_unique<BaseTableRef>(schema, vertex_table_names[0]);
	src_table_ref->alias = src_vertex_alias;
	auto edge_table_ref = make_unique<BaseTableRef>(schema, edge_table_name);
	edge_table_ref->alias = edge_table_name + "_edge_";
	auto edge_table_alias = edge_table_ref->alias;
	auto src_edge_filter = make_unique<ComparisonExpression>(
	    ExpressionType::COMPARE_EQUAL, make_unique<ColumnRefExpression>(reference_column_names[0], edge_table_alias),
	    make_unique<ColumnRefExpression>(vertex_column_names[0], src_vertex_alias));
	auto src_edge_table_ref =
	    make_unique<JoinRef>(move(src_table_ref), move(edge_table_ref), move(src_edge_filter), JoinType::RIGHT);
	auto dst_table_ref = make_unique<BaseTableRef>(schema, vertex_table_names[1]);
	dst_table_ref->alias = dst_vertex_alias;
	auto dst_edge_filter = make_unique<ComparisonExpression>(
	    ExpressionType::COMPARE_EQUAL, make_unique<ColumnRefExpression>(reference_column_names[1], edge_table_alias),
	    make_unique<ColumnRefExpression>(vertex_column_names[1], dst_vertex_alias));
	auto from_table_ref =
	    make_unique<JoinRef>(move(dst_table_ref), move(src_edge_table_ref), move(dst_edge_filter), JoinType::RIGHT);
	query_node->from_table = move(from_table_ref);
	// Order by
	vector<OrderByNode> orders;
	auto order_by_expression = make_unique<ColumnRefExpression>(COLUMN_NAME_ROW_ID, edge_table_alias);
	orders.emplace_back(OrderType::ASCENDING, move(order_by_expression));
	auto order_modifier = make_unique<OrderModifier>();
	order_modifier->orders = move(orders);
	query_node->modifiers.push_back(move(order_modifier));

	return query_node;
}

unique_ptr<BoundCreateEdgeInfo> Binder::BindCreateEdgeInfo(unique_ptr<CreateInfo> info) {
	auto &base = (CreateEdgeInfo &)*info;
	auto schema = Catalog::GetCatalog(context).GetSchema(context);
	auto edge_table = unique_ptr_cast<BoundTableRef, BoundBaseTableRef>(Bind(*base.edge_table));
	TableCatalogEntry *edge_table_entry = reinterpret_cast<LogicalGet *>(edge_table->get.get())->table;

	vector<unique_ptr<BoundVertexRef>> vertices;
	for (auto &ref : base.vertices) {
		vertices.push_back(unique_ptr_cast<BoundTableRef, BoundVertexRef>(Bind(*ref)));
	}

	vector<unique_ptr<Expression>> reference_columns;
	vector<string> reference_column_names;
	vector<string> vertex_column_names;
	EdgeBinder binder(*this, context);
	for (auto &expr : base.reference_columns) {
		reference_column_names.push_back(reinterpret_cast<ColumnRefExpression *>(expr.get())->column_name);
		reference_columns.push_back(binder.Bind(expr));
	}
	vector<column_t> edge_reference_column_ids;
	for (auto &expr : reference_columns) {
		auto column_id = reinterpret_cast<BoundColumnRefExpression *>(expr.get())->binding.column_ordinal;
		edge_reference_column_ids.push_back(column_id);
	}

	vector<unique_ptr<ParsedExpression>> vertex_column_parsed_expressions;
	vector<column_t> vertex_primary_column_ids;
	vector<VertexCatalogEntry *> vertex_entries;
	vector<string> vertex_table_names;
	for (auto &vertex_ref : vertices) {
		auto table = reinterpret_cast<LogicalGet *>(vertex_ref->base_table->get.get())->table;
		auto &primary_key_column = table->columns[table->primary_key_column];
		vertex_column_names.push_back(primary_key_column.name);
		vertex_primary_column_ids.push_back(table->primary_key_column);
		auto vertex_column_expr = make_unique<ColumnRefExpression>(primary_key_column.name, table->name);
		vertex_table_names.push_back(table->name);
		vertex_column_parsed_expressions.push_back(move(vertex_column_expr));
		vertex_entries.push_back(vertex_ref->vertex_entry);
	}

	auto query_node = Bind(*CreateSubPlanQueryNode(schema->name, base.edge_table->table_name, vertex_table_names,
	                                               vertex_column_names, reference_column_names));

	auto result =
	    make_unique<BoundCreateEdgeInfo>(move(info), base.name, schema, move(edge_table), EdgeDirection::UNDIRECTED,
	                                     move(vertices), move(reference_columns), edge_table_entry,
	                                     move(vertex_entries), vertex_primary_column_ids, edge_reference_column_ids);
	result->reference_column_names = reference_column_names;
	result->query_node = move(query_node);
	return result;
}

} // namespace duckdb
