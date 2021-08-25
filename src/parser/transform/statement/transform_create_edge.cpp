#include "duckdb/parser/expression/columnref_expression.hpp"
#include "duckdb/parser/parsed_data/create_edge_info.hpp"
#include "duckdb/parser/statement/create_statement.hpp"
#include "duckdb/parser/tableref/vertexref.hpp"
#include "duckdb/parser/transformer.hpp"

using namespace duckdb;
using namespace std;

unique_ptr<CreateStatement> Transformer::TransformCreateEdge(duckdb_libpgquery::PGNode *node) {
	auto stmt = reinterpret_cast<duckdb_libpgquery::PGCreateEdgeStmt *>(node);
	assert(stmt);
	auto result = make_unique<CreateStatement>();

	auto edge_table = make_unique<BaseTableRef>();
	edge_table->table_name = stmt->table->relname;
	string edge_table_name = edge_table->table_name;
	if (stmt->table->schemaname) {
		edge_table->schema_name = stmt->table->schemaname;
	}
	auto info = make_unique<CreateEdgeInfo>(stmt->name->relname, move(edge_table), EdgeDirection::UNDIRECTED);
	auto from_vertex = make_unique<VertexRef>();
	from_vertex->vertex_label = stmt->from_ref->relname;
	from_vertex->alias = string(stmt->from_ref->relname) + "_from";
	auto to_vertex = make_unique<VertexRef>();
	to_vertex->vertex_label = stmt->to_ref->relname;
	to_vertex->alias = string(stmt->to_ref->relname) + "_to";
	info->reference_columns.push_back(make_unique<ColumnRefExpression>(stmt->from_col, stmt->table->relname));
	info->reference_columns.push_back(make_unique<ColumnRefExpression>(stmt->to_col, stmt->table->relname));
	info->vertices.push_back(move(from_vertex));
	info->vertices.push_back(move(to_vertex));

	result->info = move(info);
	return result;
}
