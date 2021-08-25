#include "duckdb/parser/parsed_data/create_vertex_info.hpp"
#include "duckdb/parser/transformer.hpp"

using namespace duckdb;
using namespace std;

unique_ptr<CreateStatement> Transformer::TransformCreateVertex(duckdb_libpgquery::PGNode *node) {
	auto stmt = reinterpret_cast<duckdb_libpgquery::PGCreateVertexStmt *>(node);
	assert(stmt);
	auto result = make_unique<CreateStatement>();

	auto base_table = make_unique<BaseTableRef>();
	base_table->table_name = stmt->table->relname;
	if (stmt->table->schemaname) {
		base_table->schema_name = stmt->table->schemaname;
	}

	auto info = make_unique<CreateVertexInfo>(stmt->name->relname, move(base_table));
	result->info = move(info);

	return result;
}