#include "duckdb/parser/statement/create_statement.hpp"
#include "duckdb/parser/parsed_data/create_schema_info.hpp"
#include "duckdb/parser/transformer.hpp"

using namespace duckdb;
using namespace std;

unique_ptr<CreateStatement> Transformer::TransformCreateSchema(duckdb_libpgquery::PGNode *node) {
	auto stmt = reinterpret_cast<duckdb_libpgquery::PGCreateSchemaStmt *>(node);
	assert(stmt);
	auto result = make_unique<CreateStatement>();
	auto info = make_unique<CreateSchemaInfo>();

	assert(stmt->schemaname);
	info->schema = stmt->schemaname;
	info->on_conflict = stmt->if_not_exists ? OnCreateConflict::IGNORE : OnCreateConflict::ERROR;

	if (stmt->schemaElts) {
		// schema elements
		for (auto cell = stmt->schemaElts->head; cell != nullptr; cell = cell->next) {
			auto node = reinterpret_cast<duckdb_libpgquery::PGNode *>(cell->data.ptr_value);
			switch (node->type) {
			case duckdb_libpgquery::T_PGCreateStmt:
			case duckdb_libpgquery::T_PGViewStmt:
			default:
				throw NotImplementedException("Schema element not supported yet!");
			}
		}
	}
	result->info = move(info);
	return result;
}
