#include "duckdb/parser/statement/create_statement.hpp"
#include "duckdb/parser/transformer.hpp"
#include "duckdb/parser/parsed_data/create_view_info.hpp"

using namespace duckdb;
using namespace std;

unique_ptr<CreateStatement> Transformer::TransformCreateView(duckdb_libpgquery::PGNode *node) {
	assert(node);
	assert(node->type == duckdb_libpgquery::T_PGViewStmt);

	auto stmt = reinterpret_cast<duckdb_libpgquery::PGViewStmt *>(node);
	assert(stmt);
	assert(stmt->view);

	auto result = make_unique<CreateStatement>();
	auto info = make_unique<CreateViewInfo>();

	if (stmt->view->schemaname) {
		info->schema = stmt->view->schemaname;
	}
	info->view_name = stmt->view->relname;
	info->temporary = !stmt->view->relpersistence;
	if (info->temporary) {
		info->schema = TEMP_SCHEMA;
	}
	info->on_conflict = stmt->replace ? OnCreateConflict::REPLACE : OnCreateConflict::ERROR;

	info->query = TransformSelectNode((duckdb_libpgquery::PGSelectStmt *)stmt->query);

	if (stmt->aliases && stmt->aliases->length > 0) {
		for (auto c = stmt->aliases->head; c != NULL; c = lnext(c)) {
			auto node = reinterpret_cast<duckdb_libpgquery::PGNode *>(c->data.ptr_value);
			switch (node->type) {
			case duckdb_libpgquery::T_PGString: {
				auto val = (duckdb_libpgquery::PGValue *)node;
				info->aliases.push_back(string(val->val.str));
				break;
			}
			default:
				throw NotImplementedException("View projection type");
			}
		}
		if (info->aliases.size() < 1) {
			throw ParserException("Need at least one column name in CREATE VIEW projection list");
		}
	}

	if (stmt->options && stmt->options->length > 0) {
		throw NotImplementedException("VIEW options");
	}

	if (stmt->withCheckOption != duckdb_libpgquery::PGViewCheckOption::PG_NO_CHECK_OPTION) {
		throw NotImplementedException("VIEW CHECK options");
	}
	result->info = move(info);
	return result;
}
