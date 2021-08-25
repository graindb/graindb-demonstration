#include "duckdb/parser/statement/drop_statement.hpp"
#include "duckdb/parser/transformer.hpp"

using namespace duckdb;
using namespace std;

unique_ptr<SQLStatement> Transformer::TransformDrop(duckdb_libpgquery::PGNode *node) {
	auto stmt = (duckdb_libpgquery::PGDropStmt *)(node);
	auto result = make_unique<DropStatement>();
	auto &info = *result->info.get();
	assert(stmt);
	if (stmt->objects->length != 1) {
		throw NotImplementedException("Can only drop one object at a time");
	}
	switch (stmt->removeType) {
	case duckdb_libpgquery::PG_OBJECT_TABLE:
		info.type = CatalogType::TABLE;
		break;
	case duckdb_libpgquery::PG_OBJECT_SCHEMA:
		info.type = CatalogType::SCHEMA;
		break;
	case duckdb_libpgquery::PG_OBJECT_INDEX:
		info.type = CatalogType::INDEX;
		break;
	case duckdb_libpgquery::PG_OBJECT_VIEW:
		info.type = CatalogType::VIEW;
		break;
	case duckdb_libpgquery::PG_OBJECT_SEQUENCE:
		info.type = CatalogType::SEQUENCE;
		break;
	default:
		throw NotImplementedException("Cannot drop this type yet");
	}

	switch (stmt->removeType) {
	case duckdb_libpgquery::PG_OBJECT_SCHEMA:
		assert(stmt->objects && stmt->objects->length == 1);
		info.name = ((duckdb_libpgquery::PGValue *)stmt->objects->head->data.ptr_value)->val.str;
		break;
	default: {
		auto view_list = (duckdb_libpgquery::PGList *)stmt->objects->head->data.ptr_value;
		if (view_list->length == 2) {
			info.schema = ((duckdb_libpgquery::PGValue *)view_list->head->data.ptr_value)->val.str;
			info.name = ((duckdb_libpgquery::PGValue *)view_list->head->next->data.ptr_value)->val.str;
		} else {
			info.name = ((duckdb_libpgquery::PGValue *)view_list->head->data.ptr_value)->val.str;
		}
		break;
	}
	}
	info.cascade = stmt->behavior == duckdb_libpgquery::PGDropBehavior::PG_DROP_CASCADE;
	info.if_exists = stmt->missing_ok;
	return move(result);
}
