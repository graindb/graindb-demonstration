#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/parser/transformer.hpp"

using namespace duckdb;
using namespace std;

SQLType Transformer::TransformTypeName(duckdb_libpgquery::PGTypeName *type_name) {
	auto name = (reinterpret_cast<duckdb_libpgquery::PGValue *>(type_name->names->tail->data.ptr_value)->val.str);
	// transform it to the SQL type
	return TransformStringToSQLType(name);
}
