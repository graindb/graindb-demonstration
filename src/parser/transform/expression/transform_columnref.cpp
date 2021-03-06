#include "duckdb/common/exception.hpp"
#include "duckdb/parser/expression/columnref_expression.hpp"
#include "duckdb/parser/expression/star_expression.hpp"
#include "duckdb/parser/expression/table_star_expression.hpp"
#include "duckdb/parser/transformer.hpp"

using namespace duckdb;
using namespace std;

unique_ptr<ParsedExpression> Transformer::TransformColumnRef(duckdb_libpgquery::PGColumnRef *root) {
	auto fields = root->fields;
	switch ((reinterpret_cast<duckdb_libpgquery::PGNode *>(fields->head->data.ptr_value))->type) {
	case duckdb_libpgquery::T_PGString: {
		if (fields->length < 1 || fields->length > 2) {
			throw ParserException("Unexpected field length");
		}
		string column_name, table_name;
		if (fields->length == 1) {
			column_name = string(reinterpret_cast<duckdb_libpgquery::PGValue *>(fields->head->data.ptr_value)->val.str);
			return make_unique<ColumnRefExpression>(column_name, table_name);
		} else {
			table_name = string(reinterpret_cast<duckdb_libpgquery::PGValue *>(fields->head->data.ptr_value)->val.str);
			auto col_node = reinterpret_cast<duckdb_libpgquery::PGNode *>(fields->head->next->data.ptr_value);
			switch (col_node->type) {
			case duckdb_libpgquery::T_PGString: {
				column_name = string(reinterpret_cast<duckdb_libpgquery::PGValue *>(col_node)->val.str);
				return make_unique<ColumnRefExpression>(column_name, table_name);
			}
			case duckdb_libpgquery::T_PGAStar: {
				return make_unique<TableStarExpression>(table_name);
			}
			default:
				throw NotImplementedException("ColumnRef not implemented!");
			}
		}
	}
	case duckdb_libpgquery::T_PGAStar: {
		return make_unique<StarExpression>();
	}
	default:
		break;
	}
	throw NotImplementedException("ColumnRef not implemented!");
}
