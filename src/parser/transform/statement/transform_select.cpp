#include "duckdb/common/string_util.hpp"
#include "duckdb/parser/statement/select_statement.hpp"
#include "duckdb/parser/transformer.hpp"

using namespace duckdb;
using namespace std;

unique_ptr<SelectStatement> Transformer::TransformSelect(duckdb_libpgquery::PGNode *node) {
	auto stmt = reinterpret_cast<duckdb_libpgquery::PGSelectStmt *>(node);
	auto result = make_unique<SelectStatement>();

	// may contain windows so second
	if (stmt->withClause) {
		TransformCTE(reinterpret_cast<duckdb_libpgquery::PGWithClause *>(stmt->withClause), *result);
	}

	result->node = TransformSelectNode(stmt);
	return result;
}
