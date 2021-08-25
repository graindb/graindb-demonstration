#include "duckdb/parser/statement/explain_statement.hpp"
#include "duckdb/parser/transformer.hpp"

using namespace duckdb;
using namespace std;

unique_ptr<ExplainStatement> Transformer::TransformExplain(duckdb_libpgquery::PGNode *node) {
    duckdb_libpgquery::PGExplainStmt *stmt = reinterpret_cast<duckdb_libpgquery::PGExplainStmt *>(node);
	assert(stmt);
	return make_unique<ExplainStatement>(TransformStatement(stmt->query));
}
