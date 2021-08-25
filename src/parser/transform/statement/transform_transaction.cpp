#include "duckdb/parser/statement/transaction_statement.hpp"
#include "duckdb/parser/transformer.hpp"

using namespace duckdb;
using namespace std;

unique_ptr<TransactionStatement> Transformer::TransformTransaction(duckdb_libpgquery::PGNode *node) {
	auto stmt = reinterpret_cast<duckdb_libpgquery::PGTransactionStmt *>(node);
	assert(stmt);
	switch (stmt->kind) {
	case duckdb_libpgquery::PG_TRANS_STMT_BEGIN:
	case duckdb_libpgquery::PG_TRANS_STMT_START:
		return make_unique<TransactionStatement>(TransactionType::BEGIN_TRANSACTION);
	case duckdb_libpgquery::PG_TRANS_STMT_COMMIT:
		return make_unique<TransactionStatement>(TransactionType::COMMIT);
	case duckdb_libpgquery::PG_TRANS_STMT_ROLLBACK:
		return make_unique<TransactionStatement>(TransactionType::ROLLBACK);
	default:
		throw NotImplementedException("Transaction type %d not implemented yet", stmt->kind);
	}
}
