#include "duckdb/common/exception.hpp"
#include "duckdb/parser/expression/operator_expression.hpp"
#include "duckdb/parser/transformer.hpp"

using namespace duckdb;
using namespace std;

unique_ptr<ParsedExpression> Transformer::TransformNullTest(duckdb_libpgquery::PGNullTest *root) {
	assert(root);
	auto arg = TransformExpression(reinterpret_cast<duckdb_libpgquery::PGNode *>(root->arg));
	if (root->argisrow) {
		throw NotImplementedException("IS NULL argisrow");
	}
	ExpressionType expr_type =
	    (root->nulltesttype == duckdb_libpgquery::PG_IS_NULL) ? ExpressionType::OPERATOR_IS_NULL : ExpressionType::OPERATOR_IS_NOT_NULL;

	return unique_ptr<ParsedExpression>(new OperatorExpression(expr_type, move(arg)));
}
