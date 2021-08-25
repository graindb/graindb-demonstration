#include "duckdb/parser/expression/constant_expression.hpp"
#include "duckdb/parser/statement/select_statement.hpp"
#include "duckdb/parser/transformer.hpp"

using namespace duckdb;
using namespace std;

bool Transformer::TransformOrderBy(duckdb_libpgquery::PGList *order, vector<OrderByNode> &result) {
	if (!order) {
		return false;
	}

	for (auto node = order->head; node != nullptr; node = node->next) {
		auto temp = reinterpret_cast<duckdb_libpgquery::PGNode *>(node->data.ptr_value);
		if (temp->type == duckdb_libpgquery::T_PGSortBy) {
			OrderByNode ordernode;
			auto sort = reinterpret_cast<duckdb_libpgquery::PGSortBy *>(temp);
			auto target = sort->node;
			if (sort->sortby_dir == duckdb_libpgquery::PG_SORTBY_ASC || sort->sortby_dir == duckdb_libpgquery::PG_SORTBY_DEFAULT) {
				ordernode.type = OrderType::ASCENDING;
			} else if (sort->sortby_dir == duckdb_libpgquery::PG_SORTBY_DESC) {
				ordernode.type = OrderType::DESCENDING;
			} else {
				throw NotImplementedException("Unimplemented order by type");
			}
			ordernode.expression = TransformExpression(target);
			result.push_back(OrderByNode(ordernode.type, move(ordernode.expression)));
		} else {
			throw NotImplementedException("ORDER BY list member type %d\n", temp->type);
		}
	}
	return true;
}
