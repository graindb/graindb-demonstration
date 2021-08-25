//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/planner/expression_binder/rai_binder.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/planner/expression_binder.hpp"

namespace duckdb {
class BoundColumnRefExpression;

class EdgeBinder : public ExpressionBinder {
public:
	EdgeBinder(Binder &binder, ClientContext &context) : ExpressionBinder(binder, context) {
	}

protected:
	BindResult BindExpression(ParsedExpression &expr, idx_t depth, bool root_expression) override;

	string UnsupportedAggregateMessage() override;
};
} // namespace duckdb
