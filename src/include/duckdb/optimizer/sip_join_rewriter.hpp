#pragma once

#include "duckdb/optimizer/optimizer.hpp"

namespace duckdb {

class SIPJoinRewriter : public LogicalOperatorVisitor {

public:
	//! Search for joins to be rewritten
	unique_ptr<LogicalOperator> Rewrite(unique_ptr<LogicalOperator> op);

	//! Override this function to search for join operators
	void VisitOperator(LogicalOperator &op) override;

private:
	bool RewriteInternal(LogicalComparisonJoin &join, idx_t join_cond_idx);
};
} // namespace duckdb
