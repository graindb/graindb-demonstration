#include "duckdb/planner/operator/logical_comparison_join.hpp"

#include "duckdb/common/string_util.hpp"

using namespace duckdb;
using namespace std;

LogicalComparisonJoin::LogicalComparisonJoin(JoinType join_type, LogicalOperatorType logical_type)
    : LogicalJoin(join_type, logical_type), enable_lookup_join(false) {
}

string LogicalComparisonJoin::ParamsToString() const {
	string result = "[" + JoinTypeToString(join_type);
	if (!conditions.empty()) {
		result += " ";
		result += StringUtil::Join(conditions, conditions.size(), ", ", [](const JoinCondition &condition) {
			return ExpressionTypeToString(condition.comparison) + "(" + condition.left->GetName() + ", " +
			       condition.right->GetName() + ")";
		});
		result += ", ";
		switch (op_hint) {
		case OpHint::HJ:
			result += "HASH JOIN";
			break;
		case OpHint::SJ:
			result += "SIP JOIN";
			break;
		case OpHint::NLJ:
			result += "NEST LOOP JOIN";
			break;
		case OpHint::MSJ:
			result += "MERGE SIP JOIN";
			break;
		default:
			result += "DEFAULT JOIN";
			break;
		}
		result += "]";
	}

	return result;
}
