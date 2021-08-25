#include "duckdb/planner/logical_operator.hpp"

#include "duckdb/common/printer.hpp"
#include "duckdb/common/string_util.hpp"

using namespace duckdb;
using namespace std;

string LogicalOperator::ParamsToString() const {
	string result;
	if (!expressions.empty()) {
		result += "[";
		result += StringUtil::Join(expressions, expressions.size(), ", ",
		                           [](const unique_ptr<Expression> &expression) { return expression->GetName(); });
		result += "]";
	}

	return result;
}

void LogicalOperator::ResolveOperatorTypes() {
	// if (types.size() > 0) {
	// 	// types already resolved for this node
	// 	return;
	// }
	types.clear();
	// first resolve child types
	for (auto &child : children) {
		child->ResolveOperatorTypes();
	}
	// now resolve the types for this operator
	ResolveTypes();
}

vector<ColumnBinding> LogicalOperator::GenerateColumnBindings(idx_t table_idx, idx_t column_count) {
	vector<ColumnBinding> result;
	for (idx_t i = 0; i < column_count; i++) {
		result.emplace_back(table_idx, i);
	}
	return result;
}

vector<TypeId> LogicalOperator::MapTypes(vector<TypeId> types, const vector<idx_t> &projection_map) {
	if (projection_map.empty()) {
		return types;
	} else {
		vector<TypeId> result_types;
		for (auto index : projection_map) {
			result_types.push_back(types[index]);
		}
		return result_types;
	}
}

vector<ColumnBinding> LogicalOperator::MapBindings(vector<ColumnBinding> bindings,
                                                   const vector<idx_t> &projection_map) {
	if (projection_map.empty()) {
		return bindings;
	} else {
		vector<ColumnBinding> result_bindings;
		for (auto index : projection_map) {
			result_bindings.push_back(bindings[index]);
		}
		return result_bindings;
	}
}

string LogicalOperator::ToString(idx_t depth) const {
	string result = LogicalOperatorToString(type);
	result += ParamsToString();
	if (!children.empty()) {
		for (const auto &child : children) {
			result += "\n" + string(depth * 4, ' ');
			result += child->ToString(depth + 1);
		}
		result += "";
	}
	return result;
}

static string ToJSONRecursive(const LogicalOperator &node) {
	string result = R"({ "name": ")" + LogicalOperatorToString(node.type) + "\",\n";
	result += "\"timing\":" + StringUtil::Format("%.2f", 0) + ",\n";
	result += "\"cardinality\":" + to_string(0) + ",\n";
	result += R"("extra_info": ")" + node.ParamsToString() + "\",\n";
	result += "\"children\": [";
	result += StringUtil::Join(node.children, node.children.size(), ",\n",
	                           [](const unique_ptr<LogicalOperator> &child) { return ToJSONRecursive(*child); });
	result += "]\n}\n";
	return result;
}

string LogicalOperator::ToJSON() const {
	string result = "{ \"result\": " + to_string(0.1) + ",\n";
	// print the phase timings
	result += "\"timings\": {},\n";
	// recursively print the physical operator tree
	result += "\"tree\": ";
	result += ToJSONRecursive(*this);

	return result + "}";
}

void LogicalOperator::Print() const {
	Printer::Print(ToString());
}
