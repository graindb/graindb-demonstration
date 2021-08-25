#include "duckdb/planner/binder.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/planner/tableref/bound_vertexref.hpp"

using namespace duckdb;
using namespace std;

unique_ptr<LogicalOperator> Binder::CreatePlan(BoundVertexRef &ref) {
	return CreatePlan(*ref.base_table);
}
