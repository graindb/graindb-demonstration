#include "duckdb/execution/operator/schema/physical_create_vertex.hpp"
#include "duckdb/execution/physical_plan_generator.hpp"
#include "duckdb/planner/operator/logical_create_vertex.hpp"
#include "duckdb/planner/parsed_data/bound_create_vertex_info.hpp"
#include "duckdb/planner/tableref/bound_basetableref.hpp"

using namespace duckdb;
using namespace std;

static unordered_set<CatalogEntry *> ExtractDependencies(BoundCreateVertexInfo *info) {
	unordered_set<CatalogEntry *> result;
	auto base_table_op = reinterpret_cast<LogicalGet *>(info->base_table->get.get());
	result.insert(base_table_op->table);
	return result;
}

unique_ptr<PhysicalOperator> PhysicalPlanGenerator::CreatePlan(LogicalCreateVertex &op) {
	dependencies = ExtractDependencies(op.info.get());

	auto create_vertex = make_unique<PhysicalCreateVertex>(op, op.info->name, move(op.info));
	return create_vertex;
}
