#include "duckdb/execution/operator/schema/physical_create_edge.hpp"
#include "duckdb/execution/physical_plan_generator.hpp"
#include "duckdb/planner/operator/logical_create_edge.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/planner/parsed_data/bound_create_edge_info.hpp"
#include "duckdb/planner/tableref/bound_basetableref.hpp"

using namespace duckdb;
using namespace std;

static unordered_set<CatalogEntry *> ExtractDependencies(BoundCreateEdgeInfo *info) {
	unordered_set<CatalogEntry *> result;
	auto edge_base_table = reinterpret_cast<BoundBaseTableRef *>(info->edge_table.get());
	auto edge_table_op = reinterpret_cast<LogicalGet *>(edge_base_table->get.get());
	result.insert(edge_table_op->table);

	assert(info->vertices.size() == 2);
	for (auto &vertex_ref : info->vertices) {
		auto base_table = reinterpret_cast<BoundBaseTableRef *>(vertex_ref->base_table.get());
		auto table_op = reinterpret_cast<LogicalGet *>(base_table->get.get());
		result.insert(table_op->table);
	}
	return result;
}

unique_ptr<PhysicalOperator> PhysicalPlanGenerator::CreatePlan(LogicalCreateEdge &op) {
	dependencies = ExtractDependencies(op.info.get());

	assert(op.children.size() == 1 && op.children[0]);
	auto create_edge = make_unique<PhysicalCreateEdge>(op, op.info->name, move(op.info));
	create_edge->children.push_back(CreatePlan(move(op.children[0])));
	return create_edge;
}
