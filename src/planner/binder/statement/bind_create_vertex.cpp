#include "duckdb/parser/parsed_data/create_vertex_info.hpp"
#include "duckdb/planner/binder.hpp"
#include "duckdb/planner/operator/list.hpp"
#include "duckdb/planner/parsed_data/bound_create_vertex_info.hpp"
#include "duckdb/planner/tableref/bound_basetableref.hpp"

namespace duckdb {

unique_ptr<BoundCreateVertexInfo> Binder::BindCreateVertexInfo(unique_ptr<CreateInfo> info) {
	auto &base = (CreateVertexInfo &)*info;
	auto schema = Catalog::GetCatalog(context).GetSchema(context);
	auto base_table = unique_ptr_cast<BoundTableRef, BoundBaseTableRef>(Bind(*base.base_table));
	TableCatalogEntry *base_table_entry = reinterpret_cast<LogicalGet *>(base_table->get.get())->table;

	// Perform the check if base table has a primary key
	bool has_single_primary_key = false;
	for (auto &constraint : base_table_entry->constraints) {
		if (constraint->type == ConstraintType::UNIQUE) {
			auto unique_constraint = reinterpret_cast<UniqueConstraint *>(constraint.get());
			if (unique_constraint->is_primary_key && unique_constraint->columns.empty()) {
				has_single_primary_key = true;
				base_table_entry->primary_key_column = unique_constraint->index;
			}
		}
	}
	if (!has_single_primary_key) {
		throw InternalException("Cannot find a single primary key on the vertex table to create.");
	}

	auto result = make_unique<BoundCreateVertexInfo>(move(info), base.name, schema, move(base_table), base_table_entry);

	return result;
}
} // namespace duckdb
