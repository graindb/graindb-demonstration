#include "duckdb/catalog/catalog_entry/vertex_catalog_entry.hpp"
#include "duckdb/parser/tableref/basetableref.hpp"
#include "duckdb/parser/tableref/vertexref.hpp"
#include "duckdb/planner/binder.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/planner/tableref/bound_vertexref.hpp"

using namespace duckdb;
using namespace std;

unique_ptr<BoundTableRef> Binder::Bind(VertexRef &ref) {
	if (ref.vertex_label.empty()) {
		// In SELECT statement, this should already bound in the context, otherwise throw exception
		auto table_binding = bind_context.GetBaseTable(ref.alias);
		auto vertex_binding = bind_context.GetVertex(ref.alias);
		if (table_binding && vertex_binding) {
			auto bound_table = make_unique<BoundBaseTableRef>(table_binding->get.Copy());
			bound_table->table_index = table_binding->index;
			return make_unique<BoundVertexRef>(ref.alias, move(bound_table), &vertex_binding->vertex);
		} else {
			throw InternalException("Given vertex variable %s cannot be found during binding.", ref.alias.c_str());
		}
	}
	auto vertex = reinterpret_cast<VertexCatalogEntry *>(
	    Catalog::GetCatalog(context).GetEntry(context, CatalogType::VERTEX, DEFAULT_SCHEMA, ref.vertex_label));
	auto vertex_table = vertex->base_table;
	// base table: create the BoundBaseTableRef node
	auto table_index = GenerateTableIndex();
	auto logical_get = make_unique<LogicalGet>(vertex_table, table_index);
	auto alias = ref.alias.empty() ? vertex_table->name : ref.alias;
	logical_get->table_alias = alias;
	bind_context.AddVertex(table_index, alias, *vertex, *logical_get);
	auto base_table = make_unique<BoundBaseTableRef>(move(logical_get));
	base_table->table_index = table_index;

	auto bound = make_unique<BoundVertexRef>(ref.vertex_label, move(base_table), vertex);
	return bound;
}
