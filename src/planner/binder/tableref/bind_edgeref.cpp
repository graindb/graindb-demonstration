#include "duckdb/catalog/catalog_entry/edge_catalog_entry.hpp"
#include "duckdb/parser/tableref/basetableref.hpp"
#include "duckdb/parser/tableref/edgeref.hpp"
#include "duckdb/planner/binder.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/planner/tableref/bound_edgeref.hpp"

using namespace duckdb;
using namespace std;

unique_ptr<BoundTableRef> Binder::Bind(EdgeRef &ref) {
	if (ref.edge_name.empty()) {
		// In SELECT statement, this should already bound in the context, otherwise throw exception
		auto table_binding = bind_context.GetBaseTable(ref.alias);
		if (table_binding) {
			auto bound_table = make_unique<BoundBaseTableRef>(table_binding->get.Copy());
			bound_table->table_index = table_binding->index;
			return make_unique<BoundEdgeRef>(ref.edge_name, move(bound_table), nullptr);
		} else {
			throw InternalException("Given edge is not associated with a edge name.");
		}
	}
	auto edge = reinterpret_cast<EdgeCatalogEntry *>(
	    Catalog::GetCatalog(context).GetEntry(context, CatalogType::EDGE, DEFAULT_SCHEMA, ref.edge_name));
	// Check edge definition
	auto source_vertex = bind_context.GetVertex(ref.src_vertex_alias);
	auto destination_vertex = bind_context.GetVertex(ref.dst_vertex_alias);
	auto expected_source_name =
	    ref.direction == EdgeDirection::FORWARD ? edge->vertices[0]->name : edge->vertices[1]->name;
	auto expected_destination_name =
	    ref.direction == EdgeDirection::FORWARD ? edge->vertices[1]->name : edge->vertices[0]->name;
	if (source_vertex->vertex.name != expected_source_name ||
	    destination_vertex->vertex.name != expected_destination_name) {
		throw BinderException("Given edge and vertices {(%s)-->(%s)} are not matched with definition {(%s)-->(%s)}.",
		                      source_vertex->vertex.name.c_str(), destination_vertex->vertex.name.c_str(),
		                      expected_source_name.c_str(), expected_destination_name.c_str());
	}
	// base table: create the BoundBaseTableRef node
	auto edge_table = edge->edge_table;
	auto table_index = GenerateTableIndex();
	auto logical_get = make_unique<LogicalGet>(edge_table, table_index);
	auto alias = ref.alias.empty() ? edge_table->name : ref.alias;
	logical_get->table_alias = alias;
	bind_context.AddBaseTable(table_index, alias, *edge_table, *logical_get);
	auto base_table = make_unique<BoundBaseTableRef>(move(logical_get));
	base_table->table_index = table_index;

	auto bound = make_unique<BoundEdgeRef>(ref.edge_name, move(base_table), edge);
	return bound;
}