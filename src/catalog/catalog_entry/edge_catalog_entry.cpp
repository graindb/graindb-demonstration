#include "duckdb/catalog/catalog_entry/edge_catalog_entry.hpp"

#include "duckdb/catalog/catalog_entry/schema_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/vertex_catalog_entry.hpp"
#include "duckdb/parser/parsed_data/create_edge_info.hpp"

using namespace duckdb;
using namespace std;

EdgeCatalogEntry::EdgeCatalogEntry(Catalog *catalog, SchemaCatalogEntry *schema, BoundCreateEdgeInfo *info,
                                   EdgeDirection edge_direction)
    : StandardEntry(CatalogType::EDGE, schema, catalog, info->name), edge_direction(edge_direction),
      edge_table(info->edge_table_entry), vertices(move(info->vertex_entries)),
      vertex_primary_column_ids(info->vertex_primary_column_ids), reference_column_ids(info->reference_column_ids),
      reference_column_names(info->reference_column_names), rai_index(nullptr) {
}

void EdgeCatalogEntry::Serialize(Serializer &serializer) {
	serializer.WriteString(name);
	serializer.Write(edge_direction);
	serializer.WriteString(edge_table->schema->name);
	serializer.WriteString(edge_table->name);
	serializer.Write<idx_t>(vertices.size());
	for (auto &vertex : vertices) {
		serializer.WriteString(vertex->name);
	}
	serializer.Write<idx_t>(reference_column_names.size());
	for (auto &id : reference_column_names) {
		serializer.WriteString(id);
	}
}

unique_ptr<CreateInfo> EdgeCatalogEntry::Deserialize(Deserializer &source) {
	auto info = make_unique<CreateEdgeInfo>();
	info->name = source.Read<string>();
	info->edge_direction = source.Read<EdgeDirection>();
	auto edge_table_ref = make_unique<BaseTableRef>();
	edge_table_ref->schema_name = source.Read<string>();
	edge_table_ref->table_name = source.Read<string>();
	string edge_table_name = edge_table_ref->table_name;
	info->edge_table = move(edge_table_ref);
	auto num_vertex_tables = source.Read<idx_t>();
	vector<string> vertex_table_names;
	for (auto i = 0u; i < num_vertex_tables; i++) {
		auto vertex_ref = make_unique<VertexRef>();
		vertex_ref->vertex_label = source.Read<string>();
		vertex_table_names.push_back(vertex_ref->base_table_name);
		info->vertices.push_back(move(vertex_ref));
	}
	auto num_reference_columns = source.Read<idx_t>();
	assert(num_vertex_tables == num_reference_columns);
	for (auto i = 0u; i < num_reference_columns; i++) {
		auto reference_column = make_unique<ColumnRefExpression>(source.Read<string>(), edge_table_name);
		info->reference_columns.push_back(move(reference_column));
	}
	return info;
}
