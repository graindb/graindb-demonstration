#include "duckdb/catalog/catalog_entry/vertex_catalog_entry.hpp"

#include "duckdb/planner/parsed_data/bound_create_vertex_info.hpp"

using namespace std;
using namespace duckdb;

VertexCatalogEntry::VertexCatalogEntry(Catalog *catalog, SchemaCatalogEntry *schema, BoundCreateVertexInfo *info)
    : StandardEntry(CatalogType::VERTEX, schema, catalog, info->name), base_table(info->base_table_entry) {
}

void VertexCatalogEntry::Serialize(Serializer &serializer) {
	serializer.WriteString(name);
	serializer.WriteString(base_table->schema->name);
	serializer.WriteString(base_table->name);
}

unique_ptr<CreateInfo> VertexCatalogEntry::Deserialize(Deserializer &source) {
	auto info = make_unique<CreateVertexInfo>();
	info->name = source.Read<string>();
	info->base_table = make_unique<BaseTableRef>();
	info->base_table->schema_name = source.Read<string>();
	info->base_table->table_name = source.Read<string>();

	return info;
}
