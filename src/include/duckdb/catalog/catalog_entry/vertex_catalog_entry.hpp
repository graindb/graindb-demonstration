#pragma once

#include "duckdb/catalog/standard_entry.hpp"
#include "duckdb/parser/parsed_data/create_vertex_info.hpp"

namespace duckdb {

class VertexCatalogEntry : public StandardEntry {

public:
	VertexCatalogEntry(Catalog *catalog, SchemaCatalogEntry *schema, BoundCreateVertexInfo *info);

public:
	TableCatalogEntry *base_table;

public:
	void Serialize(Serializer &serializer);
	static unique_ptr<CreateInfo> Deserialize(Deserializer &source);
};

} // namespace duckdb
