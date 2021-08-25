#pragma once

#include "duckdb/catalog/standard_entry.hpp"
#include "duckdb/execution/index/rai/rel_adj_index.hpp"

namespace duckdb {

struct CreateEdgeInfo;
struct BoundCreateEdgeInfo;
class VertexCatalogEntry;

class EdgeCatalogEntry : public StandardEntry {
public:
	EdgeCatalogEntry(Catalog *catalog, SchemaCatalogEntry *schema, BoundCreateEdgeInfo *info,
	                 EdgeDirection edge_direction);

public:
	//! Edge direction
	EdgeDirection edge_direction;
	//! The edge and vertex table catalog entries
	TableCatalogEntry *edge_table;
	//! The source and destination vertex
	vector<VertexCatalogEntry *> vertices;
	//! The primary key columns of vertex_tables
	vector<column_t> vertex_primary_column_ids;
	//! The primary/foreign column in the edge table referencing primary key of vertex_tables
	vector<column_t> reference_column_ids;
	vector<string> reference_column_names;
	//! Relational adjacency index
	RelAdjIndex *rai_index;

public:
	void Serialize(Serializer &serializer);
	static unique_ptr<CreateInfo> Deserialize(Deserializer &source);
};
} // namespace duckdb
