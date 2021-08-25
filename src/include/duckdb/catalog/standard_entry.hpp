//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/catalog/standard_entry.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/catalog/catalog_entry.hpp"

#include <utility>

namespace duckdb {
class SchemaCatalogEntry;

//! A StandardEntry is a catalog entry that is a member of a schema
class StandardEntry : public CatalogEntry {
public:
	StandardEntry(CatalogType type, SchemaCatalogEntry *schema, Catalog *catalog, string name)
	    : CatalogEntry(type, catalog, move(name)), schema(schema) {
	}
	~StandardEntry() override = default;

	//! The schema the entry belongs to
	SchemaCatalogEntry *schema;
};
} // namespace duckdb
