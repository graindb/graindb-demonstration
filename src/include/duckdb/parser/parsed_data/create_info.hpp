//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/parser/parsed_data/create_info.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/enums/catalog_type.hpp"
#include "duckdb/parser/parsed_data/parse_info.hpp"

namespace duckdb {

enum class OnCreateConflict : uint8_t {
	// Standard: throw error
	ERROR,
	// CREATE IF NOT EXISTS, silently do nothing on conflict
	IGNORE,
	// CREATE OR REPLACE
	REPLACE
};

struct CreateInfo : public ParseInfo {
	explicit CreateInfo(CatalogType type, string schema = DEFAULT_SCHEMA)
	    : type(type), schema(move(schema)), on_conflict(OnCreateConflict::ERROR), temporary(false) {
	}
	~CreateInfo() override = default;

	//! The to-be-created catalog type
	CatalogType type;
	//! The schema name of the entry
	string schema;
	//! What to do on create conflict
	OnCreateConflict on_conflict;
	//! Whether or not the entry is temporary
	bool temporary;
};

} // namespace duckdb
