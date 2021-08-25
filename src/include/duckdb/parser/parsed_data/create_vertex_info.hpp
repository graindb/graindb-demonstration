#pragma once

#include "duckdb/parser/parsed_data/create_info.hpp"
#include "duckdb/parser/tableref/basetableref.hpp"

namespace duckdb {

struct CreateVertexInfo : public CreateInfo {

public:
	CreateVertexInfo() : CreateInfo(CatalogType::VERTEX) {
	}
	CreateVertexInfo(string name, unique_ptr<BaseTableRef> base_table)
	    : CreateInfo(CatalogType::VERTEX), name(move(name)), base_table(move(base_table)) {
	}

	string name;
	unique_ptr<BaseTableRef> base_table;
};
} // namespace duckdb
