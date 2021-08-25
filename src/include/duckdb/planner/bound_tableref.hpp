//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/planner/bound_tableref.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/common/enums/tableref_type.hpp"

namespace duckdb {

class BoundTableRef {
public:
	explicit BoundTableRef(TableReferenceType type) : type(type) {
	}
	virtual ~BoundTableRef() = default;

	//! The type of table reference
	TableReferenceType type;
};
} // namespace duckdb
