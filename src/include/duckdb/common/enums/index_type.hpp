//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/enums/index_type.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/constants.hpp"

namespace duckdb {

//===--------------------------------------------------------------------===//
// Index Types
//===--------------------------------------------------------------------===//
enum class IndexType {
	INVALID = 0, // invalid index type
	ART = 1,     // Adaptive Radix Tree
	RAI = 2,     // Relational Adjacency Index (one per EDGE)
};

} // namespace duckdb
