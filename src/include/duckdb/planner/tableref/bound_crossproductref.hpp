//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/planner/tableref/bound_crossproductref.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/planner/tableref/bound_joinref.hpp"

namespace duckdb {

//! Represents a cross product
class BoundCrossProductRef : public BoundJoinRef {
public:
	BoundCrossProductRef() : BoundJoinRef(TableReferenceType::CROSS_PRODUCT) {
	}
};
} // namespace duckdb
