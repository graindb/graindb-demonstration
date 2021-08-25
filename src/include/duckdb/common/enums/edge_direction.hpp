#pragma once

#include "duckdb/common/constants.hpp"

namespace duckdb {

enum class EdgeDirection : uint8_t {
	INVALID = 0,
	DIRECTED = 1,
	UNDIRECTED = 2,
	SELF = 3,
	//! direction. used in path pattern matching and alists
	FORWARD = 4,
	BACKWARD = 5
};

const string EdgeDirectionNames[] = {"INVALID", "DIRECTED", "UNDIRECTED", "SELF", "FORWARD", "BACKWARD"};
} // namespace duckdb
