#pragma once

#include "duckdb/common/enums/edge_direction.hpp"
#include "duckdb/common/enums/join_type.hpp"
#include "duckdb/common/unordered_set.hpp"
#include "duckdb/parser/parsed_expression.hpp"
#include "duckdb/parser/tableref.hpp"
#include "duckdb/parser/tableref/edgeref.hpp"

namespace duckdb {

struct PathElement {

public:
	PathElement() : direction(EdgeDirection::FORWARD), length_low(1), length_high(1) {
	}

	EdgeDirection direction;
	string rel_alias;
	string rel_label;
	int length_low;
	int length_high;
	unique_ptr<VertexRef> vertex;
	unique_ptr<EdgeRef> edge;
};

} // namespace duckdb
