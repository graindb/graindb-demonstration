#include "duckdb/common/enums/edge_direction.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/parser/query_node/select_node.hpp"
#include "duckdb/parser/tableref.hpp"
#include "duckdb/parser/tableref/edgeref.hpp"
#include "duckdb/parser/tableref/vertexref.hpp"
#include "duckdb/parser/transformer.hpp"

using namespace duckdb;
using namespace std;

VertexRef *Transformer::ConstructVertex(duckdb_libpgquery::PGNodePattern *node) {
	auto vertex = make_unique<VertexRef>();
	if (node->label) {
		vertex->vertex_label = node->label;
		if (!node->alias) {
			// anonymous variable
			vertex->alias = "$ANON_" + to_string(anonymous_variable_idx);
			anonymous_variable_idx++;
		} else {
			vertex->alias = node->alias;
		}
	} else {
		if (!node->alias) {
			throw NotImplementedException("Vertex without variable name and label is not allowed!");
		}
		vertex->alias = node->alias;
	}
	auto result = vertex.get();
	query_graph->AddVertex(move(vertex));
	return result;
}

unique_ptr<TableRef> Transformer::TransformPathPattern(duckdb_libpgquery::PGPathPattern *path_pattern) {
	auto node = reinterpret_cast<duckdb_libpgquery::PGNodePattern *>(path_pattern->node);
	auto start_vertex = ConstructVertex(node);

	if (path_pattern->elements == nullptr) {
		return start_vertex->Copy();
	}
	auto start_vertex_alias = start_vertex->alias;
	for (auto elem = path_pattern->elements->head; elem != nullptr; elem = elem->next) {
		auto pg_elem = reinterpret_cast<duckdb_libpgquery::PGNode *>(elem->data.ptr_value);
		assert(pg_elem->type == duckdb_libpgquery::T_PGPatternElement);
		auto path_elem = reinterpret_cast<duckdb_libpgquery::PGPatternElement *>(pg_elem);
		auto edge_in_path = make_unique<EdgeRef>();
		if (path_elem->rel_alias) {
			edge_in_path->alias = path_elem->rel_alias;
			edge_in_path->edge_name = path_elem->rel_label;
		} else {
			throw NotImplementedException("Edge without alias not supported yet.");
		}
		auto vertex_node = reinterpret_cast<duckdb_libpgquery::PGNodePattern *>(path_elem->node_pattern);
		auto vertex_in_path = ConstructVertex(vertex_node);

		// Normalize edge to FORWARD
		edge_in_path->src_vertex_alias =
		    path_elem->direction == duckdb_libpgquery::FORWARD_DIRECTION ? start_vertex_alias : vertex_in_path->alias;
		edge_in_path->dst_vertex_alias =
		    path_elem->direction == duckdb_libpgquery::FORWARD_DIRECTION ? vertex_in_path->alias : start_vertex_alias;
		edge_in_path->direction = EdgeDirection::FORWARD;
		if (path_elem->rel_range) {
			auto rel_range = reinterpret_cast<duckdb_libpgquery::PGRelRange *>(path_elem->rel_range);
			edge_in_path->length_low = rel_range->length_low->val.ival;
			edge_in_path->length_high = rel_range->length_high->val.ival;
		}
		query_graph->AddEdge(move(edge_in_path));

		start_vertex_alias = vertex_in_path->alias;
	}
	return nullptr;
}
