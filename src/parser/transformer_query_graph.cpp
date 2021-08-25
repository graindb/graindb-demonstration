#include "duckdb/parser/transformer_query_graph.hpp"

#include "duckdb/parser/tableref/path_joinref.hpp"

using namespace duckdb;
using namespace std;

void TransformerQueryGraph::AddVertex(unique_ptr<VertexRef> vertex) {
	if (vertex_pos_map.find(vertex->alias) == vertex_pos_map.end()) {
		auto vertex_id = vertex_variables.size();
		auto vertex_alias = vertex->alias;
		vertex_variables.push_back(vertex_alias);
		vertex_refs.push_back(move(vertex));
		assert(vertex_variables.size() == vertex_refs.size());
		vertex_pos_map.insert({vertex_alias, vertex_id});
	}
}

bool TransformerQueryGraph::AddEdge(unique_ptr<EdgeRef> edge) {
	if (vertex_pos_map.find(edge->src_vertex_alias) == vertex_pos_map.end() ||
	    vertex_pos_map.find(edge->dst_vertex_alias) == vertex_pos_map.end()) {
		return false;
	}
	// Append edges
	auto reverted_edge = edge->Revert();
	if (edges.find(edge->src_vertex_alias) == edges.end()) {
		vector<unique_ptr<EdgeRef>> path;
		edges.insert({edge->src_vertex_alias, move(path)});
	}
	edges.at(edge->src_vertex_alias).push_back(move(edge));
	if (edges.find(reverted_edge->src_vertex_alias) == edges.end()) {
		vector<unique_ptr<EdgeRef>> path;
		edges.insert({reverted_edge->src_vertex_alias, move(path)});
	}
	edges.at(reverted_edge->src_vertex_alias).push_back(move(reverted_edge));
	return true;
}

unique_ptr<PathJoinRef> TransformerQueryGraph::TraverseConnectedSubgraph(idx_t vertex_id,
                                                                         unordered_set<string> visited_edges) {
	vector<EdgeRef *> frontier_edges;
	string vertex_variable = vertex_variables[vertex_id];
	vertex_variables[vertex_id] = ""; // set to empty
	if (edges.find(vertex_variable) == edges.end()) {
		return nullptr;
	}
	auto &matched_edges = edges.at(vertex_variable);
	for (auto &edge : matched_edges) {
		auto edge_alias = edge->alias;
		if (visited_edges.find(edge_alias) != visited_edges.end()) {
			// edge visited before
			continue;
		}
		frontier_edges.push_back(edge.get());
	}
	unique_ptr<PathJoinRef> current_root = nullptr;
	for (auto edge : frontier_edges) {
		auto join = make_unique<PathJoinRef>();
		if (current_root == nullptr) {
			join->source_vertex = vertex_refs[vertex_id]->Copy();
		} else {
			join->source_vertex = move(current_root);
		}
		visited_edges.insert(edge->alias);
		auto vertex = TraverseConnectedSubgraph(vertex_pos_map.at(edge->dst_vertex_alias), visited_edges);
		if (vertex) {
			join->destination_vertex = move(vertex);
		} else {
			join->destination_vertex = vertex_refs[vertex_pos_map.at(edge->dst_vertex_alias)]->Copy();
		}
		join->edge = unique_ptr_cast<TableRef, EdgeRef>(edge->Copy());
		join->alias = join->destination_vertex->alias;
		current_root = move(join);
	}
	return current_root;
}

vector<unique_ptr<PathJoinRef>> TransformerQueryGraph::GeneratePathJoins() {
	if (vertex_variables.empty() || edges.empty()) {
		vector<unique_ptr<PathJoinRef>> empty_result;
		return empty_result;
	}
	unordered_set<string> visited_edges;
	idx_t current_vertex_id = 0;
	while (true) {
		if (current_vertex_id == UINT64_MAX) {
			break;
		}
		auto sub_plan = TraverseConnectedSubgraph(current_vertex_id, visited_edges);
		assert(sub_plan);
		sub_plans.push_back(move(sub_plan));

		current_vertex_id = UINT64_MAX;
		for (auto i = 0u; i < vertex_variables.size(); i++) {
			if (!vertex_variables[i].empty()) {
				current_vertex_id = i;
			}
		}
	}

	return move(sub_plans);
}
