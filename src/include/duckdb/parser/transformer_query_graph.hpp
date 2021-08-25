#pragma once

#include <unordered_map>
#include <unordered_set>

using namespace std;
namespace duckdb {

class TransformerQueryGraph {

public:
	TransformerQueryGraph() = default;

	bool AddEdge(unique_ptr<EdgeRef> edge);
	void AddVertex(unique_ptr<VertexRef> vertex);

	vector<unique_ptr<PathJoinRef>> GeneratePathJoins();

private:
	unique_ptr<PathJoinRef> TraverseConnectedSubgraph(idx_t vertex_id, unordered_set<string> visited_edges);

	vector<string> vertex_variables;
	vector<unique_ptr<VertexRef>> vertex_refs;
	unordered_map<string, idx_t> vertex_pos_map;
	unordered_map<string, vector<unique_ptr<EdgeRef>>> edges;

	vector<unique_ptr<PathJoinRef>> sub_plans;
};
} // namespace duckdb
