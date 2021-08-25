#include "duckdb/execution/operator/schema/physical_create_vertex.hpp"

using namespace std;
using namespace duckdb;

void PhysicalCreateVertex::GetChunkInternal(ClientContext &context, DataChunk &chunk, PhysicalOperatorState *state,
                                            SelectionVector *sel, Vector *rid_vector, DataChunk *rai_chunk) {
	assert(children.empty());
	// Create vertex catalog entry
	auto vertex_entry = info->schema->CreateVertex(context, info.get());
	auto result = 0;
	if (vertex_entry != nullptr) {
		result = 1;
	}
	chunk.SetCardinality(1);
	chunk.SetValue(0, 0, result);
	state->finished = true;
}
