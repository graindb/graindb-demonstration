#include "duckdb/execution/operator/scan/physical_dummy_scan.hpp"

using namespace duckdb;
using namespace std;

void PhysicalDummyScan::GetChunkInternal(ClientContext &context, DataChunk &chunk, PhysicalOperatorState *state,
                                         SelectionVector *sel, Vector *rid_vector, DataChunk *rai_chunk) {
	state->finished = true;
	// return a single row on the first call to the dummy scan
	chunk.SetCardinality(1);
}
