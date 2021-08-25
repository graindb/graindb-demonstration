#include "duckdb/execution/operator/helper/physical_vacuum.hpp"

#include "duckdb/transaction/transaction.hpp"

using namespace duckdb;
using namespace std;

void PhysicalVacuum::GetChunkInternal(ClientContext &context, DataChunk &chunk, PhysicalOperatorState *state,
                                      SelectionVector *sel, Vector *rid_vector, DataChunk *rai_chunk) {
	// NOP
	state->finished = true;
}
