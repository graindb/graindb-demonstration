#include "duckdb/execution/operator/schema/physical_create_edge.hpp"

#include "duckdb/catalog/catalog_entry/edge_catalog_entry.hpp"
#include "duckdb/execution/index/rai/rel_adj_index.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/storage/storage_lock.hpp"

using namespace duckdb;
using namespace std;

void PhysicalCreateEdge::GetChunkInternal(ClientContext &context, DataChunk &chunk, PhysicalOperatorState *state,
                                          SelectionVector *sel, Vector *rid_vector, DataChunk *rai_chunk) {
	assert(children.size() == 1);
	// Create edge catalog entry
	auto edge_entry = reinterpret_cast<EdgeCatalogEntry *>(info->schema->CreateEdge(context, info.get()));
	edge_entry->edge_table->correlated_edges.push_back(edge_entry);
	auto edge_table_name = edge_entry->edge_table->name;

	// Alter edge table to append join index columns
	vector<idx_t> join_index_column_ids(info->reference_column_ids.size());
	auto isAdded = edge_entry->edge_table->AddJoinIndexColumns(info->reference_column_ids, join_index_column_ids);

	// Build join column index
	idx_t count = 0;
	vector<unique_ptr<ColumnAppendState>> append_states(join_index_column_ids.size());
	// Initialize append states
	for (auto i = 0u; i < join_index_column_ids.size(); i++) {
		if (!isAdded[i]) {
			continue;
		}
		append_states[i] = make_unique<ColumnAppendState>();
		edge_entry->edge_table->storage->GetColumn(join_index_column_ids[i])->InitializeAppend(*append_states[i]);
	}
	// Perform appending
	while (true) {
		children[0]->GetChunk(context, state->child_chunk, state->child_state.get());
		idx_t chunk_count = state->child_chunk.size();
		if (chunk_count == 0) {
			break;
		}
		for (auto i = 0u; i < join_index_column_ids.size(); i++) {
			// Join index column already exists. Skip appending it.
			if (!isAdded[i]) {
				continue;
			}
			edge_entry->edge_table->storage->GetColumn(join_index_column_ids[i])
			    ->Append(*append_states[i], state->child_chunk.data[i], chunk_count);
		}
		count += chunk_count;
	}
	// Release locke
	for (auto &append_state : append_states) {
		append_state.reset();
	}

	// Build rai index
	vector<unique_ptr<Expression>> unbound_expressions(join_index_column_ids.size());
	vector<unique_ptr<Expression>> index_expressions(join_index_column_ids.size());
	for (auto i = 0u; i < join_index_column_ids.size(); i++) {
		auto column_ref_expression = make_unique<BoundReferenceExpression>(TypeId::INT64, i);
		unbound_expressions[i] = column_ref_expression->Copy();
		index_expressions[i] = move(column_ref_expression);
	}
	auto rai_index = make_unique<RelAdjIndex>(edge_entry->name, join_index_column_ids, move(unbound_expressions),
	                                          edge_entry->edge_direction, true /* keep edges */);
	edge_entry->rai_index = rai_index.get();
	edge_entry->edge_table->storage->AddIndex(move(rai_index), index_expressions);

	// Return num of tuples appended as the result
	chunk.SetCardinality(1);
	chunk.SetValue(0, 0, Value::BIGINT(count));
	state->finished = true;
}
