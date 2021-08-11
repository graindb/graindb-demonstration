//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/operator/helper/physical_prepare.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/enums/physical_operator_type.hpp"
#include "duckdb/execution/physical_operator.hpp"
#include "duckdb/main/prepared_statement_data.hpp"

namespace duckdb {

class PhysicalPrepare : public PhysicalOperator {
public:
	PhysicalPrepare(string name, unique_ptr<PreparedStatementData> prepared)
	    : PhysicalOperator(PhysicalOperatorType::PREPARE, {TypeId::BOOL}), name(name), prepared(move(prepared)) {
	}

	string name;
	unique_ptr<PreparedStatementData> prepared;

public:
	void GetChunkInternal(ClientContext &context, DataChunk &chunk, PhysicalOperatorState *state_,
	                      SelectionVector *sel = nullptr, Vector *rid_vector = nullptr,
	                      DataChunk *rai_chunk = nullptr) override;
};

} // namespace duckdb
