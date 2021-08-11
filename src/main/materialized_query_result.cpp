#include "duckdb/main/materialized_query_result.hpp"

using namespace duckdb;
using namespace std;

MaterializedQueryResult::MaterializedQueryResult(StatementType statement_type)
    : QueryResult(QueryResultType::MATERIALIZED_RESULT, statement_type) {
}

MaterializedQueryResult::MaterializedQueryResult(StatementType statement_type, vector<SQLType> sql_types,
                                                 vector<TypeId> types, vector<string> names)
    : QueryResult(QueryResultType::MATERIALIZED_RESULT, statement_type, sql_types, types, names) {
}

MaterializedQueryResult::MaterializedQueryResult(string error)
    : QueryResult(QueryResultType::MATERIALIZED_RESULT, error) {
}

Value MaterializedQueryResult::GetValue(idx_t column, idx_t index) {
	auto &data = collection.GetChunk(index).data[column];
	auto offset_in_chunk = index % STANDARD_VECTOR_SIZE;
	return data.GetValue(offset_in_chunk);
}

string MaterializedQueryResult::ToString() {
	string result;
	if (success) {
		result = HeaderToString();
		result += "[ Rows: " + to_string(collection.count) + "]\n";
		for (idx_t j = 0; j < collection.count; j++) {
			auto column_count = collection.column_count();
			for (idx_t i = 0; i < column_count - 1; i++) {
				auto val = collection.GetValue(i, j);
				result += val.is_null ? "NULL" : val.ToString(sql_types[i]);
				result += ",";
			}
			auto val = collection.GetValue(column_count - 1, j);
			result += val.is_null ? "NULL" : val.ToString(sql_types[column_count - 1]);
			result += "\n";
		}
	} else {
		result = "Query Error: " + error + "\n";
	}
//	result += "Cost: " + to_string(time) + "\n";
	return result;
}

string MaterializedQueryResult::ToStringAggr() {
	string result;
	if (success) {
		result = HeaderToString();
		result += "[ Rows: " + to_string(collection.count) + "]\n";
		auto rows_count = collection.count;
		if (rows_count > 1) {
			result += "[1st row]";
			rows_count = 1;
		}
		for (idx_t j = 0; j < rows_count; j++) {
			auto column_count = collection.column_count();
			for (idx_t i = 0; i < column_count - 1; i++) {
				auto val = collection.GetValue(i, j);
				result += val.is_null ? "NULL" : val.ToString(sql_types[i]);
				result += ",";
			}
			auto val = collection.GetValue(column_count - 1, j);
			result += val.is_null ? "NULL" : val.ToString(sql_types[column_count - 1]);
			result += "\n";
		}
	} else {
		result = "Query Error: " + error + "\n";
	}
	//	result += "Cost: " + to_string(time) + "\n";
	return result;
}

unique_ptr<DataChunk> MaterializedQueryResult::Fetch() {
	if (!success) {
		return nullptr;
	}
	if (collection.chunks.size() == 0) {
		return make_unique<DataChunk>();
	}
	auto chunk = move(collection.chunks[0]);
	collection.chunks.erase(collection.chunks.begin() + 0);
	return chunk;
}
