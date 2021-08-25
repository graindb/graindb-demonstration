#pragma once

#ifdef _MSC_VER
// these break enum.hpp otherwise
#undef DELETE
#undef DEFAULT
#undef EXISTS
#undef IN
// this breaks file_system.cpp otherwise
#undef CreateDirectory
#undef RemoveDirectory
#endif

#include "compare_result.hpp"
#include "duckdb.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/types.hpp"

namespace duckdb {

void DeleteDatabase(const string &path);
void TestDeleteDirectory(const string &path);
void TestCreateDirectory(const string &path);
void TestDeleteFile(const string &path);
string TestCreatePath(const string &suffix);
unique_ptr<DBConfig> GetTestConfig();

string GetCSVPath();
void WriteCSV(const string &path, const char *csv);
void WriteBinary(const string &path, const uint8_t *data, uint64_t length);

bool NO_FAIL(QueryResult &result);
bool NO_FAIL(unique_ptr<QueryResult> result);

#define REQUIRE_NO_FAIL(result) REQUIRE(NO_FAIL((result)))
#define REQUIRE_FAIL(result) REQUIRE(!(result)->success)

#define COMPARE_CSV(result, csv, header)                                                                               \
	{                                                                                                                  \
		auto res = compare_csv(*result, csv, header);                                                                  \
		if (!res.empty())                                                                                              \
			FAIL(res);                                                                                                 \
	}

} // namespace duckdb
