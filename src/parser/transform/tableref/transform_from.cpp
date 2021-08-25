#include "duckdb/parser/tableref/crossproductref.hpp"
#include "duckdb/parser/tableref/emptytableref.hpp"
#include "duckdb/parser/transformer.hpp"

using namespace duckdb;
using namespace std;

unique_ptr<TableRef> Transformer::TransformFrom(duckdb_libpgquery::PGList *root) {
	if (!root) {
		return make_unique<EmptyTableRef>();
	}

	vector<unique_ptr<TableRef>> table_refs;
	for (auto node = root->head; node != nullptr; node = node->next) {
		auto n = reinterpret_cast<duckdb_libpgquery::PGNode *>(node->data.ptr_value);
		unique_ptr<TableRef> next = TransformTableRefNode(n);
		if (next) {
			// path pattern returns nullptr, otherwise it is non-nullptr
			table_refs.push_back(move(next));
		}
	}
	auto path_joins = query_graph->GeneratePathJoins();
	for (auto &e : path_joins) {
		table_refs.push_back(move(e));
	}
	assert(!table_refs.empty());
	if (table_refs.size() > 1) {
		// Cross Product
		auto result = make_unique<CrossProductRef>();
		CrossProductRef *cur_root = result.get();
		for (auto &node : table_refs) {
			if (!cur_root->right) {
				cur_root->right = move(node);
			} else if (!cur_root->left) {
				cur_root->left = move(node);
			} else {
				auto old_res = move(result);
				result = make_unique<CrossProductRef>();
				result->right = move(old_res);
				result->left = move(node);
				cur_root = result.get();
			}
		}
		return move(result);
	}
	return move(table_refs[0]);
}
