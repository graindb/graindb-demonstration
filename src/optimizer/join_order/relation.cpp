#include "duckdb/optimizer/join_order/join_relation.hpp"
#include "duckdb/common/string_util.hpp"

#include <algorithm>
#include <string>

using namespace duckdb;
using namespace std;

using JoinRelationTreeNode = JoinRelationSetManager::JoinRelationTreeNode;

string JoinRelationSet::ToString() const {
	string result = "[";
	result += StringUtil::Join(relations, count, ", ", [](const idx_t &relation) { return to_string(relation); });
	result += "]";
	return result;
}

//! Returns true if sub is a subset of super
bool JoinRelationSet::IsSubset(JoinRelationSet *super, JoinRelationSet *sub) {
	if (sub->count == 0) {
		return false;
	}
	if (sub->count > super->count) {
		return false;
	}
	idx_t j = 0;
	for (idx_t i = 0; i < super->count; i++) {
		if (sub->relations[j] == super->relations[i]) {
			j++;
			if (j == sub->count) {
				return true;
			}
		}
	}
	return false;
}

JoinRelationSet *JoinRelationSetManager::GetJoinRelation(unique_ptr<idx_t[]> relations, idx_t count) {
	// now look it up in the tree
	JoinRelationTreeNode *info = &root;
	for (idx_t i = 0; i < count; i++) {
		auto entry = info->children.find(relations[i]);
		if (entry == info->children.end()) {
			// node not found, create it
			auto insert_it = info->children.insert(make_pair(relations[i], make_unique<JoinRelationTreeNode>()));
			entry = insert_it.first;
		}
		// move to the next node
		info = entry->second.get();
	}
	// now check if the JoinRelationSet has already been created
	if (!info->relation) {
		// if it hasn't we need to create it
		sort(relations.get(), relations.get() + count);
		info->relation = make_unique<JoinRelationSet>(move(relations), count);
	}
	return info->relation.get();
}

//! Create or get a JoinRelationSet from a single node with the given index
JoinRelationSet *JoinRelationSetManager::GetJoinRelation(idx_t index) {
	// create a sorted vector of the relations
	auto relations = unique_ptr<idx_t[]>(new idx_t[1]);
	relations[0] = index;
	idx_t count = 1;
	return GetJoinRelation(move(relations), count);
}

JoinRelationSet *JoinRelationSetManager::GetJoinRelation(unordered_set<idx_t> &bindings) {
	// create a sorted vector of the relations
	unique_ptr<idx_t[]> relations = bindings.size() == 0 ? nullptr : unique_ptr<idx_t[]>(new idx_t[bindings.size()]);
	idx_t count = 0;
	for (auto &entry : bindings) {
		relations[count++] = entry;
	}
	sort(relations.get(), relations.get() + count);
	return GetJoinRelation(move(relations), count);
}

JoinRelationSet *JoinRelationSetManager::Union(JoinRelationSet *left, JoinRelationSet *right) {
	auto relations = unique_ptr<idx_t[]>(new idx_t[left->count + right->count]);
	idx_t count = 0;
	// move through the left and right relations, eliminating duplicates
	idx_t i = 0, j = 0;
	while (true) {
		if (i == left->count) {
			// exhausted left relation, add remaining of right relation
			for (; j < right->count; j++) {
				relations[count++] = right->relations[j];
			}
			break;
		} else if (j == right->count) {
			// exhausted right relation, add remaining of left
			for (; i < left->count; i++) {
				relations[count++] = left->relations[i];
			}
			break;
		} else if (left->relations[i] == right->relations[j]) {
			// equivalent, add only one of the two pairs
			relations[count++] = left->relations[i];
			i++;
			j++;
		} else if (left->relations[i] < right->relations[j]) {
			// left is smaller, progress left and add it to the set
			relations[count++] = left->relations[i];
			i++;
		} else {
			// right is smaller, progress right and add it to the set
			relations[count++] = right->relations[j];
			j++;
		}
	}
	return GetJoinRelation(move(relations), count);
}

JoinRelationSet *JoinRelationSetManager::Difference(JoinRelationSet *left, JoinRelationSet *right) {
	auto relations = unique_ptr<idx_t[]>(new idx_t[left->count]);
	idx_t count = 0;
	// move through the left and right relations
	idx_t i = 0, j = 0;
	while (true) {
		if (i == left->count) {
			// exhausted left relation, we are done
			break;
		} else if (j == right->count) {
			// exhausted right relation, add remaining of left
			for (; i < left->count; i++) {
				relations[count++] = left->relations[i];
			}
			break;
		} else if (left->relations[i] == right->relations[j]) {
			// equivalent, add nothing
			i++;
			j++;
		} else if (left->relations[i] < right->relations[j]) {
			// left is smaller, progress left and add it to the set
			relations[count++] = left->relations[i];
			i++;
		} else {
			// right is smaller, progress right
			j++;
		}
	}
	return GetJoinRelation(move(relations), count);
}
