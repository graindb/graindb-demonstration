#include "duckdb/optimizer/join_order_optimizer.hpp"

#include "duckdb/main/client_context.hpp"
#include "duckdb/planner/expression/list.hpp"
#include "duckdb/planner/expression_iterator.hpp"
#include "duckdb/planner/operator/list.hpp"

#include <iostream>

using namespace duckdb;
using namespace std;

using json = nlohmann::json;
using JoinNode = JoinOrderOptimizer::JoinNode;

//! Returns true if A and B are disjoint, false otherwise
template <class T> static bool Disjoint(unordered_set<T> &a, unordered_set<T> &b) {
	for (auto &entry : a) {
		if (b.find(entry) != b.end()) {
			return false;
		}
	}
	return true;
}

//! Extract the set of relations referred to inside an expression
bool JoinOrderOptimizer::ExtractBindings(Expression &expression, unordered_set<idx_t> &bindings) {
	if (expression.type == ExpressionType::BOUND_COLUMN_REF) {
		auto &colref = (BoundColumnRefExpression &)expression;
		assert(colref.depth == 0);
		assert(colref.binding.table_index != INVALID_INDEX);
		// map the base table index to the relation index used by the JoinOrderOptimizer
		assert(relation_mapping.find(colref.binding.table_index) != relation_mapping.end());
		bindings.insert(relation_mapping[colref.binding.table_index]);
	}
	if (expression.type == ExpressionType::BOUND_REF) {
		// bound expression
		bindings.clear();
		return false;
	}
	assert(expression.type != ExpressionType::SUBQUERY);
	bool can_reorder = true;
	ExpressionIterator::EnumerateChildren(expression, [&](Expression &expr) {
		if (!ExtractBindings(expr, bindings)) {
			can_reorder = false;
			return;
		}
	});
	return can_reorder;
}

static unique_ptr<LogicalOperator> PushFilter(unique_ptr<LogicalOperator> node, unique_ptr<Expression> expr) {
	// push an expression into a filter
	// first check if we have any filter to push it into
	if (node->type != LogicalOperatorType::FILTER) {
		// we don't, we need to create one
		auto filter = make_unique<LogicalFilter>();
		filter->children.push_back(move(node));
		node = move(filter);
	}
	// push the filter into the LogicalFilter
	assert(node->type == LogicalOperatorType::FILTER);
	auto filter = (LogicalFilter *)node.get();
	filter->expressions.push_back(move(expr));
	return node;
}

bool JoinOrderOptimizer::ExtractJoinRelations(LogicalOperator &input_op, vector<LogicalOperator *> &filter_operators,
                                              LogicalOperator *parent) {
	LogicalOperator *op = &input_op;
	while (op->children.size() == 1 &&
	       (op->type != LogicalOperatorType::PROJECTION && op->type != LogicalOperatorType::EXPRESSION_GET)) {
		if (op->type == LogicalOperatorType::FILTER) {
			// extract join conditions from filter
			filter_operators.push_back(op);
		}
		if (op->type == LogicalOperatorType::AGGREGATE_AND_GROUP_BY || op->type == LogicalOperatorType::WINDOW) {
			// don't push filters through projection or aggregate and group by
			JoinOrderOptimizer optimizer(context);
			op->children[0] = optimizer.Optimize(move(op->children[0]));
			return false;
		}
		op = op->children[0].get();
	}
	bool non_reorderable_operation = false;
	if (op->type == LogicalOperatorType::UNION || op->type == LogicalOperatorType::EXCEPT ||
	    op->type == LogicalOperatorType::INTERSECT || op->type == LogicalOperatorType::DELIM_JOIN ||
	    op->type == LogicalOperatorType::ANY_JOIN) {
		// set operation, optimize separately in children
		non_reorderable_operation = true;
	}

	if (op->type == LogicalOperatorType::COMPARISON_JOIN) {
		auto *join = (LogicalJoin *)op;
		if (join->join_type == JoinType::INNER) {
			// extract join conditions from inner join
			filter_operators.push_back(op);
		} else {
			// non-inner join, not reordarable yet
			non_reorderable_operation = true;
		}
	}
	if (non_reorderable_operation) {
		// we encountered a non-reordable operation (setop or non-inner join)
		// we do not reorder non-inner joins yet, however we do want to expand the potential join graph around them
		// non-inner joins are also tricky because we can't freely make conditions through them
		// e.g. suppose we have (left LEFT OUTER JOIN right WHERE right IS NOT NULL), the join can generate
		// new NULL values in the right side, so pushing this condition through the join leads to incorrect results
		// for this reason, we just start a new JoinOptimizer pass in each of the children of the join
		for (idx_t i = 0; i < op->children.size(); i++) {
			JoinOrderOptimizer optimizer(context);
			op->children[i] = optimizer.Optimize(move(op->children[i]));
		}
		// after this we want to treat this node as one  "end node" (like e.g. a base relation)
		// however the join refers to multiple base relations
		// enumerate all base relations obtained from this join and add them to the relation mapping
		// also, we have to resolve the join conditions for the joins here
		// get the left and right bindings
		unordered_set<idx_t> bindings;
		LogicalJoin::GetTableReferences(*op, bindings);
		// now create the relation that refers to all these bindings
		auto relation = make_unique<SingleJoinRelation>(&input_op, parent);
		auto relation_index = relations.size();
		for (idx_t it : bindings) {
			relation_mapping[it] = relation_index;
		}
		relations.push_back(move(relation));
		return true;
	}
	if (op->type == LogicalOperatorType::COMPARISON_JOIN || op->type == LogicalOperatorType::CROSS_PRODUCT) {
		// inner join or cross product
		bool can_reorder_left = ExtractJoinRelations(*op->children[0], filter_operators, op);
		bool can_reorder_right = ExtractJoinRelations(*op->children[1], filter_operators, op);
		return can_reorder_left && can_reorder_right;
	} else if (op->type == LogicalOperatorType::GET) {
		// base table scan, add to set of relations
		auto get = (LogicalGet *)op;
		auto table_oid = get->table == nullptr ? 0 : get->table->oid;
		auto relation = make_unique<SingleJoinRelation>(&input_op, parent, table_oid);
		auto relation_index = relations.size();
		relation_mapping[get->table_index] = relation_index;
		relation_alias_mapping[get->table_alias] = relation_index;
		relations.push_back(move(relation));
		return true;
	} else if (op->type == LogicalOperatorType::EXPRESSION_GET) {
		// base table scan, add to set of relations
		auto get = (LogicalExpressionGet *)op;
		auto relation = make_unique<SingleJoinRelation>(&input_op, parent);
		relation_mapping[get->table_index] = relations.size();
		relations.push_back(move(relation));
		return true;
	} else if (op->type == LogicalOperatorType::TABLE_FUNCTION) {
		// table function call, add to set of relations
		auto table_function = (LogicalTableFunction *)op;
		auto relation = make_unique<SingleJoinRelation>(&input_op, parent);
		relation_mapping[table_function->table_index] = relations.size();
		relations.push_back(move(relation));
		return true;
	} else if (op->type == LogicalOperatorType::PROJECTION) {
		auto proj = (LogicalProjection *)op;
		// we run the join order optimizer witin the subquery as well
		JoinOrderOptimizer optimizer(context);
		op->children[0] = optimizer.Optimize(move(op->children[0]));
		// projection, add to the set of relations
		auto relation = make_unique<SingleJoinRelation>(&input_op, parent);
		relation_mapping[proj->table_index] = relations.size();
		relations.push_back(move(relation));
		return true;
	}
	return false;
}

//! Update the exclusion set with all entries in the subgraph
static void UpdateExclusionSet(JoinRelationSet *node, unordered_set<idx_t> &exclusion_set) {
	for (idx_t i = 0; i < node->count; i++) {
		exclusion_set.insert(node->relations[i]);
	}
}

//! Create a new JoinTree node by joining together two previous JoinTree nodes
unique_ptr<JoinNode> JoinOrderOptimizer::CreateJoinTree(JoinRelationSet *set, NeighborInfo *info, JoinNode *left,
                                                        JoinNode *right) {
	// for the hash join we want the right side (build side) to have the smallest cardinality
	// also just a heuristic but for now...
	// FIXME: we should probably actually benchmark that as well
	// FIXME: should consider different join algorithms, should we pick a join algorithm here as well? (probably)
	if (left->cardinality < right->cardinality) {
		return CreateJoinTree(set, info, right, left);
	}
	idx_t expected_cardinality, expected_cost = 0;
	if (info->filters.size() == 0) {
		// cross product
		expected_cardinality = left->cardinality * right->cardinality;
		expected_cost = expected_cardinality;
	} else {
		// normal join, expect foreign key join
		if (cached_cardinalities.find(set) != cached_cardinalities.end()) {
			expected_cardinality = cached_cardinalities[set];
		} else {
			expected_cardinality = std::max(left->cardinality, right->cardinality);
		}
		expected_cost = std::max(left->cardinality, right->cardinality);
	}
	return make_unique<JoinNode>(set, info, left, right, expected_cardinality, expected_cost);
}

JoinNode *JoinOrderOptimizer::EmitPair(JoinRelationSet *left, JoinRelationSet *right, NeighborInfo *info) {
	// get the left and right join plans
	auto &left_plan = plans[left];
	auto &right_plan = plans[right];
	auto new_set = set_manager.Union(left, right);
	// create the join tree based on combining the two plans
	auto new_plan = CreateJoinTree(new_set, info, left_plan.get(), right_plan.get());
	// check if this plan is the optimal plan we found for this set of relations
	auto entry = plans.find(new_set);
#ifdef LOG_ENUMERATION
	cout << "" + new_set->ToString() + " emits pair: " + left->ToString() + "|" + right->ToString() +
	            ". Card: " + to_string(new_plan->cardinality) + ". Cost: " + to_string(new_plan->cost)
	     << endl;
	if (entry == plans.end() || new_plan->cost < entry->second->cost) {
		string exist_cost_str = entry == plans.end() ? "NULL" : to_string(entry->second->cost);
		cout << "Pair updated into dpTable " + new_set->ToString() << ". Exist cost: " << exist_cost_str << endl;
	} else {
		cout << "Pair not updated into dpTable " + new_set->ToString()
		     << ". Exist cost: " << to_string(entry->second->cost) << endl;
	}
#endif
	if (entry == plans.end() || new_plan->cost < entry->second->cost) {
		// the plan is the optimal plan, move it into the dynamic programming tree
		auto result = new_plan.get();
		plans[new_set] = move(new_plan);
		return result;
	}
	return entry->second.get();
}

bool JoinOrderOptimizer::TryEmitPair(JoinRelationSet *left, JoinRelationSet *right, NeighborInfo *info) {
	pairs++;
	if (pairs >= 2000) {
		// when the amount of pairs gets too large we exit the dynamic programming and resort to a greedy algorithm
		// FIXME: simple heuristic currently
		// at 10K pairs stop searching exactly and switch to heuristic
		return false;
	}
	EmitPair(left, right, info);
	return true;
}

bool JoinOrderOptimizer::EmitCSG(JoinRelationSet *node) {
	// create the exclusion set as everything inside the subgraph AND anything with members BELOW it
	unordered_set<idx_t> exclusion_set;
	for (idx_t i = 0; i < node->relations[0]; i++) {
		exclusion_set.insert(i);
	}
	UpdateExclusionSet(node, exclusion_set);
	// find the neighbors given this exclusion set
	auto neighbors = query_graph.GetNeighbors(node, exclusion_set);
	if (neighbors.size() == 0) {
		return true;
	}
	// we iterate over the neighbors ordered by their first node
	sort(neighbors.begin(), neighbors.end(), greater<idx_t>());
	for (auto neighbor : neighbors) {
		// since the GetNeighbors only returns the smallest element in a list, the entry might not be connected to
		// (only!) this neighbor,  hence we have to do a connectedness check before we can emit it
		auto neighbor_relation = set_manager.GetJoinRelation(neighbor);
		auto connection = query_graph.GetConnection(node, neighbor_relation);
		if (connection) {
			if (!TryEmitPair(node, neighbor_relation, connection)) {
				return false;
			}
		}
		UpdateExclusionSet(neighbor_relation, exclusion_set);
		if (!EnumerateCmpRecursive(node, neighbor_relation, exclusion_set)) {
			return false;
		}
	}
	return true;
}

bool JoinOrderOptimizer::EnumerateCmpRecursive(JoinRelationSet *left, JoinRelationSet *right,
                                               unordered_set<idx_t> exclusion_set) {
	// get the neighbors of the second relation under the exclusion set
	auto neighbors = query_graph.GetNeighbors(right, exclusion_set);
	if (neighbors.size() == 0) {
		return true;
	}
	vector<JoinRelationSet *> union_sets;
	union_sets.resize(neighbors.size());
	for (idx_t i = 0; i < neighbors.size(); i++) {
		auto neighbor = set_manager.GetJoinRelation(neighbors[i]);
		// emit the combinations of this node and its neighbors
		auto combined_set = set_manager.Union(right, neighbor);
		if (plans.find(combined_set) != plans.end()) {
			auto connection = query_graph.GetConnection(left, combined_set);
			if (connection) {
				if (!TryEmitPair(left, combined_set, connection)) {
					return false;
				}
			}
		}
		union_sets[i] = combined_set;
	}
	// recursively enumerate the sets
	for (idx_t i = 0; i < neighbors.size(); i++) {
		// updated the set of excluded entries with this neighbor
		unordered_set<idx_t> new_exclusion_set = exclusion_set;
		new_exclusion_set.insert(neighbors[i]);
		if (!EnumerateCmpRecursive(left, union_sets[i], new_exclusion_set)) {
			return false;
		}
	}
	return true;
}

bool JoinOrderOptimizer::EnumerateCSGRecursive(JoinRelationSet *node, unordered_set<idx_t> &exclusion_set) {
	// find neighbors of S under the exlusion set
	auto neighbors = query_graph.GetNeighbors(node, exclusion_set);
	if (neighbors.size() == 0) {
		return true;
	}
	// now first emit the connected subgraphs of the neighbors
	vector<JoinRelationSet *> union_sets;
	union_sets.resize(neighbors.size());
	for (idx_t i = 0; i < neighbors.size(); i++) {
		auto neighbor = set_manager.GetJoinRelation(neighbors[i]);
		// emit the combinations of this node and its neighbors
		auto new_set = set_manager.Union(node, neighbor);
		if (plans.find(new_set) != plans.end()) {
			if (!EmitCSG(new_set)) {
				return false;
			}
		}
		union_sets[i] = new_set;
	}
	unordered_set<idx_t> new_exclusion_set = exclusion_set;
	for (idx_t i = 0; i < neighbors.size(); i++) {
		new_exclusion_set.insert(neighbors[i]);
	}
	// recursively enumerate the sets
	for (idx_t i = 0; i < neighbors.size(); i++) {
		if (!EnumerateCSGRecursive(union_sets[i], new_exclusion_set)) {
			return false;
		}
	}
	return true;
}

bool JoinOrderOptimizer::SolveJoinOrderExactly() {
	// now we perform the actual dynamic programming to compute the final result
	// we enumerate over all the possible pairs in the neighborhood
	for (idx_t i = relations.size(); i > 0; i--) {
		// for every node in the set, we consider it as the start node once
		auto start_node = set_manager.GetJoinRelation(i - 1);
		// emit the start node
		if (!EmitCSG(start_node)) {
			return false;
		}
		// initialize the set of exclusion_set as all the nodes with a number below this
		unordered_set<idx_t> exclusion_set;
		for (idx_t j = 0; j < i - 1; j++) {
			exclusion_set.insert(j);
		}
		// then we recursively search for neighbors that do not belong to the banned entries
		if (!EnumerateCSGRecursive(start_node, exclusion_set)) {
			return false;
		}
	}
	return true;
}

void JoinOrderOptimizer::SolveJoinOrderApproximately() {
	// at this point, we exited the dynamic programming but did not compute the final join order because it took too
	// long instead, we use a greedy heuristic to obtain a join ordering now we use Greedy Operator Ordering to
	// construct the result tree first we start out with all the base relations (the to-be-joined relations)
	vector<JoinRelationSet *> T;
	for (idx_t i = 0; i < relations.size(); i++) {
		T.push_back(set_manager.GetJoinRelation(i));
	}
	while (T.size() > 1) {
		// now in every step of the algorithm, we greedily pick the join between the to-be-joined relations that has
		// the smallest cost. This is O(r^2) per step, and every step will reduce the total amount of relations
		// to-be-joined by 1, so the total cost is O(r^3) in the amount of relations
		idx_t best_left = 0, best_right = 0;
		JoinNode *best_connection = nullptr;
		for (idx_t i = 0; i < T.size(); i++) {
			auto left = T[i];
			for (idx_t j = i + 1; j < T.size(); j++) {
				auto right = T[j];
				// check if we can connect these two relations
				auto connection = query_graph.GetConnection(left, right);
				if (connection) {
					// we can! check the cost of this connection
					auto node = EmitPair(left, right, connection);
					if (!best_connection || node->cost < best_connection->cost) {
						// best pair found so far
						best_connection = node;
						best_left = i;
						best_right = j;
					}
				}
			}
		}
		if (!best_connection) {
			// could not find a connection, but we were not done with finding a completed plan
			// we have to add a cross product; we add it between the two smallest relations
			JoinNode *smallest_plans[2] = {nullptr};
			idx_t smallest_index[2];
			for (idx_t i = 0; i < T.size(); i++) {
				// get the plan for this relation
				auto current_plan = plans[T[i]].get();
				// check if the cardinality is smaller than the smallest two found so far
				for (idx_t j = 0; j < 2; j++) {
					if (!smallest_plans[j] || smallest_plans[j]->cardinality > current_plan->cardinality) {
						smallest_plans[j] = current_plan;
						smallest_index[j] = i;
						break;
					}
				}
			}
			assert(smallest_plans[0] && smallest_plans[1]);
			assert(smallest_index[0] != smallest_index[1]);
			auto left = smallest_plans[0]->set, right = smallest_plans[1]->set;
			// create a cross product edge (i.e. edge with empty filter) between these two sets in the query graph
			query_graph.CreateEdge(left, right, nullptr, false);
			// now emit the pair and continue with the algorithm
			auto connection = query_graph.GetConnection(left, right);
			assert(connection);

			best_connection = EmitPair(left, right, connection);
			best_left = smallest_index[0];
			best_right = smallest_index[1];
			// the code below assumes best_right > best_left
			if (best_left > best_right) {
				swap(best_left, best_right);
			}
		}
		// now update the to-be-checked pairs
		// remove left and right, and add the combination

		// important to erase the biggest element first
		// if we erase the smallest element first the index of the biggest element changes
		assert(best_right > best_left);
		T.erase(T.begin() + best_right);
		T.erase(T.begin() + best_left);
		T.push_back(best_connection->set);
	}
}

void JoinOrderOptimizer::SolveJoinOrder() {
	// first try to solve the join order exactly
	if (!SolveJoinOrderExactly()) {
		// otherwise, if that times out we resort to a greedy algorithm
		SolveJoinOrderApproximately();
	}
}

JoinRelationSet *JoinOrderOptimizer::InjectJoinNode(unique_ptr<JoinOrderNode> join_order) {
	switch (join_order->op_mark) {
	case OpMark::NLJ:
	case OpMark::HASH_JOIN:
	case OpMark::SIP_JOIN:
	case OpMark::JOIN: {
		auto left_set = InjectJoinNode(move(join_order->children[0]));
		auto right_set = InjectJoinNode(move(join_order->children[1]));
		auto new_set = set_manager.Union(left_set, right_set);
		auto left_node = plans[left_set].get();
		auto right_node = plans[right_set].get();
		auto neighbor_info = query_graph.GetConnection(left_set, right_set);
		auto cardinality = std::max(left_node->cardinality, right_node->cardinality);
		auto node = make_unique<JoinNode>(new_set, neighbor_info, left_node, right_node, cardinality, cardinality,
		                                  join_order->op_mark);
		plans[new_set] = move(node);

		return new_set;
	}
	case OpMark::SCAN:
	case OpMark::NO_OP: {
		auto relation = set_manager.GetJoinRelation(join_order->relation_id);
		plans[relation]->op_mark = join_order->op_mark;
		return relation;
	}
	default: {
		return nullptr; // error! todo: throw exception
	}
	}
}

void JoinOrderOptimizer::InjectCardinality(json &jo) {
	if (jo.empty()) {
		return;
	}
	// inject joins;
	nlohmann::json joins = jo["tables"];
	for (auto it = joins.begin(); it != joins.end(); it++) {
		const string &join_pattern = it.key();
		unordered_set<idx_t> join_set;
		idx_t pos = 0;
		idx_t last_pos = 0;
		while (pos <= string::npos) {
			pos = join_pattern.find(',', last_pos);
			if (pos == string::npos) {
				break;
			}
			string table = join_pattern.substr(last_pos, pos - last_pos);
			if (relation_alias_mapping.find(table) != relation_alias_mapping.end()) {
				join_set.insert(relation_alias_mapping[table]);
			}
			last_pos = pos + 1;
		}
		string table = join_pattern.substr(last_pos, join_pattern.size() - last_pos);
		if (relation_alias_mapping.find(table) != relation_alias_mapping.end()) {
			join_set.insert(relation_alias_mapping[table]);
		}
		auto rel_set = set_manager.GetJoinRelation(join_set);
		cached_cardinalities[rel_set] = it.value();
	}
}

void JoinOrderOptimizer::GenerateCrossProducts() {
	// generate a set of cross products to combine the currently available plans into a full join plan
	// we create edges between every relation with a high cost
	for (idx_t i = 0; i < relations.size(); i++) {
		auto left = set_manager.GetJoinRelation(i);
		for (idx_t j = 0; j < relations.size(); j++) {
			if (i != j) {
				auto right = set_manager.GetJoinRelation(j);
				query_graph.CreateEdge(left, right, nullptr, false);
				query_graph.CreateEdge(right, left, nullptr, false);
			}
		}
	}
}

static unique_ptr<LogicalOperator> ExtractJoinRelation(SingleJoinRelation &rel) {
	auto &children = rel.parent->children;
	for (idx_t i = 0; i < children.size(); i++) {
		if (children[i].get() == rel.op) {
			// found it! take ownership of it from the parent
			auto result = move(children[i]);
			children.erase(children.begin() + i);
			return result;
		}
	}
	throw Exception("Could not find relation in parent node (?)");
}

pair<JoinRelationSet *, unique_ptr<LogicalOperator>>
JoinOrderOptimizer::GenerateJoins(vector<unique_ptr<LogicalOperator>> &extracted_relations, JoinNode *node) {
	JoinRelationSet *left_node = nullptr, *right_node = nullptr;
	JoinRelationSet *result_relation;
	unique_ptr<LogicalOperator> result_operator;
	if (node->left && node->right) {
		// generate the left and right children
		auto left = GenerateJoins(extracted_relations, node->left);
		auto right = GenerateJoins(extracted_relations, node->right);

		if (node->info->filters.size() == 0) {
			// no filters, create a cross product
			auto join = make_unique<LogicalCrossProduct>();
			join->children.push_back(move(left.second));
			join->children.push_back(move(right.second));
			result_operator = move(join);
		} else {
			// we have filters, create a join node
			auto join = make_unique<LogicalComparisonJoin>(JoinType::INNER);
			join->children.push_back(move(left.second));
			join->children.push_back(move(right.second));
			// set the join conditions from the join node
			for (auto &f : node->info->filters) {
				// extract the filter from the operator it originally belonged to
				assert(filters[f->filter_index]);
				auto condition = move(filters[f->filter_index]);
				// now create the actual join condition
				assert((JoinRelationSet::IsSubset(left.first, f->left_set) &&
				        JoinRelationSet::IsSubset(right.first, f->right_set)) ||
				       (JoinRelationSet::IsSubset(left.first, f->right_set) &&
				        JoinRelationSet::IsSubset(right.first, f->left_set)));
				JoinCondition cond;
				assert(condition->GetExpressionClass() == ExpressionClass::BOUND_COMPARISON);
				auto &comparison = (BoundComparisonExpression &)*condition;
				// we need to figure out which side is which by looking at the relations available to us
				bool invert = JoinRelationSet::IsSubset(left.first, f->left_set) ? false : true;
				cond.left = !invert ? move(comparison.left) : move(comparison.right);
				cond.right = !invert ? move(comparison.right) : move(comparison.left);
				cond.comparison = condition->type;
				if (invert) {
					// reverse comparison expression if we reverse the order of the children
					cond.comparison = FlipComparisionExpression(cond.comparison);
				}
				join->conditions.push_back(move(cond));
				join->op_mark = node->op_mark;
			}
			assert(join->conditions.size() > 0);
			result_operator = move(join);
		}
		left_node = left.first;
		right_node = right.first;
		result_relation = set_manager.Union(left_node, right_node);
	} else {
		// base node, get the entry from the list of extracted relations
		assert(node->set->count == 1);
		assert(extracted_relations[node->set->relations[0]]);
		result_relation = node->set;
		result_operator = move(extracted_relations[node->set->relations[0]]);
	}
	// check if we should do a pushdown on this node
	// basically, any remaining filter that is a subset of the current relation will no longer be used in joins
	// hence we should push it here
	for (idx_t i = 0; i < filter_infos.size(); i++) {
		// check if the filter has already been extracted
		auto info = filter_infos[i].get();
		if (filters[info->filter_index]) {
			// now check if the filter is a subset of the current relation
			// note that infos with an empty relation set are a special case and we do not push them down
			if (info->set->count > 0 && JoinRelationSet::IsSubset(result_relation, info->set)) {
				auto filter = move(filters[info->filter_index]);
				// if it is, we can push the filter
				// we can push it either into a join or as a filter
				// check if we are in a join or in a base table
				if (!left_node || !info->left_set) {
					// base table or non-comparison expression, push it as a filter
					result_operator = PushFilter(move(result_operator), move(filter));
					continue;
				}
				// the node below us is a join or cross product and the expression is a comparison
				// check if the nodes can be split up into left/right
				bool found_subset = false;
				bool invert = false;
				if (JoinRelationSet::IsSubset(left_node, info->left_set) &&
				    JoinRelationSet::IsSubset(right_node, info->right_set)) {
					found_subset = true;
				} else if (JoinRelationSet::IsSubset(right_node, info->left_set) &&
				           JoinRelationSet::IsSubset(left_node, info->right_set)) {
					invert = true;
					found_subset = true;
				}
				if (!found_subset) {
					// could not be split up into left/right
					result_operator = PushFilter(move(result_operator), move(filter));
					continue;
				}
				// create the join condition
				JoinCondition cond;
				assert(filter->GetExpressionClass() == ExpressionClass::BOUND_COMPARISON);
				auto &comparison = (BoundComparisonExpression &)*filter;
				// we need to figure out which side is which by looking at the relations available to us
				cond.left = !invert ? move(comparison.left) : move(comparison.right);
				cond.right = !invert ? move(comparison.right) : move(comparison.left);
				cond.comparison = comparison.type;
				if (invert) {
					// reverse comparison expression if we reverse the order of the children
					cond.comparison = FlipComparisionExpression(comparison.type);
				}
				// now find the join to push it into
				auto node = result_operator.get();
				if (node->type == LogicalOperatorType::FILTER) {
					node = node->children[0].get();
				}
				if (node->type == LogicalOperatorType::CROSS_PRODUCT) {
					// turn into comparison join
					auto comp_join = make_unique<LogicalComparisonJoin>(JoinType::INNER);
					comp_join->children.push_back(move(node->children[0]));
					comp_join->children.push_back(move(node->children[1]));
					comp_join->conditions.push_back(move(cond));
					if (node == result_operator.get()) {
						result_operator = move(comp_join);
					} else {
						assert(result_operator->type == LogicalOperatorType::FILTER);
						result_operator->children[0] = move(comp_join);
					}
				} else {
					assert(node->type == LogicalOperatorType::COMPARISON_JOIN);
					auto &comp_join = (LogicalComparisonJoin &)*node;
					comp_join.conditions.push_back(move(cond));
				}
			}
		}
	}
	return make_pair(result_relation, move(result_operator));
}

unique_ptr<LogicalOperator> JoinOrderOptimizer::RewritePlan(unique_ptr<LogicalOperator> plan, JoinNode *node) {
	// now we have to rewrite the plan
	bool root_is_join = plan->children.size() > 1;

	// first we will extract all relations from the main plan
	vector<unique_ptr<LogicalOperator>> extracted_relations;
	for (idx_t i = 0; i < relations.size(); i++) {
		extracted_relations.push_back(ExtractJoinRelation(*relations[i]));
	}
	// now we generate the actual joins
	auto join_tree = GenerateJoins(extracted_relations, node);
	// perform the final pushdown of remaining filters
	for (idx_t i = 0; i < filters.size(); i++) {
		// check if the filter has already been extracted
		if (filters[i]) {
			// if not we need to push it
			join_tree.second = PushFilter(move(join_tree.second), move(filters[i]));
		}
	}

	// find the first join in the relation to know where to place this node
	if (root_is_join) {
		// first node is the join, return it immediately
		return move(join_tree.second);
	}
	assert(plan->children.size() == 1);
	// have to move up through the relations
	auto op = plan.get();
	auto parent = plan.get();
	while (op->type != LogicalOperatorType::CROSS_PRODUCT && op->type != LogicalOperatorType::COMPARISON_JOIN) {
		assert(op->children.size() == 1);
		parent = op;
		op = op->children[0].get();
	}
	// have to replace at this node
	parent->children[0] = move(join_tree.second);
	return plan;
}

unique_ptr<JoinOrderNode> JoinOrderOptimizer::ParseJONode(json &jo) {
	if (jo.empty()) {
		return nullptr;
	}
	auto node = make_unique<JoinOrderNode>();
	string type = jo["type"];
	auto children = jo["children"];
	if (type == "HJ") {
		node->op_mark = OpMark::HASH_JOIN;
	} else if (type == "RJ") {
		node->op_mark = OpMark::SIP_JOIN;
	} else if (type == "NLJ") {
		node->op_mark = OpMark::NLJ;
	} else if (type == "JOIN") {
		node->op_mark = OpMark::JOIN;
	} else if (type == "SCAN") {
		node->op_mark = OpMark::SCAN;
		if (jo.find("rid") != jo.end()) {
			string rid = jo["rid"];
			node->relation_id = stoi(rid);
		} else {
			string relation_name = jo["table"];
			node->relation_id = relation_alias_mapping[relation_name];
		}
	} else {
		node->op_mark = OpMark::NO_OP;
	}
	for (auto &i : children) {
		auto child = ParseJONode(i);
		node->children.push_back(move(child));
	}
	return node;
}

// the join ordering is pretty much a straight implementation of the paper "Dynamic Programming Strikes Back" by
// Guido Moerkotte and Thomas Neumannn, see that paper for additional info/documentation bonus slides:
// https://db.in.tum.de/teaching/ws1415/queryopt/chapter3.pdf?lang=de
// FIXME: incorporate cardinality estimation into the plans, possibly by pushing samples?
unique_ptr<LogicalOperator> JoinOrderOptimizer::Optimize(unique_ptr<LogicalOperator> plan) {
	assert(filters.size() == 0 && relations.size() == 0); // assert that the JoinOrderOptimizer has not been used before
	LogicalOperator *op = plan.get();
	// now we optimize the current plan
	// we skip past until we find the first projection, we do this because the HAVING clause inserts a Filter AFTER
	// the group by and this filter cannot be reordered extract a list of all relations that have to be joined
	// together and a list of all conditions that is applied to them
	vector<LogicalOperator *> filter_operators;
	if (!ExtractJoinRelations(*op, filter_operators)) {
		// do not support reordering this type of plan
		return plan;
	}
	if (relations.size() <= 1) {
		// at most one relation, nothing to reorder
		return plan;
	}
	// now that we know we are going to perform join ordering we actually extract the filters, eliminating duplicate
	// filters in the process
	expression_set_t filter_set;
	for (auto &op : filter_operators) {
		if (op->type == LogicalOperatorType::COMPARISON_JOIN) {
			auto &join = (LogicalComparisonJoin &)*op;
			assert(join.join_type == JoinType::INNER);
			assert(join.expressions.size() == 0);
			for (auto &cond : join.conditions) {
				auto comparison =
				    make_unique<BoundComparisonExpression>(cond.comparison, move(cond.left), move(cond.right));
				if (filter_set.find(comparison.get()) == filter_set.end()) {
					filter_set.insert(comparison.get());
					filters.push_back(move(comparison));
				}
			}
			join.conditions.clear();
		} else {
			for (idx_t i = 0; i < op->expressions.size(); i++) {
				if (filter_set.find(op->expressions[i].get()) == filter_set.end()) {
					filter_set.insert(op->expressions[i].get());
					filters.push_back(move(op->expressions[i]));
				}
			}
			op->expressions.clear();
		}
	}
	// create potential edges from the comparisons
	for (idx_t i = 0; i < filters.size(); i++) {
		auto &filter = filters[i];
		auto info = make_unique<FilterInfo>();
		auto filter_info = info.get();
		bool has_equality = false;
		filter_infos.push_back(move(info));
		// first extract the relation set for the entire filter
		unordered_set<idx_t> bindings;
		ExtractBindings(*filter, bindings);
		filter_info->set = set_manager.GetJoinRelation(bindings);
		filter_info->filter_index = i;
		// now check if it can be used as a join predicate
		if (filter->GetExpressionClass() == ExpressionClass::BOUND_COMPARISON) {
			auto comparison = (BoundComparisonExpression *)filter.get();
			// extract the bindings that are required for the left and right side of the comparison
			unordered_set<idx_t> left_bindings, right_bindings;
			ExtractBindings(*comparison->left, left_bindings);
			ExtractBindings(*comparison->right, right_bindings);
			if (left_bindings.size() > 0 && right_bindings.size() > 0) {
				// both the left and the right side have bindings
				// first create the relation sets, if they do not exist
				filter_info->left_set = set_manager.GetJoinRelation(left_bindings);
				filter_info->right_set = set_manager.GetJoinRelation(right_bindings);
				// we can only create a meaningful edge if the sets are not exactly the same
				if (filter_info->left_set != filter_info->right_set) {
					// check if the sets are disjoint
					if (Disjoint(left_bindings, right_bindings)) {
						// they are disjoint, we only need to create one set of edges in the join graph
						query_graph.CreateEdge(filter_info->left_set, filter_info->right_set, filter_info,
						                       has_equality);
						query_graph.CreateEdge(filter_info->right_set, filter_info->left_set, filter_info,
						                       has_equality);
					} else {
						continue;
						// the sets are not disjoint, we create two sets of edges
						// auto left_difference = set_manager.Difference(filter_info->left_set,
						// filter_info->right_set); auto right_difference =
						// set_manager.Difference(filter_info->right_set, filter_info->left_set);
						// // -> LEFT <-> RIGHT \ LEFT
						// query_graph.CreateEdge(filter_info->left_set, right_difference, filter_info);
						// query_graph.CreateEdge(right_difference, filter_info->left_set, filter_info);
						// // -> RIGHT <-> LEFT \ RIGHT
						// query_graph.CreateEdge(left_difference, filter_info->right_set, filter_info);
						// query_graph.CreateEdge(filter_info->right_set, left_difference, filter_info);
					}
					continue;
				}
			}
		}
	}

#if ENABLE_CARD_INJECTION
	if (context.enable_cardinality_injection && !context.injected_cardinality.empty()) {
		auto jo = json::parse(context.injected_cardinality);
		InjectCardinality(jo);
	}
#endif

	// now use dynamic programming to figure out the optimal join order
	// First we initialize each of the single-node plans with themselves and with their cardinalities these are the
	// leaf nodes of the join tree NOTE: we can just use pointers to JoinRelationSet* here because the
	// GetJoinRelation function ensures that a unique combination of relations will have a unique JoinRelationSet
	// object.
	for (idx_t i = 0; i < relations.size(); i++) {
		auto &rel = *relations[i];
		auto node = set_manager.GetJoinRelation(i);
		idx_t card;
		if (cached_cardinalities.find(node) != cached_cardinalities.end()) {
			card = cached_cardinalities[node];
		} else {
			card = rel.op->EstimateCardinality();
		}
		plans[node] = make_unique<JoinNode>(node, card);
	}

#if ENABLE_JOIN_ORDER_INJECTION
	if (context.enable_join_order_injection && !context.injected_join_order.empty()) {
		auto jo = json::parse(context.injected_join_order);
		auto join_order_node = ParseJONode(jo);
		if (join_order_node == nullptr) {
			SolveJoinOrder();
		} else {
			InjectJoinNode(move(join_order_node));
		}
	} else {
		SolveJoinOrder();
	}
#else
	SolveJoinOrder();
#endif
	// now the optimal join path should have been found
	// get it from the node
	unordered_set<idx_t> bindings;
	for (idx_t i = 0; i < relations.size(); i++) {
		bindings.insert(i);
	}
	auto total_relation = set_manager.GetJoinRelation(bindings);
	auto final_plan = plans.find(total_relation);
	if (final_plan == plans.end()) {
		// could not find the final plan
		// this should only happen in case the sets are actually disjunct
		// in this case we need to generate cross product to connect the disjoint sets
		GenerateCrossProducts();
		//! solve the join order again
		SolveJoinOrder();
		// now we can obtain the final plan!
		final_plan = plans.find(total_relation);
		assert(final_plan != plans.end());
	}

	// now perform the actual reordering
	return RewritePlan(move(plan), final_plan->second.get());
}
