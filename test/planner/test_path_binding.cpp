#include "catch.hpp"
#include "expression_helper.hpp"
#include "test_helpers.hpp"

using namespace duckdb;
using namespace std;

TEST_CASE("Test path bindings", "[path-binding]") {
	ExpressionHelper helper;
	using Op = LogicalOperatorType;

	auto join_matches = [&](const string &query, const vector<LogicalOperatorType> &path, LogicalOperatorType last,
	                        OpHint op_hint) -> bool {
		auto plan = helper.ParseAndOptimizeLogicalTree(query);
		for (auto type : path) {
			if (plan->type != type)
				return false;
			if (plan->children.empty())
				return false;
			if (plan->children.size() == 1) {
				plan = move(plan->children[0]);
			} else {
				plan = move(plan->children[1]);
			}
		}
		return (plan->type == last && plan->op_hint == op_hint);
	};

	auto &con = helper.con;
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE person (id INTEGER PRIMARY KEY, i INTEGER)"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE post (id INTEGER PRIMARY KEY, j INTEGER)"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE likes (personid INTEGER, postid INTEGER, k INTEGER)"));
	REQUIRE_NO_FAIL(con.Query("CREATE VERTEX vPerson ON person"));
	REQUIRE_NO_FAIL(con.Query("CREATE VERTEX vPost ON post"));
	REQUIRE_NO_FAIL(
	    con.Query("CREATE EDGE eLikes ON likes(FROM vPerson REFERENCES personid, TO vPost REFERENCES postid)"));

	REQUIRE(join_matches("SELECT i FROM (a:vPerson)", {Op::PROJECTION}, Op::GET, OpHint::NO_OP));
	REQUIRE(join_matches("SELECT b.j FROM (a:vPerson)-[e:eLikes]->(b:vPost)", {Op::PROJECTION, Op::COMPARISON_JOIN},
	                     LogicalOperatorType::COMPARISON_JOIN, OpHint::SJ));
	REQUIRE(join_matches("SELECT b.j FROM (b:vPost)<-[e:eLikes]-(a:vPerson)", {Op::PROJECTION, Op::COMPARISON_JOIN},
	                     LogicalOperatorType::COMPARISON_JOIN, OpHint::SJ));
	REQUIRE(join_matches("SELECT b.j FROM (a:vPerson)-[e1:eLikes]->(b:vPost)<-[e2:eLikes]-(c:vPerson)",
	                     {Op::PROJECTION, Op::COMPARISON_JOIN}, LogicalOperatorType::COMPARISON_JOIN, OpHint::SJ));
}
