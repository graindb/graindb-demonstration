#include "catch.hpp"
#include "duckdb/common/helper.hpp"
#include "duckdb/optimizer/filter_pushdown.hpp"
#include "duckdb/optimizer/join_order_optimizer.hpp"
#include "duckdb/optimizer/optimizer.hpp"
#include "duckdb/optimizer/remove_unused_columns.hpp"
#include "duckdb/optimizer/sip_join_merger.hpp"
#include "duckdb/optimizer/sip_join_rewriter.hpp"
#include "duckdb/planner/binder.hpp"
#include "duckdb/planner/expression/bound_comparison_expression.hpp"
#include "duckdb/planner/expression/bound_operator_expression.hpp"
#include "duckdb/planner/expression/common_subexpression.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "expression_helper.hpp"
#include "ldbc.hpp"
#include "test_helpers.hpp"

#include <vector>

using namespace duckdb;
using namespace std;

static unique_ptr<LogicalOperator> ApplyOptPipeline(unique_ptr<LogicalOperator> plan, Optimizer &opt,
                                                    ClientContext &context) {
	FilterPushdown predicatePushDown(opt);
	plan = predicatePushDown.Rewrite(move(plan));
	JoinOrderOptimizer join_order_opt(context);
	plan = join_order_opt.Optimize(move(plan));
	SIPJoinRewriter sip_join_rewriter;
	plan = sip_join_rewriter.Rewrite(move(plan));
	RemoveUnusedColumns remove_unused(true);
	remove_unused.VisitOperator(*plan);
	return plan;
}

TEST_CASE("Test Simple Sip Join Rewriter", "[sip_join_rewriter]") {
	ExpressionHelper helper;
	auto &con = helper.con;
	Binder binder(*con.context);
	Optimizer opt(binder, *con.context);
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE person (id integer primary key, name varchar);"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE knows (p1id integer, p2id integer);"));
	REQUIRE_NO_FAIL(con.Query("CREATE VERTEX vPerson ON person;"));
	REQUIRE_NO_FAIL(
	    con.Query("CREATE EDGE eKnows ON knows (FROM vPerson REFERENCES p1id, TO vPerson REFERENCES p2id);"));
	auto tree =
	    helper.ParseLogicalTree("SELECT p2id FROM person p1, knows k, person p2 WHERE p1.id=k.p1id AND p2.id=k.p2id;");
	con.EnableExplicitJoinOrder(
	    "{\"type\": \"JOIN\", \"children\": [{\"type\": \"SCAN\", \"table\": \"p1\"}, {\"type\": \"JOIN\", "
	    "\"children\": [{\"type\": \"SCAN\", \"table\": \"k\"}, {\"type\": \"SCAN\", \"table\": \"p2\"} ] } ] }");
	auto plan = ApplyOptPipeline(move(tree), opt, *con.context);
	REQUIRE(plan->children[0]->type == LogicalOperatorType::COMPARISON_JOIN);
	REQUIRE(plan->children[0]->op_hint == OpHint::SJ);
	REQUIRE(plan->children[0]->children[1]->type == LogicalOperatorType::COMPARISON_JOIN);
	REQUIRE(plan->children[0]->children[1]->op_hint == OpHint::SJ);
}

TEST_CASE("Test LDBC Sip Join Rewriter", "[sip_join_rewriter]") {
	ExpressionHelper helper;
	auto &con = helper.con;
	Binder binder(*con.context);
	Optimizer opt(binder, *con.context);
	for (auto i = 0; i < 18; i++) {
		REQUIRE_NO_FAIL(con.Query(ldbc::get_ddl(i)));
	}
	REQUIRE_NO_FAIL(con.Query("CREATE VERTEX vPerson ON person;"));
	REQUIRE_NO_FAIL(con.Query(
	    "CREATE EDGE eKnows ON knows (FROM vPerson REFERENCES k_person1id, TO vPerson REFERENCES k_person2id);"));
	auto tree =
	    helper.ParseLogicalTree("select p2.id, p2.p_lastname, p2.p_birthday, p2.p_creationdate, p2.p_gender, "
	                            "p2.p_browserused, p2.p_locationip, pl.pl_name from person p1 JOIN (knows JOIN (person "
	                            "p2 JOIN place pl ON p2.p_placeid=pl.pl_placeid) ON k_person2id=p2.p_personid) ON "
	                            "p1.p_personid=k_person1id where p1.id = 8796093028953 and p2.p_firstname = 'John';");
	con.EnableExplicitJoinOrder(
	    "{\"type\": \"JOIN\", \"children\": [{\"type\": \"SCAN\", \"table\": \"pl\"}, {\"type\": \"JOIN\", "
	    "\"children\": [{\"type\": \"SCAN\", \"table\": \"p2\"}, {\"type\": \"JOIN\", \"children\": [{\"type\": "
	    "\"SCAN\", \"table\": \"knows\"}, {\"type\": \"SCAN\", \"table\": \"p1\"} ] } ] } ] }");
	auto plan = ApplyOptPipeline(move(tree), opt, *con.context);
	REQUIRE(plan->children[0]->type == LogicalOperatorType::COMPARISON_JOIN);
	REQUIRE(plan->children[0]->children[1]->type == LogicalOperatorType::COMPARISON_JOIN);
	REQUIRE(plan->children[0]->children[1]->op_hint == OpHint::SJ);
	REQUIRE(plan->children[0]->children[1]->children[1]->type == LogicalOperatorType::COMPARISON_JOIN);
	REQUIRE(plan->children[0]->children[1]->children[1]->op_hint == OpHint::SJ);
}

TEST_CASE("Test Simple Sip Join Merger", "[sip_join_merger]") {
	ExpressionHelper helper;
	auto &con = helper.con;
	Binder binder(*con.context);
	Optimizer opt(binder, *con.context);
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE person (id integer primary key, name varchar);"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE knows (p1id integer, p2id integer);"));
	REQUIRE_NO_FAIL(con.Query("CREATE VERTEX vPerson ON person;"));
	REQUIRE_NO_FAIL(
	    con.Query("CREATE EDGE eKnows ON knows (FROM vPerson REFERENCES p1id, TO vPerson REFERENCES p2id);"));
	auto tree = helper.ParseLogicalTree(
	    "SELECT p2.name FROM person p1, knows k, person p2 WHERE p1.id=k.p1id AND p2.id=k.p2id;");
	con.EnableExplicitJoinOrder(
	    "{\"type\": \"JOIN\", \"children\": [{\"type\": \"SCAN\", \"table\": \"p1\"}, {\"type\": \"JOIN\", "
	    "\"children\": [{\"type\": \"SCAN\", \"table\": \"k\"}, {\"type\": \"SCAN\", \"table\": \"p2\"} ] } ] }");
	auto plan = ApplyOptPipeline(move(tree), opt, *con.context);
	SIPJoinMerger merger;
	plan = merger.Rewrite(move(plan));
	REQUIRE(plan->children[0]->type == LogicalOperatorType::COMPARISON_JOIN);
	REQUIRE(plan->children[0]->op_hint == OpHint::MSJ);
	REQUIRE(plan->children[0]->children[0]->type == LogicalOperatorType::GET);
	REQUIRE(plan->children[0]->children[1]->type == LogicalOperatorType::GET);
}

TEST_CASE("Test LDBC Sip Join Merger", "[sip_join_merger]") {
	ExpressionHelper helper;
	auto &con = helper.con;
	Binder binder(*con.context);
	Optimizer opt(binder, *con.context);
	for (auto i = 0; i < 18; i++) {
		REQUIRE_NO_FAIL(con.Query(ldbc::get_ddl(i)));
	}
	REQUIRE_NO_FAIL(con.Query("CREATE VERTEX vPerson ON person;"));
	REQUIRE_NO_FAIL(con.Query(
	    "CREATE EDGE eKnows ON knows (FROM vPerson REFERENCES k_person1id, TO vPerson REFERENCES k_person2id);"));
	auto tree =
	    helper.ParseLogicalTree("select p2.id, p2.p_lastname, p2.p_birthday, p2.p_creationdate, p2.p_gender, "
	                            "p2.p_browserused, p2.p_locationip, pl.pl_name from person p1 JOIN (knows JOIN (person "
	                            "p2 JOIN place pl ON p2.p_placeid=pl.pl_placeid) ON k_person2id=p2.p_personid) ON "
	                            "p1.p_personid=k_person1id where p1.id = 8796093028953 and p2.p_firstname = 'John';");
	con.EnableExplicitJoinOrder(
	    "{\"type\": \"JOIN\", \"children\": [{\"type\": \"SCAN\", \"table\": \"pl\"}, {\"type\": \"JOIN\", "
	    "\"children\": [{\"type\": \"SCAN\", \"table\": \"p2\"}, {\"type\": \"JOIN\", \"children\": [{\"type\": "
	    "\"SCAN\", \"table\": \"knows\"}, {\"type\": \"SCAN\", \"table\": \"p1\"} ] } ] } ] }");
	auto plan = ApplyOptPipeline(move(tree), opt, *con.context);
	SIPJoinMerger merger;
	plan = merger.Rewrite(move(plan));
	REQUIRE(plan->children[0]->type == LogicalOperatorType::COMPARISON_JOIN);
	REQUIRE(plan->children[0]->children[0]->type == LogicalOperatorType::GET);
	REQUIRE(plan->children[0]->children[1]->type == LogicalOperatorType::COMPARISON_JOIN);
	REQUIRE(plan->children[0]->children[1]->op_hint == OpHint::MSJ);
}