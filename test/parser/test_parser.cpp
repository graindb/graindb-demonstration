#include "catch.hpp"
#include "duckdb/common/enums/statement_type.hpp"
#include "duckdb/parser/parser.hpp"

using namespace duckdb;

TEST_CASE("Test parser", "[parser]") {
	Parser parser;

	SECTION("Query with several statements") {
		parser.ParseQuery("CREATE TABLE nums (num INTEGER);"
		                  "BEGIN TRANSACTION;"
		                  "    INSERT INTO nums VALUES(1);"
		                  "    INSERT INTO nums VALUES(2);"
		                  "    INSERT INTO nums VALUES(3);"
		                  "    INSERT INTO nums VALUES(4);"
		                  "COMMIT;");

		REQUIRE(parser.statements.size() == 7);
		REQUIRE(parser.statements[0]->type == StatementType::CREATE_STATEMENT);
		REQUIRE(parser.statements[1]->type == StatementType::TRANSACTION_STATEMENT);
		REQUIRE(parser.statements[2]->type == StatementType::INSERT_STATEMENT);
		REQUIRE(parser.statements[3]->type == StatementType::INSERT_STATEMENT);
		REQUIRE(parser.statements[4]->type == StatementType::INSERT_STATEMENT);
		REQUIRE(parser.statements[5]->type == StatementType::INSERT_STATEMENT);
		REQUIRE(parser.statements[6]->type == StatementType::TRANSACTION_STATEMENT);
	}

	SECTION("Create vertex statement") {
		parser.ParseQuery("CREATE VERTEX vPerson ON person;");
		REQUIRE(parser.statements.size() == 1);
		REQUIRE(parser.statements[0]->type == StatementType::CREATE_STATEMENT);
	}

	SECTION("Create edge statement") {
		parser.ParseQuery("CREATE EDGE eKnows ON knows (FROM vPerson REFERENCES person1id,"
		                  " TO vPerson REFERENCES person2id);");
		REQUIRE(parser.statements.size() == 1);
		REQUIRE(parser.statements[0]->type == StatementType::CREATE_STATEMENT);
	}

	SECTION("Node only path pattern query") {
		parser.ParseQuery("SELECT * FROM (a:person);");
		REQUIRE(parser.statements.size() == 1);
		REQUIRE(parser.statements[0]->type == StatementType::SELECT_STATEMENT);
	}

	SECTION("Node only path pattern query") {
		parser.ParseQuery("SELECT * FROM (:person);");
		REQUIRE(parser.statements.size() == 1);
		REQUIRE(parser.statements[0]->type == StatementType::SELECT_STATEMENT);
	}

	SECTION("Simple path pattern query") {
		parser.ParseQuery("SELECT * FROM (a:person)-[e:knows]->(b:person);");
		REQUIRE(parser.statements.size() == 1);
		REQUIRE(parser.statements[0]->type == StatementType::SELECT_STATEMENT);
	}

	SECTION("Simple recursive path pattern query 1") {
		parser.ParseQuery("SELECT * FROM (a:person)-[e:knows*2..5]->(b:person);");
		REQUIRE(parser.statements.size() == 1);
		REQUIRE(parser.statements[0]->type == StatementType::SELECT_STATEMENT);
	}

	SECTION("Simple recursive path pattern query 2") {
		parser.ParseQuery("SELECT * FROM (a:person)-[e:knows*2..]->(b:person);");
		REQUIRE(parser.statements.size() == 1);
		REQUIRE(parser.statements[0]->type == StatementType::SELECT_STATEMENT);
	}

	SECTION("Simple recursive path pattern query 3") {
		parser.ParseQuery("SELECT * FROM (a:person)-[e:knows*..5]->(b:person);");
		REQUIRE(parser.statements.size() == 1);
		REQUIRE(parser.statements[0]->type == StatementType::SELECT_STATEMENT);
	}

	SECTION("Simple recursive path pattern query 4") {
		parser.ParseQuery("SELECT * FROM (a:person)-[e:knows*2]->(b:person);");
		REQUIRE(parser.statements.size() == 1);
		REQUIRE(parser.statements[0]->type == StatementType::SELECT_STATEMENT);
	}

	SECTION("Simple recursive path pattern query 5") {
		parser.ParseQuery("SELECT * FROM (a:person)-[e:knows*]->(b:person);");
		REQUIRE(parser.statements.size() == 1);
		REQUIRE(parser.statements[0]->type == StatementType::SELECT_STATEMENT);
	}

	SECTION("Complicated path pattern query") {
		parser.ParseQuery("SELECT * FROM (a:person)-[e1:knows]->(b:person)<-[e2:knows]-(c:person);");
		REQUIRE(parser.statements.size() == 1);
		REQUIRE(parser.statements[0]->type == StatementType::SELECT_STATEMENT);
	}

	SECTION("Multiple parts path pattern query") {
		parser.ParseQuery("SELECT * FROM (a:person)-[e1:knows]->(b:person), (b)<-[e2:knows]-(c:person);");
		REQUIRE(parser.statements.size() == 1);
		REQUIRE(parser.statements[0]->type == StatementType::SELECT_STATEMENT);
	}

	SECTION("Multiple parts path pattern query 2") {
		parser.ParseQuery("SELECT * FROM (a:person)-[e1:knows]->(b:person), (d)<-[e2:knows]-(c:person);");
		REQUIRE(parser.statements.size() == 1);
		REQUIRE(parser.statements[0]->type == StatementType::SELECT_STATEMENT);
	}

	SECTION("Mixed path pattern and table query") {
		parser.ParseQuery(
		    "SELECT * FROM (a:person)-[e1:knows]->(b:person)<-[e2:knows]-(c:person), table1 WHERE table1.id=c.id;");
		REQUIRE(parser.statements.size() == 1);
		REQUIRE(parser.statements[0]->type == StatementType::SELECT_STATEMENT);
	}

	SECTION("Wrong path pattern query") {
		REQUIRE_THROWS(parser.ParseQuery("SELECT * FROM (a:person)-[e1:knows]<-(b:person);"));
	}

	SECTION("Wrong query") {
		REQUIRE_THROWS(parser.ParseQuery("TABLE"));
	}

	SECTION("Empty query") {
		parser.ParseQuery("");
		REQUIRE(parser.statements.empty());
	}

	SECTION("Pragma query") {
		parser.ParseQuery("PRAGMA table_info('nums');");
		REQUIRE(parser.statements.size() == 1);
	}

	SECTION("Wrong pragma query") {
		parser.ParseQuery("PRAGMA table_info;");
		REQUIRE(parser.statements.size() == 1);
	}
}
