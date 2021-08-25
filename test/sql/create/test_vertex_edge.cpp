#include "catch.hpp"
#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_entry/edge_catalog_entry.hpp"
#include "duckdb/execution/index/rai/alists.hpp"
#include "duckdb/main/client_context.hpp"
#include "test_helpers.hpp"

using namespace duckdb;
using namespace std;

TEST_CASE("Test CREATE VERTEX", "[create]") {
	unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db);
	con.Query("CREATE TABLE person(id INTEGER PRIMARY KEY, name VARCHAR)");
	con.Query("CREATE TABLE post(id INTEGER, content VARCHAR)");

	REQUIRE_NO_FAIL(con.Query("CREATE VERTEX vPerson ON person;"));
	result = con.Query("CREATE VERTEX vPost ON post;");
	REQUIRE(result->success == false);
	REQUIRE(result->error == "INTERNAL: Cannot find a single primary key on the vertex table to create.");
}

TEST_CASE("Test persistent CREATE VERTEX", "[create]") {
	unique_ptr<QueryResult> result;
	auto db_path = TestCreatePath("test_edge.db");
	DuckDB db(db_path);
	Connection con(db);
	con.Query("CREATE TABLE person(id INTEGER PRIMARY KEY, name VARCHAR)");
	con.Query("CREATE TABLE post(id INTEGER, content VARCHAR)");

	REQUIRE_NO_FAIL(con.Query("CREATE VERTEX vPerson ON person;"));
	result = con.Query("CREATE VERTEX vPost ON post;");
	REQUIRE(result->success == false);
	REQUIRE(result->error == "INTERNAL: Cannot find a single primary key on the vertex table to create.");

	DuckDB db2(db_path);
	Connection con2(db2);
	REQUIRE_NO_FAIL(con2.Query("SELECT * FROM person;"));

	DeleteDatabase(db_path);
}

TEST_CASE("Test Empty CREATE EDGE", "[create]") {
	unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db);
	con.Query("CREATE TABLE person(id INTEGER PRIMARY KEY, name VARCHAR)");
	con.Query("CREATE TABLE likes(personid INTEGER, postid INTEGER)");
	con.Query("CREATE TABLE knows(person1id INTEGER, person2id INTEGER)");
	con.Query("CREATE TABLE post(id INTEGER PRIMARY KEY, content VARCHAR)");
	con.Query("CREATE TABLE orders(orderid INTEGER PRIMARY KEY, personid INTEGER, orderDate TIMESTAMP)");

	REQUIRE_NO_FAIL(con.Query("CREATE VERTEX vPerson ON person"));
	REQUIRE_NO_FAIL(con.Query("CREATE VERTEX vPost ON post"));
	REQUIRE_NO_FAIL(con.Query("CREATE VERTEX vOrders ON orders"));

	REQUIRE_NO_FAIL(
	    con.Query("CREATE EDGE eLikes ON likes (FROM vPerson REFERENCES personid, TO vPost REFERENCES postid)"));
	REQUIRE_NO_FAIL(
	    con.Query("CREATE EDGE eKnows ON knows (FROM vPerson REFERENCES person1id, TO vPerson REFERENCES person2id)"));
	REQUIRE_NO_FAIL(
	    con.Query("CREATE EDGE eOrders ON orders (FROM vPerson REFERENCES personid, TO vOrders REFERENCES orderid)"));
}

TEST_CASE("Test Non-Empty CREATE EDGE", "[create]") {
	unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db);
	con.Query("CREATE TABLE person(id INTEGER PRIMARY KEY, name VARCHAR, gender INTEGER, isStudent BOOLEAN, isWorker "
	          "BOOLEAN, age INTEGER, eyeSight DOUBLE, birthdate DATE, studyAt INTEGER, studyYear INTEGER, workAt "
	          "INTEGER, workYear INTEGER)");
	con.Query("INSERT INTO person VALUES(0,'Alice',1,true,false,35,5.0,'1900-01-01',1,2021,NULL,NULL)");
	con.Query("INSERT INTO person VALUES(2,'Bob',2,true,false,30,5.1,'1900-01-01',1,2020,NULL,NULL)");
	con.Query("INSERT INTO person VALUES(3,'Carol',1,false,true,45,5.0,'1940-06-22',NULL,NULL,4,2010)");
	con.Query("INSERT INTO person VALUES(5,'Dan',2,false,true,20,4.8,'1950-7-23',NULL,NULL,6,2010)");
	con.Query("INSERT INTO person VALUES(7,'Elizabeth',1,false,true,20,4.7,'1980-10-26',NULL,NULL,6,2010)");
	con.Query("INSERT INTO person VALUES(8,'Farooq',2,true,false,25,4.5,'1980-10-26',1,2020,NULL,NULL)");
	con.Query("INSERT INTO person VALUES(9,'Greg',2,false,false,40,4.9,'1980-10-26',NULL,NULL,NULL,NULL)");
	con.Query("INSERT INTO person VALUES(10,'Hubert Blaine "
	          "Wolfeschlegelsteinhausenbergerdorff',2,false,true,83,4.9,'1990-11-27',NULL,NULL,NULL,NULL)");
	result = con.Query("SELECT COUNT(*) FROM person;");
	REQUIRE(CHECK_COLUMN(result, 0, {8}));

	con.Query("CREATE TABLE knows(fromLabel VARCHAR, fromId INTEGER, toLabel VARCHAR, toId INTEGER, since DATE)");
	con.Query("INSERT INTO knows VALUES('person',0,'person',2,'2021-06-30')");
	con.Query("INSERT INTO knows VALUES('person',0,'person',3,'2021-06-30')");
	con.Query("INSERT INTO knows VALUES('person',0,'person',5,'2021-06-30')");
	con.Query("INSERT INTO knows VALUES('person',2,'person',0,'2021-06-30')");
	con.Query("INSERT INTO knows VALUES('person',2,'person',3,'1950-05-14')");
	con.Query("INSERT INTO knows VALUES('person',2,'person',5,'1950-05-14')");
	result = con.Query("SELECT COUNT(*) FROM knows;");
	REQUIRE(CHECK_COLUMN(result, 0, {6}));

	con.Query(
	    "CREATE TABLE organisation(id INTEGER PRIMARY KEY, name VARCHAR, orgCode INTEGER, mark DOUBLE, score INTEGER)");
	con.Query("INSERT INTO organisation VALUES(1,'ABFsUni',325,3.7,-2)");
	con.Query("INSERT INTO organisation VALUES(4,'CsWork',934,4.1,-100)");
	con.Query("INSERT INTO organisation VALUES(6,'DEsWork',824,4.1,7)");
	result = con.Query("SELECT COUNT(*) FROM organisation;");
	REQUIRE(CHECK_COLUMN(result, 0, {3}));

	REQUIRE_NO_FAIL(con.Query("CREATE VERTEX vPerson ON person"));
	REQUIRE_NO_FAIL(con.Query("CREATE VERTEX vOrganisation ON organisation"));

	REQUIRE_NO_FAIL(
	    con.Query("CREATE EDGE eKnows ON knows (FROM vPerson REFERENCES fromId, TO vPerson REFERENCES toId)"));
	REQUIRE_NO_FAIL(
	    con.Query("CREATE EDGE eStudyAt ON person (FROM vPerson REFERENCES id, TO vOrganisation REFERENCES studyAt)"));
	REQUIRE_NO_FAIL(
	    con.Query("CREATE EDGE eWorkAt ON person (FROM vPerson REFERENCES id, TO vOrganisation REFERENCES workAt)"));

	string query = "SELECT fromId_join_index, toId_join_index FROM knows";
	result = con.Query(query);
	REQUIRE(CHECK_COLUMN(result, 0, {0, 0, 0, 1, 1, 1}));
	REQUIRE(CHECK_COLUMN(result, 1, {1, 2, 3, 0, 2, 3}));

	query = "SELECT id_join_index, studyAt_join_index, workAt_join_index FROM person";
	result = con.Query(query);
	REQUIRE(CHECK_COLUMN(result, 0, {0, 1, 2, 3, 4, 5, 6, 7}));
	REQUIRE(CHECK_COLUMN(result, 1, {0, 0, Value(), Value(), Value(), 0, Value(), Value()}));
	REQUIRE(CHECK_COLUMN(result, 2, {Value(), Value(), 1, 2, 2, Value(), Value(), Value()}));

	// Check correctness of the index building.
	con.BeginTransaction();
	auto edge_entry = reinterpret_cast<EdgeCatalogEntry *>(
	    con.context->catalog.GetEntry(*con.context, CatalogType::EDGE, DEFAULT_SCHEMA, "eknows"));
	auto fwd_alists = edge_entry->rai_index->GetALists(EdgeDirection::FORWARD);
	auto fwd_0_list = fwd_alists->GetAList(0);
	REQUIRE(fwd_0_list.numElements == 3);
	for (auto i = 0u; i < fwd_0_list.numElements; i++) {
		REQUIRE(fwd_0_list.vertex_list[i] == (1 + i));
		REQUIRE(fwd_0_list.edge_list[i] == i);
	}

	auto fwd_1_list = fwd_alists->GetAList(1);
	REQUIRE(fwd_1_list.numElements == 3);
	idx_t fwd_1_vertices[] = {0, 2, 3};
	for (auto i = 0u; i < fwd_1_list.numElements; i++) {
		REQUIRE(fwd_1_list.vertex_list[i] == fwd_1_vertices[i]);
		REQUIRE(fwd_1_list.edge_list[i] == (i + 3));
	}

	auto bwd_alists = edge_entry->rai_index->GetALists(EdgeDirection::BACKWARD);
	auto bwd_1_list = bwd_alists->GetAList(1);
	REQUIRE(bwd_1_list.numElements == 1);
	REQUIRE(bwd_1_list.vertex_list[0] == 0);
	REQUIRE(bwd_1_list.edge_list[0] == 0);

	REQUIRE(fwd_alists->GetAList(2).numElements == 0);
	con.Commit();
}

TEST_CASE("Test Persistent CREATE EDGE", "[create]") {
	unique_ptr<QueryResult> result;
	auto db_path = TestCreatePath("test_edge.db");
	DuckDB db(db_path);
	Connection con(db);
	con.Query("CREATE TABLE person(id INTEGER PRIMARY KEY, name VARCHAR, gender INTEGER, isStudent BOOLEAN, isWorker "
	          "BOOLEAN, age INTEGER, eyeSight DOUBLE, birthdate DATE, studyAt INTEGER, studyYear INTEGER, workAt "
	          "INTEGER, workYear INTEGER)");
	con.Query("INSERT INTO person VALUES(0,'Alice',1,true,false,35,5.0,'1900-01-01',1,2021,NULL,NULL)");
	con.Query("INSERT INTO person VALUES(2,'Bob',2,true,false,30,5.1,'1900-01-01',1,2020,NULL,NULL)");
	con.Query("INSERT INTO person VALUES(3,'Carol',1,false,true,45,5.0,'1940-06-22',NULL,NULL,4,2010)");
	con.Query("INSERT INTO person VALUES(5,'Dan',2,false,true,20,4.8,'1950-7-23',NULL,NULL,6,2010)");
	con.Query("INSERT INTO person VALUES(7,'Elizabeth',1,false,true,20,4.7,'1980-10-26',NULL,NULL,6,2010)");
	con.Query("INSERT INTO person VALUES(8,'Farooq',2,true,false,25,4.5,'1980-10-26',1,2020,NULL,NULL)");
	con.Query("INSERT INTO person VALUES(9,'Greg',2,false,false,40,4.9,'1980-10-26',NULL,NULL,NULL,NULL)");
	con.Query("INSERT INTO person VALUES(10,'Hubert Blaine "
	          "Wolfeschlegelsteinhausenbergerdorff',2,false,true,83,4.9,'1990-11-27',NULL,NULL,NULL,NULL)");
	result = con.Query("SELECT COUNT(*) FROM person;");
	REQUIRE(CHECK_COLUMN(result, 0, {8}));

	con.Query("CREATE TABLE knows(fromLabel VARCHAR, fromId INTEGER, toLabel VARCHAR, toId INTEGER, since DATE)");
	con.Query("INSERT INTO knows VALUES('person',0,'person',2,'2021-06-30')");
	con.Query("INSERT INTO knows VALUES('person',0,'person',3,'2021-06-30')");
	con.Query("INSERT INTO knows VALUES('person',0,'person',5,'2021-06-30')");
	con.Query("INSERT INTO knows VALUES('person',2,'person',0,'2021-06-30')");
	con.Query("INSERT INTO knows VALUES('person',2,'person',3,'1950-05-14')");
	con.Query("INSERT INTO knows VALUES('person',2,'person',5,'1950-05-14')");
	result = con.Query("SELECT COUNT(*) FROM knows;");
	REQUIRE(CHECK_COLUMN(result, 0, {6}));

	con.Query(
	    "CREATE TABLE organisation(id INTEGER PRIMARY KEY, name VARCHAR, orgCode INTEGER, mark DOUBLE, score INTEGER)");
	con.Query("INSERT INTO organisation VALUES(1,'ABFsUni',325,3.7,-2)");
	con.Query("INSERT INTO organisation VALUES(4,'CsWork',934,4.1,-100)");
	con.Query("INSERT INTO organisation VALUES(6,'DEsWork',824,4.1,7)");
	result = con.Query("SELECT COUNT(*) FROM organisation;");
	REQUIRE(CHECK_COLUMN(result, 0, {3}));

	REQUIRE_NO_FAIL(con.Query("CREATE VERTEX vPerson ON person"));
	REQUIRE_NO_FAIL(con.Query("CREATE VERTEX vOrganisation ON organisation"));

	REQUIRE_NO_FAIL(
	    con.Query("CREATE EDGE eKnows ON knows (FROM vPerson REFERENCES fromId, TO vPerson REFERENCES toId)"));
	REQUIRE_NO_FAIL(
	    con.Query("CREATE EDGE eStudyAt ON person (FROM vPerson REFERENCES id, TO vOrganisation REFERENCES studyAt)"));
	REQUIRE_NO_FAIL(
	    con.Query("CREATE EDGE eWorkAt ON person (FROM vPerson REFERENCES id, TO vOrganisation REFERENCES workAt)"));

	string query = "SELECT fromId_join_index, toId_join_index FROM knows";
	result = con.Query(query);
	REQUIRE(CHECK_COLUMN(result, 0, {0, 0, 0, 1, 1, 1}));
	REQUIRE(CHECK_COLUMN(result, 1, {1, 2, 3, 0, 2, 3}));

	query = "SELECT id_join_index, studyAt_join_index, workAt_join_index FROM person";
	result = con.Query(query);
	REQUIRE(CHECK_COLUMN(result, 0, {0, 1, 2, 3, 4, 5, 6, 7}));
	REQUIRE(CHECK_COLUMN(result, 1, {0, 0, Value(), Value(), Value(), 0, Value(), Value()}));
	REQUIRE(CHECK_COLUMN(result, 2, {Value(), Value(), 1, 2, 2, Value(), Value(), Value()}));

	DeleteDatabase(db_path);
}
