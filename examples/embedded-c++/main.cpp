#include "duckdb.hpp"
#include "duckdb/common/string_util.hpp"

#include <iostream>

using namespace std;
using namespace duckdb;

void recursiveQuery() {
	DuckDB duckDB(nullptr);
	Connection conn(duckDB);
	auto result = conn.Query("CREATE TABLE person\n"
	                         "(\n"
	                         "    id           INTEGER PRIMARY KEY,\n"
	                         "    first_name   VARCHAR,\n"
	                         "    last_name    VARCHAR,\n"
	                         "    gender       VARCHAR,\n"
	                         "    age_group    VARCHAR,\n"
	                         "    status       VARCHAR,\n"
	                         "    zipcode INTEGER,\n"
	                         "    variant      INTEGER,\n"
	                         "    episode_date DATE,\n"
	                         "    report_date  DATE\n"
	                         ");");
	result = conn.Query("COPY person FROM "
	                    "'/home/guodong/Developer/graindb-private/dataset/dummy-covid/person.csv' WITH HEADER "
	                    "DELIMITER '|';");
	result->Print();
	result = conn.Query("CREATE TABLE contact\n"
	                    "(\n"
	                    "    p1id         BIGINT,\n"
	                    "    p2id         BIGINT,\n"
	                    "    contact_date DATE, relationship VARCHAR\n"
	                    ");");
	result->Print();
	result =
	    conn.Query("COPY contact FROM '/home/guodong/Developer/graindb-private/dataset/dummy-covid/contacts.csv' WITH "
	               "HEADER DELIMITER '|';");
	result->Print();
	conn.Query("CREATE TABLE visit\n"
	           "(\n"
	           "    personid       INTEGER,\n"
	           "    placeid        INTEGER,\n"
	           "    visit_day      DATE,\n"
	           "    visit_hour     INTEGER,\n"
	           "    visit_duration DOUBLE\n"
	           ");");
	result = conn.Query(
	    "COPY visit FROM '/home/guodong/Developer/graindb-private/dataset/dummy-covid/visit.csv' WITH HEADER "
	    "DELIMITER '|';");
	result->Print();
	conn.Query("CREATE TABLE place\n"
	           "(\n"
	           "    id      INTEGER PRIMARY KEY,\n"
	           "    name    VARCHAR,\n"
	           "    address VARCHAR,\n"
	           "    zipcode INTEGER\n"
	           ");");
	result = conn.Query(
	    "COPY place FROM '/home/guodong/Developer/graindb-private/dataset/dummy-covid/place.csv' WITH HEADER "
	    "DELIMITER '|'");
	result->Print();
	conn.Query("CREATE TABLE zipcode\n"
	           "(\n"
	           "    id   INTEGER PRIMARY KEY,\n"
	           "    code VARCHAR,\n"
	           "    city VARCHAR\n"
	           ");");
	result =
	    conn.Query("COPY zipcode FROM '/home/guodong/Developer/graindb-private/dataset/dummy-covid/zipcode.csv' WITH "
	               "HEADER DELIMITER '|';");
	result->Print();
	conn.Query("CREATE TABLE pathogen\n"
	           "(\n"
	           "    id         INTEGER PRIMARY KEY,\n"
	           "    lineage    VARCHAR,\n"
	           "    label      VARCHAR,\n"
	           "    risk_level DOUBLE\n"
	           ");");
	result =
	    conn.Query("COPY pathogen FROM '/home/guodong/Developer/graindb-private/dataset/dummy-covid/pathogen.csv' WITH "
	               "HEADER DELIMITER '|';");
	result->Print();
	conn.Query("CREATE VERTEX vPerson ON person;");
	conn.Query("CREATE VERTEX vPlace ON place;");
	conn.Query("CREATE EDGE eContact ON contact (FROM vPerson REFERENCES p1id, TO vPerson REFERENCES p2id);");
	conn.Query("CREATE EDGE eVisit ON visit (FROM vPerson REFERENCES personid, TO vPlace REFERENCES placeid);");

	conn.EnableProfiling();
	result = conn.Query("select a.id, count(c.id) * 10 + sum(pathogen.risk_level) as risk\n"
	                    "from (c:vPerson)<-[e0:eContact]-(a:vPerson)-[e1:eVisit]->(p:vPlace)<-[e2:eVisit]-(b:vPerson), "
	                    "zipcode, pathogen\n"
	                    "where a.status='unknown'\n"
	                    "  and a.zipcode=zipcode.id\n"
	                    "  and zipcode.code='N2T 1H4'\n"
	                    "  and e1.visit_day=e2.visit_day\n"
	                    "  and b.status='positive'\n"
	                    "  and c.status='positive'\n"
	                    "  and c.variant=pathogen.id\n"
	                    "group by a.id\n"
	                    "order by risk desc\n"
	                    "limit 5");
	result->Print();
	//	auto result = conn.Query("WITH RECURSIVE contact_cte AS\n"
	//	                         "(\n"
	//	                         "   SELECT p1.rowid as p1rowid, p1.rowid as p2rowid, p1.id as p1id, p1.id as p2id\n"
	//	                         "   FROM person p1\n"
	//	                         "   WHERE p1.id = 65\n"
	//	                         "   UNION ALL\n"
	//	                         "   SELECT contact_cte.p2rowid, b.rowid, contact_cte.p2id, b.id\n"
	//	                         "   FROM person b, contact e, contact_cte \n"
	//	                         "   WHERE contact_cte.p2id = e.p1id AND b.id = e.p2id\n"
	//	                         ")\n"
	//	                         "SELECT * FROM contact_cte where p1id!=p2id;");
	//	cout << conn.GetProfilingInformation() << endl;
	//	result->Print();
}

void createEdge() {
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

	con.Query("CREATE TABLE knows(fromLabel VARCHAR, fromId INTEGER, toLabel VARCHAR, toId INTEGER, since DATE)");
	con.Query("INSERT INTO knows VALUES('person',0,'person',2,'2021-06-30')");
	con.Query("INSERT INTO knows VALUES('person',0,'person',3,'2021-06-30')");
	con.Query("INSERT INTO knows VALUES('person',0,'person',5,'2021-06-30')");
	con.Query("INSERT INTO knows VALUES('person',2,'person',0,'2021-06-30')");
	con.Query("INSERT INTO knows VALUES('person',2,'person',3,'1950-05-14')");
	con.Query("INSERT INTO knows VALUES('person',2,'person',5,'1950-05-14')");

	result = con.Query("CREATE EDGE eKnows ON knows (FROM fromId REFERENCES person.id, TO toId REFERENCES person.id)");
	result->Print();
}

void createVertex() {
	unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db);
	con.Query("CREATE TABLE person(id INTEGER PRIMARY KEY, name VARCHAR)");
	con.Query("CREATE TABLE post(id INTEGER PRIMARY KEY, content VARCHAR)");
	con.Query("CREATE TABLE likes(personid INTEGER, postid INTEGER)");
	result = con.Query("CREATE VERTEX vPerson ON person;");
	result->Print();
	result = con.Query("CREATE VERTEX vPost ON post;");
	result->Print();
	result = con.Query("CREATE EDGE eLikes ON likes (FROM vPerson REFERENCES personid, TO vPost REFERENCES postid);");
	result->Print();
	result = con.Query("SELECT ps.content FROM person p, likes l, post ps WHERE p.id=l.personid AND ps.id=l.postid;");
	con.EnableProfiling();
	result->Print();
	cout << con.GetProfilingInformation();
}

void pathPattern() {
	unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db);
	result =
	    con.Query("CREATE TABLE person (id INTEGER PRIMARY KEY, first_name VARCHAR, last_name VARCHAR, gender VARCHAR, "
	              "birthday DATE, status INTEGER, place INTEGER, variant INTEGER, confirm_date DATE);");
	result->Print();
	result = con.Query("CREATE TABLE contacts (p1id INTEGER, p2id INTEGER, contact_date DATE);");
	result->Print();
	result = con.Query("COPY person FROM '/home/guodong/Developer/graindb-private/dataset/dummy-covid/person.csv'  "
	                   "WITH HEADER DELIMITER '|';;");
	result->Print();
	result = con.Query("COPY contacts FROM '/home/guodong/Developer/graindb-private/dataset/dummy-covid/contacts.csv'  "
	                   "WITH HEADER DELIMITER '|';;");
	result->Print();
	result = con.Query("CREATE VERTEX vPerson ON person;");
	result->Print();
	result = con.Query("CREATE EDGE eContacts ON contacts (FROM vPerson REFERENCES p1id, TO vPerson REFERENCES p2id);");
	result->Print();
	con.EnableProfiling();
	string plan =
	    "{\"type\": \"JOIN\", \"children\": [{\"type\": \"SCAN\", \"table\": \"p1\"}, {\"type\": \"JOIN\", "
	    "\"children\": [{\"type\": \"SCAN\", \"table\": \"c\"}, {\"type\": \"SCAN\", \"table\": \"p2\"} ] } ] }";
	con.EnableExplicitJoinOrder(plan);
	// result = con.Query("SELECT p2.first_name FROM person p1, contacts c, person p2 WHERE p1.first_name='Marc' AND "
	//                   "p1.id=c.p1id AND p2.id=c.p2id;");
	result = con.Query("SELECT p2.first_name FROM (p1:vPerson)-[c:eContacts]->(p2:vPerson) WHERE p1.first_name='Marc'");
	result->Print();
	cout << con.GetProfilingInformation();
}

void variableReturn() {
	unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db);
	con.Query("CREATE TABLE person (id INTEGER PRIMARY KEY, i INTEGER)");
	con.Query("CREATE TABLE post (id INTEGER PRIMARY KEY, j INTEGER)");
	con.Query("CREATE TABLE likes (personid INTEGER, postid INTEGER, l INTEGER)");
	con.Query("CREATE TABLE knows (p1id INTEGER, p2id INTEGER, k INTEGER)");
	con.Query("CREATE VERTEX vPerson ON person");
	con.Query("CREATE VERTEX vPost ON post");
	con.Query("CREATE EDGE eLikes ON likes(FROM vPerson REFERENCES personid, TO vPost REFERENCES postid)");
	con.Query("CREATE EDGE eKnows ON knows(FROM vPerson REFERENCES p1id, TO vPerson REFERENCES p2id)");

	con.EnableProfiling();
	result = con.Query("SELECT a FROM (a:vPerson)");
	result->Print();
	cout << con.GetProfilingInformation();
}

void debug() {
	unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db);
	con.Query("CREATE TABLE person (id INTEGER PRIMARY KEY, i INTEGER)");
	con.Query("CREATE TABLE post (id INTEGER PRIMARY KEY, j INTEGER)");
	con.Query("CREATE TABLE likes (personid INTEGER, postid INTEGER, l INTEGER)");
	con.Query("CREATE TABLE knows (p1id INTEGER, p2id INTEGER, k INTEGER)");
	con.Query("CREATE VERTEX vPerson ON person");
	con.Query("CREATE VERTEX vPost ON post");
	con.Query("CREATE EDGE eLikes ON likes(FROM vPerson REFERENCES personid, TO vPost REFERENCES postid)");
	con.Query("CREATE EDGE eKnows ON knows(FROM vPerson REFERENCES p1id, TO vPerson REFERENCES p2id)");

	con.EnableProfiling();
	// result = con.Query("SELECT b.* FROM (a:vPerson)-[e1:eLikes*1..2]->(b:vPost)");
	result = con.Query("SELECT b.j FROM (a:vPerson)-[e:eLikes]->(b:vPost), (d:vPerson)-[e2:eKnows]->(c:vPerson)");
	result->Print();
	cout << con.GetProfilingInformation();
}

int main(int argc, char *argv[]) {
	//	variableReturn();
	//	recursiveQuery();
	// createEdge();
	// createVertex();
	//	pathPattern();
	debug();
}
