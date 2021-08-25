#pragma once

#include <cstring>

using namespace std;

static const string prepare_statements[] = {
    "CREATE TABLE person (id INTEGER PRIMARY KEY, first_name VARCHAR, last_name VARCHAR, gender VARCHAR, age_group "
    "VARCHAR, status VARCHAR, zipcode INTEGER, variant INTEGER, episode_date DATE, report_date  DATE );",
    "CREATE TABLE place (id INTEGER PRIMARY KEY, name VARCHAR, address VARCHAR, zipcode INTEGER );",
    "CREATE TABLE zipcode (id INTEGER PRIMARY KEY, code VARCHAR, city VARCHAR );",
    "CREATE TABLE pathogen (id INTEGER PRIMARY KEY, lineage VARCHAR, label VARCHAR, risk_level DOUBLE );",
    "CREATE TABLE contact (p1id INTEGER, p2id INTEGER, contact_date DATE, relationship VARCHAR );",
    "CREATE TABLE visit(personid INTEGER, placeid INTEGER, visit_day DATE, visit_hour INTEGER, visit_duration DOUBLE);",
    "COPY person FROM './dataset/dummy-covid/person.csv' WITH HEADER DELIMITER '|'",
    "COPY place FROM './dataset/dummy-covid/place.csv' WITH HEADER DELIMITER '|';",
    "COPY pathogen FROM './dataset/dummy-covid/pathogen.csv' WITH HEADER DELIMITER '|'",
    "COPY contact FROM './dataset/dummy-covid/contacts.csv' WITH HEADER DELIMITER '|'",
    "COPY visit FROM './dataset/dummy-covid/visit.csv' WITH HEADER DELIMITER '|';",
    "COPY zipcode FROM './dataset/dummy-covid/zipcode.csv' WITH HEADER DELIMITER '|';"};
