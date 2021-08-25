-- restful query
http://localhost:1294/query?q=
-- rewrite: "=" -> %3D, "+" -> %2B

-- SCENARIO 1
CREATE VERTEX vPerson ON person;
CREATE VERTEX vPlace ON place;
CREATE EDGE eContact ON contact (FROM vPerson REFERENCES p1id, TO vPerson REFERENCES p2id);
CREATE EDGE eVisit ON visit (FROM vPerson REFERENCES personid, TO vPlace REFERENCES placeid);

-- Query I
select person.age_group, pathogen.label, count(*) from person, pathogen, zipcode where zipcode.city%3D'Waterloo'and person.status%3D'positive'and person.report_date>%3D'2011-08-22'and person.variant%3Dpathogen.id and person.zipcode%3Dzipcode.id group by person.age_group, pathogen.label

-- Query II
select a, b from (a:vPerson)-[e:eContact*1..3]->(b:vPerson) where a.first_name%3D'ken' and a.last_name%3D'sen' and b.status%3D'positive'

-- Query III find people in a region with high-risk of infection
select a.id, count(c.id) * 10 %2B sum(pathogen.risk_level) as risk from (c:vPerson)<-[e0:eContact]-(a:vPerson)-[e1:eVisit]->(p:vPlace)<-[e2:eVisit]-(b:vPerson), zipcode, pathogen where a.status%3D'unknown'and a.zipcode%3Dzipcode.id and zipcode.code%3D'N2T 1H4'and e1.visit_day%3De2.visit_day and b.status%3D'positive'and c.status%3D'positive'and c.variant%3Dpathogen.id group by a.id order by risk desc limit 5

-- SCENARIO 2
-- Get the positive individual, Mario
select a from (a:vperson) where a.id%3D134;

-- Find Mario's close contacts
select a, b from (a:vperson)-[e:econtact]->(b:vperson) where e.contact_date>%3D'2021-08-15' and a.id%3D134;

-- Find places Mario visited in the 7 days before tested positive
select a, b from (a:vperson)-[e:evisit]->(b:vplace) where a.id%3D134 and e.visit_day>%3D'2021-08-15';

-- Find other people co-visited the library in the same day as Mario did
select a, b from (a:vperson)-[e:evisit]->(b:vplace) where b.id%3D1472 and e.visit_day%3D'2021-08-20';

-- Find places Alex visited in the past week
select a, b from (a:vperson)-[e:evisit]->(b:vplace) where a.id%3D164 and e.visit_day>%3D'2021-08-15';

-- Find people co-visited the park
select a, b from (a:vperson)-[e:evisit]->(b:vplace) where b.id%3D1473 and e.visit_day>%3D'2021-08-15';

-- Identify Carlos as a positive individual, and expand on Carlos's close contacts and visited places
select a, b from (a:vperson)-[e:evisit]->(b:vplace) where a.id%3D153 and e.visit_day>%3D'2021-08-15';
select a, b from (a:vperson)-[e:evisit]->(b:vplace) where b.id%3D1474 and e.visit_day>%3D'2021-08-15';