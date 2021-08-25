-- restful query
http://localhost:1294/query?q=

-- SCENARIO 1
-- Schema Design
CREATE VERTEX vPerson ON person;
CREATE VERTEX vPlace ON place;
CREATE EDGE eContact ON contact (FROM vPerson REFERENCES p1id, TO vPerson REFERENCES p2id);
CREATE EDGE eVisit ON visit (FROM vPerson REFERENCES personid, TO vPlace REFERENCES placeid);

-- Query I
select person.age_group, pathogen.label, count(*) from person, pathogen, zipcode where zipcode.city = 'Waterloo'and person.status = 'positive'and person.report_date >= '2011-08-22'and person.variant = pathogen.id and person.zipcode = zipcode.id group by person.age_group, pathogen.label

-- Query II
select a, b from (a:vPerson)-[e:eContact*1..3]->(b:vPerson) where a.first_name='ken' and a.last_name='sen' and b.status='positive'

-- Query III find people in a region with high-risk of infection
select a.id, count(c.id) * 10 + sum(pathogen.risk_level) as risk from (c:vPerson)<-[e0:eContact]-(a:vPerson)-[e1:eVisit]->(p:vPlace)<-[e2:eVisit]-(b:vPerson), zipcode, pathogen where a.status='unknown'and a.zipcode=zipcode.id and zipcode.code='N2T 1H4'and e1.visit_day=e2.visit_day and b.status='positive'and c.status='positive'and c.variant=pathogen.id group by a.id order by risk desc limit 5


-- SCENARIO 2
-- Get the positive individual, Mario
select a from (a:vPerson) where a.id = 134

-- Find Mario's close contacts
select a, b from (a:vPerson)-[e:eContact]->(b:vPerson) where e.contact_date>='2021-08-15' and a.id=134

-- Find places Mario visited in the 7 days before tested positive
select a, b from (a:vPerson)-[e:eVisit]->(b:vPlace) where a.id=134 and e.visit_day>='2021-08-15'

-- Find other people co-visited the library in the same day as Mario did
select a, b from (a:vPerson)-[e:evisit]->(b:vPlace) where b.id=1472 and e.visit_day='2021-08-20'

-- Find places Alex visited in the past week
select a, b from (a:vPerson)-[e:evisit]->(b:vPlace) where a.id=164 and e.visit_day>='2021-08-15'

-- Find people co-visited the park
select a, b from (a:vPerson)-[e:evisit]->(b:vPlace) where b.id=1473 and e.visit_day>='2021-08-15'

-- Identify Carlos as a positive individual, and expand on Carlos's close contacts and visited places
select a, b from (a:vPerson)-[e:evisit]->(b:vPlace) where a.id=153 and e.visit_day>='2021-08-15'
select a, b from (a:vPerson)-[e:evisit]->(b:vPlace) where b.id=1474 and e.visit_day>='2021-08-15'


-- SCENARIO 1
We show developers can use GRainDB for daily reporting in the Waterloo region

1. Cases by reported date.
SELECT report_date, COUNT(*) FROM person WHERE status='positive' GROUP BY report_date

2. Map all cases Waterloo region by zipcode.
SELECT zipcode, COUNT(*) FROM person WHERE status='positive' GROUP BY zipcode

3. Cases by age group and reported date

4. Cases by gender and reported date

5. Test positive rate in Waterloo region in the past week
WITH positive_num AS (
    SELECT COUNT(*) as count FROM person WHERE person.status='positive' AND person.report_date>='2021-08-21'
)
tested_num AS (
    SELECT COUNT(*) as count FROM person WHERE person.status='positive' OR person.status='negative' AND person.report_date>='2021-08-21'
)
SELECT positive_num.count / tested_num.count

4. Percentage of mutation and variant cases identified among all positive cases in Waterloo region.


5. Find high risk people to prioritize contact tracing
select a.id, count(c.id) * 10 + sum(pathogen.risk_level) as risk 
from (c:vPerson)<-[e0:eContact]-(a:vPerson)-[e1:eVisit]->(p:vPlace)<-[e2:eVisit]-(b:vPerson), zipcode, pathogen 
where a.status='unknown'and a.zipcode=zipcode.id and e1.visit_day=e2.visit_day and b.status='positive'and c.status='positive'and c.variant=pathogen.id 
group by a.id order by risk desc 
limit 100

select a.id, count(c.id) * 10 + sum(pathogen.risk_level) as risk 
from person a, person b, person c, contact c0, visit v1, visit v2, place pl, zipcode, pathogen 
where c.id=c0.p2id AND c0.p1id=a.id AND v1.personid=a.id AND v1.placeid=pl.id AND pl.id=v2.placeid AND v2.personid=b.id
AND a.status='unknown'and a.zipcode=zipcode.id and v1.visit_day=v2.visit_day and b.status='positive'and c.status='positive'and c.variant=pathogen.id 
group by a.id order by risk desc 
limit 100

6. Find 1-3 hop close contacts whose status are unknown (not trakced and not tested yet) of positive individuals within the week
select a, b from (a:vPerson)-[e:eContact*1..3]->(b:vPerson)
where a.status='positive' and a.report_date>='2021-08-22' and b.status='unknown'

WITH RECURSIVE contacts_cte AS(
    SELECT b.id as id, 1 as level
    FROM person a, contact e, person b
    WHERE a.status='positive'
    AND a.report_date>='2021-08-22'
    UNION
    SELECT b.id, level + 1
    FROM person b,contact e,contacts_cte
    WHERE contacts_cte.id = e.p1id
    AND b.id = e.p2id
    AND level <= 3)
SELECT b.*
FROM contacts_cte cte, person b
WHERE cte.id = b.id AND b.status='unknown';

We show the demonstration from the perspective of a software engineer, 
Alice, who is developing a reporting application for the Waterloo Public Health Services (WPHS) 
that manages the COVID-19 pandemic in the city of Waterloo.
WPHS maintains a database of people who have taken COVID-19 tests, 
as well as information that is gathered by the contact tracing system that is deployed in Waterloo
 that records the places people visited and the close contacts of people with COVID-19, as well as variant of concerns. 


WITH top_five_risky_regions AS (
  SELECT zipcode.id as zipcode, COUNT(*) num_cases 
  FROM person, zipcode 
  WHERE person.zipcode=zipcode.id 
  GROUP BY zipcode.id 
  ORDER BY num_cases DESC 
  LIMIT 5
)
SELECT a.id, SUM(pathogen.risk_level) as risk 
FROM (b:vPerson)<-[:eContact*1..3]-(a:vPerson), zipcode, pathogen 
WHERE a.status='unknown' AND b.status='positive' 
  AND b.variant=pathogen.id AND a.zipcode IN (
   SELECT zipcode FROM top_five_risky_regions )
GROUP BY a.id 
ORDER BY risk DESC 
LIMIT 100;
