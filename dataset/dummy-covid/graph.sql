CREATE VERTEX vPerson ON person;
CREATE VERTEX vPlace ON place;
CREATE EDGE eContact ON contact (FROM vPerson REFERENCES p1id, TO vPerson REFERENCES p2id);
CREATE EDGE eVisit ON visit (FROM vPerson REFERENCES personid, TO vPlace REFERENCES placeid);