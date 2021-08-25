COPY person FROM './dataset/dummy-covid/person.csv' WITH HEADER DELIMITER '|';
COPY place FROM './dataset/dummy-covid/place.csv' WITH HEADER DELIMITER '|';
COPY pathogen FROM './dataset/dummy-covid/pathogen.csv' WITH HEADER DELIMITER '|';
COPY contact FROM './dataset/dummy-covid/contacts.csv' WITH HEADER DELIMITER '|';
COPY visit FROM './dataset/dummy-covid/visit.csv' WITH HEADER DELIMITER '|';
COPY zipcode FROM './dataset/dummy-covid/zipcode.csv' WITH HEADER DELIMITER '|';

COPY person FROM '/home/guodong/Developer/graindb-private/dataset/dummy-covid/person.csv' WITH HEADER DELIMITER '|';
COPY place FROM '/home/guodong/Developer/graindb-private/dataset/dummy-covid/place.csv' WITH HEADER DELIMITER '|';
COPY pathogen FROM '/home/guodong/Developer/graindb-private/dataset/dummy-covid/pathogen.csv' WITH HEADER DELIMITER '|';
COPY contact FROM '/home/guodong/Developer/graindb-private/dataset/dummy-covid/contacts.csv' WITH HEADER DELIMITER '|';
COPY visit FROM '/home/guodong/Developer/graindb-private/dataset/dummy-covid/visit.csv' WITH HEADER DELIMITER '|';
COPY zipcode FROM '/home/guodong/Developer/graindb-private/dataset/dummy-covid/zipcode.csv' WITH HEADER DELIMITER '|';
