SELECT MIN(cn.name) AS movie_company,
       MIN(mi_idx.info) AS rating,
       MIN(t.title) AS mainstream_movie
FROM info_type AS it1 JOIN 
(movie_info AS mi JOIN 
  (title AS t JOIN 
    (movie_companies AS mc JOIN company_type AS ct ON ct.id = mc.company_type_id JOIN company_name AS cn ON cn.id = mc.company_id) 
    ON t.id = mc.movie_id JOIN (movie_info_idx AS mi_idx JOIN info_type AS it2 ON mi_idx.info_type_id = it2.id) 
    ON t.id = mi_idx.movie_id) 
  ON t.id = mi.movie_id) 
ON mi.info_type_id = it1.id
WHERE cn.country_code = '[us]'
  AND ct.kind = 'production companies'
  AND it1.info = 'genres'
  AND it2.info = 'rating'
  AND mi.info IN ('Drama',
                  'Horror',
                  'Western',
                  'Family')
  AND mi_idx.info > '7.0'
  AND t.production_year BETWEEN 2000 AND 2010;

