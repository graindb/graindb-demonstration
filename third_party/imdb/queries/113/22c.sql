SELECT MIN(cn.name) AS movie_company,
       MIN(mi_idx.info) AS rating,
       MIN(t.title) AS western_violent_movie
FROM title AS t 
JOIN (movie_keyword AS mk JOIN keyword AS k ON k.id = mk.keyword_id) ON t.id = mk.movie_id
JOIN (movie_info AS mi JOIN info_type AS it1 ON it1.id = mi.info_type_id) ON t.id = mi.movie_id
JOIN (movie_companies AS mc JOIN company_name AS cn ON cn.id = mc.company_id JOIN company_type AS ct ON ct.id = mc.company_type_id) ON t.id = mc.movie_id
JOIN (movie_info_idx AS mi_idx JOIN info_type AS it2 ON it2.id = mi_idx.info_type_id) ON t.id = mi_idx.movie_id
JOIN kind_type AS kt ON kt.id = t.kind_id
WHERE cn.country_code != '[us]'
  AND it1.info = 'countries'
  AND it2.info = 'rating'
  AND k.keyword IN ('murder',
                    'murder-in-title',
                    'blood',
                    'violence')
  AND kt.kind IN ('movie',
                  'episode')
  AND mc.note NOT LIKE '%(USA)%'
  AND mc.note LIKE '%(200%)%'
  AND mi.info IN ('Sweden',
                  'Norway',
                  'Germany',
                  'Denmark',
                  'Swedish',
                  'Danish',
                  'Norwegian',
                  'German',
                  'USA',
                  'American')
  AND mi_idx.info < '8.5'
  AND t.production_year > 2005;

