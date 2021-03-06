SELECT k.keyword AS movie_keyword,
       n.name AS actor_name,
       t.title AS marvel_movie
FROM title AS t JOIN (movie_keyword AS mk JOIN keyword AS k ON k.id = mk.keyword_id) ON t.id = mk.movie_id
JOIN (cast_info AS ci JOIN name AS n ON n.id = ci.person_id) ON t.id = ci.movie_id
WHERE k.keyword = 'marvel-cinematic-universe'
  AND n.name LIKE '%Downey%Robert%'
  AND t.production_year > 2010;

