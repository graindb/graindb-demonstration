/* bi-10 */
WITH person_tag_interest AS (
    SELECT p.p_personid AS personid
    FROM person p
       , person_tag pt
       , tag t
    WHERE 1 = 1
      -- join
      AND p.p_personid = pt.pt_personid
      AND pt.pt_tagid = t.t_tagid
      -- filter
      AND t.t_name = 'Paradorn_Srichaphan'
)
   , person_message_score AS (
    SELECT p.p_personid AS personid
         , count(*)     AS message_score
    FROM message m
       , person p
       , message_tag pt
       , tag t
    WHERE 1 = 1
      -- join
      AND m.m_creatorid = p.p_personid
      AND m.m_messageid = pt.mt_messageid
      AND pt.mt_tagid = t.t_tagid
      -- filter
      AND m.m_creationdate > epoch_ms(1321333200000)
      AND t.t_name = 'Paradorn_Srichaphan'
    GROUP BY p.p_personid
)
   , person_score AS (
    SELECT coalesce(pti.personid, pms.personid) AS personid
         , CASE WHEN pti.personid IS NULL then 0 ELSE 100 END -- scored from interest in the given tag
        + coalesce(pms.message_score, 0)        AS score
    FROM person_tag_interest pti
             FULL JOIN person_message_score pms ON (pti.personid = pms.personid)
)
SELECT p.personid   AS "person.id"
     , p.score      AS score
     , sum(f.score) AS friendsScore
FROM person_score p
   , knows k
   , person_score f -- the friend
WHERE 1 = 1
  -- join
  AND p.personid = k.k_person1id
  AND k.k_person2id = f.personid
GROUP BY p.personid, p.score
ORDER BY p.score + sum(f.score) DESC, p.personid LIMIT 100
;
