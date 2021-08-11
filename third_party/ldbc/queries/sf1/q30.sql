/* bi-9 */
WITH popular_forums AS (
    SELECT fp_forumid as forumid
    FROM forum_person
    GROUP BY fp_forumid
    HAVING count(*) > 200
)
SELECT f.f_forumid                    AS "forum.id"
     , count(DISTINCT p1.m_messageid) AS count1
     , count(DISTINCT p2.m_messageid) AS count2
FROM tagclass tc1
   , tag t1
   , message_tag pt1
   , message p1
   , tagclass tc2
   , tag t2
   , message_tag pt2
   , message p2
   , forum f
   , popular_forums pf
WHERE 1 = 1
  -- join
  -- tagClass1 to forum
  AND tc1.tc_tagclassid = t1.t_tagclassid
  AND t1.t_tagid = pt1.mt_tagid
  AND pt1.mt_messageid = p1.m_messageid
  AND p1.m_ps_forumid = f.f_forumid
  AND f.f_forumid = pf.forumid
  -- tagClass2 to forum
  AND tc2.tc_tagclassid = t2.t_tagclassid
  AND t2.t_tagid = pt2.mt_tagid
  AND pt2.mt_messageid = p2.m_messageid
  AND p2.m_ps_forumid = f.f_forumid
  -- filter
  AND tc1.tc_name = 'ChristianBishop'
  AND tc2.tc_name = 'BaseballPlayer'
  AND p1.m_c_replyof IS NULL
  AND p2.m_c_replyof IS NULL
GROUP BY f.f_forumid
ORDER BY abs(count(DISTINCT p2.m_messageid) - count(DISTINCT p1.m_messageid)) DESC, f.f_forumid LIMIT 100
;
