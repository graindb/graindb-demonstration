/* bi-14 */
WITH RECURSIVE post_all AS (
    SELECT m_messageid    AS psa_threadid
         , m_creatorid    AS psa_thread_creatorid
         , m_messageid    AS psa_messageid
         , m_creationdate AS psa_creationdate
         , 'Post'         AS psa_messagetype
    FROM message
    WHERE 1 = 1
      AND m_c_replyof IS NULL -- post, not comment
      AND m_creationdate BETWEEN epoch_ms(1288584000000) AND epoch_ms(1291179600000)
    UNION ALL
    SELECT psa.psa_threadid         AS psa_threadid
         , psa.psa_thread_creatorid AS psa_thread_creatorid
         , m_messageid
         , m_creationdate
         , 'Comment'
    FROM message p
       , post_all psa
    WHERE 1 = 1
      AND p.m_c_replyof = psa.psa_messageid
      -- this is a performance optimisation only
      AND m_creationdate BETWEEN epoch_ms(1288584000000) AND epoch_ms(1291179600000)
)
SELECT p.p_personid                      AS "person.id"
     , p.p_firstname                     AS "person.firstName"
     , p.p_lastname                      AS "person.lastName"
     , count(DISTINCT psa.psa_threadid)  AS threadCount
     -- if the thread initiator message does not count as a reply
     --, count(DISTINCT CASE WHEN psa.psa_messagetype = 'Comment' then psa.psa_messageid ELSE null END) AS messageCount
     , count(DISTINCT psa.psa_messageid) AS messageCount
FROM person p
         left join post_all psa on (
        1 = 1
        AND p.p_personid = psa.psa_thread_creatorid
        AND psa_creationdate BETWEEN epoch_ms(1288584000000) AND epoch_ms(1291179600000)
    )
GROUP BY p.p_personid, p.p_firstname, p.p_lastname
ORDER BY messageCount DESC, p.p_personid LIMIT 100
;
