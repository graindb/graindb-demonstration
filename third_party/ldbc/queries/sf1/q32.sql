/* bi-11 */
WITH replies_w_tags AS (
    SELECT p.p_personid  AS creatorid
         , r.m_messageid AS replyid
         , r.m_c_replyof AS replyof
         , r.m_content   AS content
         --, CASE
         -- WHEN count(pt.mt_tagid)=0 THEN ARRAY[]::bigint[] -- no tags for the message, so we return an empty array
         -- ELSE array_agg(pt.mt_tagid)
         -- END AS replyTags
    FROM place co -- country
       , place ci -- city
       , person p
       , message r -- reply
             LEFT JOIN message_tag pt ON (r.m_messageid = pt.mt_messageid)
    WHERE 1 = 1
      -- join
      AND co.pl_placeid = ci.pl_containerplaceid
      AND ci.pl_placeid = p.p_placeid
      AND p.p_personid = r.m_creatorid
      -- filter
      AND co.pl_name = 'Germany'
      AND r.m_c_replyof IS NOT NULL
      -- exclude messages by the blacklist.
      -- Note: we use string_to_array(trim(regexp_replace(...))) instead regexp_split_to_array to avoid translating "Foo." into {Foo,""},
      -- i.e. remove possible empty firts/last elements
      --AND NOT string_to_array(trim(regexp_replace(r.m_content, E'[[:space:],.?!()\r\n]+', ' ', 'g')), ' ') && 'one';'has';'David'
    GROUP BY p.p_personid, r.m_messageid, r.m_c_replyof, r.m_content
)
-- blacklisting is done after the joins and country filter above as it tured out to be an expensive operation (in 1st option)
-- first blacklisting option is to tokenize message as words and use word-based search
   , replies_blacklist_excluded_1 AS (
    SELECT *
    FROM replies_w_tags
    WHERE 1 = 1
      -- filter
      -- exclude messages by the blacklist.
      -- Note: we use string_to_array(trim(regexp_replace(...))) instead regexp_split_to_array to avoid translating "Foo." into {Foo,""},
      -- i.e. remove possible empty firts/last elements
      AND NOT string_to_array(trim(regexp_replace(content, E '[[:space:],.?!()\r\n]+', ' ', 'g')), ' ')
    && '{one, has, David}')
-- second blacklisting option is done using pure string contains
   , replies_blacklist_excluded_2 AS (
SELECT *
FROM replies_w_tags r
    LEFT JOIN unnest('{one, has, David}') AS bl(word)
ON (r.content like '%'||bl.word||'%')
WHERE 1=1
-- exclude messages by the blacklist.
  AND bl.word IS NULL
    )
    , replies_not_sharing_tags_w_base_message AS (
SELECT r.replyid
        , r.creatorid
FROM replies_blacklist_excluded_2 r
        , message b -- base message of the reply
    LEFT JOIN message_tag pt
ON (b.m_messageid = pt.mt_messageid)
WHERE 1=1
    -- join
  AND r.replyof = b.m_messageid
    -- filter
GROUP BY r.replyid, r.creatorid, r.replyTags
HAVING NOT r.replyTags --&&
    --CASE
    --WHEN count(pt.mt_tagid)=0 THEN ARRAY[]::bigint[] -- no tags for the message, so we return an empty array
    --ELSE array_agg(pt.mt_tagid)
    --END
    )
SELECT r.creatorid               AS "person.id"
     , t.t_name                  AS "tag.name"
     , count(l.l_messageid)      as likeCount
     , count(DISTINCT r.replyid) as replyCount
     --, array_agg(DISTINCT r.replyid ORDER BY r.replyid) AS affectedReplyIds -- for debugging purposes
FROM replies_not_sharing_tags_w_base_message r
         LEFT JOIN likes l ON (r.replyid = l.l_messageid)
   , message_tag pt
   , tag t
WHERE 1 = 1
  -- join
  AND r.replyid = pt.mt_messageid
  AND pt.mt_tagid = t.t_tagid
GROUP BY r.creatorid, t.t_name
ORDER BY likeCount DESC, r.creatorid, t.t_name LIMIT 100
;
