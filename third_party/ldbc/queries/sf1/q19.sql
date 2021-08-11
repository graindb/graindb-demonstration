/* interactive-complex-12 */
with recursive extended_tags as (
    select tc_tagclassid as s_subtagclassid, tc_tagclassid as s_supertagclassid
    from tagclass
    UNION
    select tc.tc_tagclassid, t.s_supertagclassid
    from tagclass tc,
         extended_tags t
    where tc.tc_subclassoftagclassid = t.s_subtagclassid
)
select p_personid, p_firstname, p_lastname, array_agg(distinct t_name), count(*)
from person,
     message p1,
     knows,
     message p2,
     message_tag,
     (select distinct t_tagid, t_name
      from tag
      where (t_tagclassid in (
          select distinct s_subtagclassid
          from extended_tags k,
               tagclass
          where tc_tagclassid = k.s_supertagclassid
            and tc_name = 'BasketballPlayer')
                )) selected_tags
where k_person1id = 15393162793684
  and k_person2id = p_personid
  and p_personid = p1.m_creatorid
  and p1.m_c_replyof = p2.m_messageid
  and p2.m_c_replyof is null
  and p2.m_messageid = mt_messageid
  and mt_tagid = t_tagid
group by p_personid, p_firstname, p_lastname
order by 5 desc, 1 limit 20
;
