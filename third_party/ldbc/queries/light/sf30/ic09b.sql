/* interactive-complex-9 */
select p2.p_firstname, p2.p_lastname, m_creationdate
from person p1 JOIN (
    knows k1 JOIN (
        knows k2 JOIN (
            person p2 JOIN message ON p2.p_personid=m_creatorid)
            ON k2.k_person2id=p2.p_personid)
        ON k1.k_person2id=k2.k_person1id)
    ON p1.p_personid=k1.k_person1id
where p1.id=6597069890045
    and m_creationdate < '2012-04-24 00:00:00'
;