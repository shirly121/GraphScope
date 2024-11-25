:param personId => 933;
:param minDate => 20101101080000000;
// :param minDate => 1288612800000;
// :param minDate => "2011-01-02T06:43:51.955+0000";

MATCH (person:PERSON { id: $personId })-[:KNOWS*1..3]-(friend),
      (friend)<-[membership:HASMEMBER]-(forum),
      (friend)<-[:HASCREATOR]-(post)<-[:CONTAINEROF]-(forum)
WHERE
    NOT friend.id = $personId
    AND membership.joinDate > $minDate
WITH
    forum,
    count(distinct post) AS postCount
ORDER BY
    postCount DESC,
    forum.id ASC
LIMIT 20
RETURN
    forum.title AS forumName,
    postCount;
