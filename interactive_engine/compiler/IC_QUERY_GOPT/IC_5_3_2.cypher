:param personId => 933;
:param minDate => 20101101080000000;

MATCH (person:PERSON { id: $personId })-[:KNOWS*1..3]-(friend),
      (friend)<-[:HASCREATOR]-(post)<-[:CONTAINEROF]-(forum)
WHERE
    NOT friend.id = $personId
WITH friend, forum, post
MATCH (friend)<-[membership:HASMEMBER]-(forum1)
WHERE membership.joinDate > $minDate AND forum1 = forum
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
