:param personId => 933;
:param minDate => 20101101080000000;

MATCH (person:PERSON { id: $personId })-[:KNOWS*1..3]-(friend),
      (friend)<-[membership:HASMEMBER]-(forum),
      (post)<-[:CONTAINEROF]-(forum)
WHERE
    friend <> person
    AND membership.joinDate > $minDate
WITH forum, friend, post
MATCH (friend)<-[:HASCREATOR]-(post1)
WHERE post1 = post
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
