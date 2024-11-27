:param personId => 933;
:param tagName => "North_German_Confederation";

MATCH (p_:PERSON {id: $personId})-[:KNOWS*1..3]-(other:PERSON)
WHERE other <> p_
WITH distinct other

MATCH (other)<-[:HASCREATOR]-(p:POST)-[:HASTAG]->(t:TAG {name: $tagName})

Match (p:POST)-[:HASTAG]->(otherTag:TAG)
WHERE
    otherTag <> t

WITH DISTINCT
      otherTag,
      p

RETURN
    otherTag.name as name,
    count(p) as postCnt
ORDER BY
    postCnt desc,
    name asc
LIMIT 10;
