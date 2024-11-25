:param personId => 933;
:param startDate => 20100601000000000;
:param endDate => 20100630000000000;
// :param startDate => 1275350400000;
// :param endDate => 1277856000000;

MATCH (person:PERSON {id: $personId})-[:KNOWS]-(friend:PERSON)<-[:HASCREATOR]-(post:POST)-[:HASTAG]->(tag: TAG)
WITH DISTINCT tag, post
WITH tag,
     CASE
       WHEN post.creationDate < $endDate  AND post.creationDate >= $startDate THEN 1
       ELSE 0
     END AS valid,
     CASE
       WHEN $startDate > post.creationDate THEN 1
       ELSE 0
     END AS inValid
WITH tag, sum(valid) AS postCount, sum(inValid) AS inValidPostCount
WHERE postCount>0 AND inValidPostCount=0

RETURN tag.name AS tagName, postCount
ORDER BY postCount DESC, tagName ASC
LIMIT 10;
