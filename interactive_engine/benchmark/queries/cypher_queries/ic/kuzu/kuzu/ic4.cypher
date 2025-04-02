MATCH (person:PERSON {id: 10995116278874})-[:PERSON_KNOWS_PERSON]-(friend:PERSON)<-[:POST_HASCREATOR_PERSON]-(post:POST)-[:POST_HASTAG_TAG]->(tag: TAG)
WITH tag,
     CASE
       WHEN  post.creationDate >= 1338508800000 AND post.creationDate < 1340928000000 THEN 1
       ELSE 0
     END AS valid,
     CASE
       WHEN post.creationDate < 1338508800000  THEN 1
       ELSE 0
     END AS inValid
WITH tag, sum(valid) AS postCount, sum(inValid) AS inValidPostCount
WHERE postCount>0 AND inValidPostCount=0

RETURN tag.name AS tagName, postCount
ORDER BY postCount DESC, tagName ASC
LIMIT 10;