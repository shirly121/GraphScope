:param languages => ['ar', 'hu'];
:param lengthThreshold => 20;
:param startDate => 20100630000000000;

MATCH (person:PERSON)<-[:HASCREATOR]-(message),
      (message)-[:REPLYOF * 0..6]->(post:POST)
WHERE message.length < $lengthThreshold
      AND message.creationDate > $startDate
      AND post.language IN $languages
WITH person, count(message) as msgCnt
RETURN msgCnt, count(person) as personCnt
ORDER BY
  personCnt DESC,
  msgCnt DESC;
