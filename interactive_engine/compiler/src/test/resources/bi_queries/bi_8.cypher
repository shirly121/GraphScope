MATCH (tag:TAG {name: $tag})
CALL {
  OPTIONAL MATCH (tag)<-[interest:HASINTEREST]-(person:PERSON)
  RETURN person, count(tag) as cnt1, 0 as cnt2
}
UNION
CALL {
  MATCH (tag)<-[:HASTAG]-(message:POST|COMMENT)
  OPTIONAL MATCH (message)-[:HASCREATOR]->(person:PERSON)
  WHERE $startDate < message.creationDate AND message.creationDate < $endDate
  RETURN person, 0 as cnt1, count(tag) as cnt2
}
WITH person, sum(cnt1) * 100 + sum(cnt2) as score
CALL {
  MATCH (person)-[:KNOWS]-(person2:PERSON)
  RETURN person2 as person, 0 as score, sum(score) as friendScore
}
UNION
CALL {
  RETURN person, score, 0 as friendScore
}
RETURN person.id as id, sum(score) as score, sum(friendScore) as friendScore
ORDER BY
  score + friendScore DESC,
  id ASC
LIMIT 100