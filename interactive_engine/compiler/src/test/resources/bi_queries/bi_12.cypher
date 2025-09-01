CALL {
  MATCH (p2:PERSON)
  WITH count(p2) as personCnt
  RETURN 0 as msgCnt, personCnt
}
UNION
CALL {
  MATCH (person:PERSON)<-[:HASCREATOR]-(message:COMMENT|POST),
        (message)-[:REPLYOF * 0..30]->(post:POST)
  WHERE message.length > $lengthThreshold AND message.creationDate > $startDate
        AND post.language IN ["a", "b"]
  WITH person, count(message) as msgCnt
  WITH msgCnt, count(person) as personCnt
  CALL {
    RETURN msgCnt, personCnt
  }
  UNION
  CALL {
    RETURN 0 as msgCnt, -1 * sum(personCnt) as personCnt
  }
  RETURN msgCnt, personCnt
}
RETURN msgCnt, sum(personCnt) as personCnt
ORDER BY
  personCnt DESC,
  msgCnt DESC