CALL {
  MATCH (person:PERSON)<-[:HASCREATOR]-(msg:COMMENT|POST)-[:HASTAG]->(tag:TAG {name:$tagA})
  WHERE gs.function.date32(msg.creationDate) = $dateA
  WITH person, count(msg) as aCount

  CALL {
    RETURN person, aCount, -1 * $maxKnows as degree
  }
  UNION
  CALL {
    MATCH (person)-[:KNOWS]-(person2:PERSON)
    RETURN person2 as person, 0 as aCount, count(person) as degree
  }

  WITH person, sum(aCount) as aCount, sum(degree) as degree
  WHERE degree <= 0
  RETURN person, aCount, 0 as bCount
}
UNION
CALL {
  MATCH (person:PERSON)<-[:HASCREATOR]-(msg:COMMENT|POST)-[:HASTAG]->(tag:TAG {name:$tagB})
  WHERE gs.function.date32(msg.creationDate) = $dateB
  WITH person, count(msg) as bCount

  CALL {
    RETURN person, bCount, -1 * $maxKnows as degree
  }
  UNION
  CALL {
    MATCH (person)-[:KNOWS]-(person2:PERSON)
    RETURN person2 as person, 0 as bCount, count(person) as degree
  }

  WITH person, sum(bCount) as bCount, sum(degree) as degree
  WHERE degree <= 0
  RETURN person, 0 as aCount, bCount
}
Return person.id as id, sum(aCount) as aCount, sum(bCount) as bCount
ORDER BY aCount + bCount DESC, id ASC
LIMIT 20;