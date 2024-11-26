:param tag => "Che_Guevara";
:param startDate => 20100601000000000;
:param endDate => 20100630000000000;

CALL {
  MATCH (tag:TAG {name: $tag})<-[interest:HASINTEREST]-(person:PERSON)
  RETURN tag, count(person) as totalCount
}
UNION
CALL {
  MATCH (tag:TAG {name: $tag})<-[:HASTAG]-(message)-[:HASCREATOR]->(person:PERSON)
  WHERE $startDate < message.creationDate
      AND message.creationDate < $endDate
  RETURN tag, count(person) as totalCount                                
}
RETURN tag, sum(totalCount) AS totalCount;
