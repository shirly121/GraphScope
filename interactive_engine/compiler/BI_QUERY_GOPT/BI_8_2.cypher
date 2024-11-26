:param tag => 'Che_Guevara';
:param startDate => 1275393600000;
:param endDate => 1277812800000;

CALL {
  MATCH (tag:TAG {name: $tag})<-[interest:HASINTEREST]-(person:PERSON)
  RETURN tag, count(person) as totalCount
UNION
  MATCH (tag:TAG {name: $tag})<-[:HASTAG]-(message)-[:HASCREATOR]->(person:PERSON)
  WHERE $startDate < message.creationDate
      AND message.creationDate < $endDate
  RETURN tag, count(person) as totalCount                                
}
RETURN tag, sum(totalCount) AS totalCount;
