:param tag => 'Che_Guevara';
:param startDate => 1275393600000;
:param endDate => 1277812800000;

MATCH (tag:TAG {name: $tag})
// score
OPTIONAL MATCH (tag)<-[interest:HASINTEREST]-(person:PERSON)
OPTIONAL MATCH (tag)<-[:HASTAG]-(message)-[:HASCREATOR]->(person:PERSON)
WHERE $startDate < message.creationDate
      AND message.creationDate < $endDate
RETURN tag, count(person) AS totalCount;
