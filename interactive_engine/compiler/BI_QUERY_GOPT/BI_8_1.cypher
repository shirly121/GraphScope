:param tag => "Che_Guevara";
:param startDate => 20100601000000000;
:param endDate => 20100630000000000;

MATCH (tag:TAG {name: $tag})
OPTIONAL MATCH (tag)<-[interest:HASINTEREST]-(person:PERSON)
OPTIONAL MATCH (tag)<-[:HASTAG]-(message)-[:HASCREATOR]->(person:PERSON)
WHERE message IS NULL OR $startDate < message.creationDate
      AND message.creationDate < $endDate
RETURN tag, count(person) AS totalCount;
