:param personId => 933;

MATCH (p: PERSON)-[k:KNOWS]-(f: PERSON)
WITH f
LIMIT 1000
JOIN MATCH (f:PERSON)-[:ISLOCATEDIN]->(locationCity:PLACE)
RETURN count(f);