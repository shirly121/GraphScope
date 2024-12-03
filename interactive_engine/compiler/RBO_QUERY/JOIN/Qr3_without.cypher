:param personId => 933;
:param firstName => "Mikhail";

MATCH (p: PERSON{id: $personId})-[k:KNOWS*1..4]-(f: PERSON {firstName: $firstName})
WITH f, p
JOIN MATCH (f: PERSON {firstName: $firstName})-[:ISLOCATEDIN]->(locationCity:PLACE)
RETURN count(p);