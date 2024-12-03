:param personId => 933;
:param firstName => "Mikhail";

MATCH (p: PERSON{id: $personId})-[k:KNOWS*1..4]-(f: PERSON {firstName: $firstName})
MATCH (f: PERSON)-[:ISLOCATEDIN]->(locationCity:PLACE)
RETURN count(p);