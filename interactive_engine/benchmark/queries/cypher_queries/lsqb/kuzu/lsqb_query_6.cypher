MATCH (person1:PERSON)-[:PERSON_KNOWS_PERSON]->(person2:PERSON)-[:PERSON_KNOWS_PERSON]->(person3:PERSON)-[:PERSON_HASINTEREST_TAG]->(tag:TAG) 
WHERE id(person1) <> id(person3) 
RETURN count(person1) AS count