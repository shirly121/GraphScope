MATCH (person1:PERSON)-[:KNOWS]->(person2:PERSON)-[:KNOWS]->(person3:PERSON)-[:HASINTEREST]->(tag:TAG) 
WHERE person1 <> person3 
RETURN count(person1) AS count