MATCH (person1:PERSON)-[:KNOWS]->(person2:PERSON)-[:KNOWS]->(person3:PERSON)-[:HASINTEREST]->(tag:TAG) 
  WHERE NOT (person1)-[:KNOWS]->(person3) AND person1 <> person3
RETURN count(person1) AS count