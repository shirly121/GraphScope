MATCH 
  (person1:PERSON)-[:KNOWS]->(person2:PERSON), 
  (person1)<-[:HASCREATOR]-(comment:COMMENT)-[:REPLYOF]->(Post:POST)-[:HASCREATOR]->(person2) 
RETURN count(person1) AS count