MATCH 
  (person1:PERSON)-[:KNOWS]->(person2:PERSON), 
  (person1)<-[:HASCREATOR]-(:COMMENT)-[:REPLYOF]->(:POST)-[:HASCREATOR]->(person2) 
RETURN count(person1) AS count