MATCH (person2:PERSON)<-[:HASMODERATOR]-(place:FORUM),
      (message:COMMENT|POST)-[:HASCREATOR]->(person2:PERSON)
WITH message, place
MATCH (message:COMMENT|POST)<-[:LIKES]-(person1:PERSON)<-[:HASMODERATOR]-(place:FORUM)
RETURN count(message)