MATCH (person2:FORUM)-[:HASMODERATOR|HASTAG]->(place:PERSON|TAG),
      (message:POST)<-[:CONTAINEROF]-(person2:FORUM)
WITH message, place
MATCH (message:POST)<-[:LIKES]-(person1:PERSON)-[:KNOWS|HASINTEREST]->(place:PERSON|TAG)
RETURN count(message);