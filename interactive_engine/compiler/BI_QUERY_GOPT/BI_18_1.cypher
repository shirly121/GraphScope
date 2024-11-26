:param tag => "Meryl_Streep";

MATCH (tag:TAG {name: $tag})<-[:HASINTEREST]-(person1:PERSON)-[:KNOWS]-(mutualFriend:PERSON),
      (mutualFriend:PERSON)-[:KNOWS]-(person2:PERSON)
WHERE person1 <> person2
  AND NOT (person1)-[:KNOWS]-(person2)
WITH person1, person2, mutualFriend
MATCH (person2:PERSON)-[:HASINTEREST]->(tag2:TAG {name: $tag})
RETURN person1.id AS person1Id, person2.id AS person2Id, count(DISTINCT mutualFriend) AS mutualFriendCount
ORDER BY mutualFriendCount DESC, person1Id ASC, person2Id ASC
LIMIT 20;
