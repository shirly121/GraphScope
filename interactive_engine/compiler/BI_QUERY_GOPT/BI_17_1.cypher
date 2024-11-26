:param tag => "Meryl_Streep";

MATCH
  (comment)-[:HASTAG]->(tag:TAG {name: $tag}),
  (message2)-[:HASTAG]->(tag),
  (comment)-[:REPLYOF]->(message2)
WITH tag, message2
EXPAND MATCH (person3:PERSON)<-[:HASCREATOR]-(message2)
WITH tag, person3
EXPAND MATCH (tag)<-[:HASTAG]-(message1)-[:REPLYOF*0..10]->(post1:POST)<-[:CONTAINEROF]-(forum1:FORUM)-[:HASMEMBER]->(person32)
WHERE person32 = person3
RETURN count(*);
