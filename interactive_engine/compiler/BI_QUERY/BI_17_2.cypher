:param tag => 'Meryl_Streep';

MATCH
  (comment)-[:HASTAG]->(tag:TAG {name: $tag}),
  (comment)-[:REPLYOF]->(message2),
  (message2)-[:HASTAG]->(tag)
MATCH (tag)<-[:HASTAG]-(message1)-[:REPLYOF*0..10]->(post1:POST)<-[:CONTAINEROF]-(forum1:FORUM)
WITH DISTINCT message2, forum1
MATCH (message2)-[:HASCREATOR]->(person3)<-[:HASMEMBER]-(forum1)
RETURN count(*);
