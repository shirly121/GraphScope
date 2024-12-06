:param tag => 'Wendy_Turnbull';

MATCH (comment:COMMENT)-[:REPLYOF]->(message2:COMMENT)-[:HASTAG]->(tag:TAG {name: $tag})
WITH comment, tag, message2
EXPAND MATCH (comment:COMMENT)-[:HASTAG]->(tag3:TAG)
WHERE tag3 = tag
WITH tag, message2
EXPAND MATCH (tag2:TAG)<-[:HASTAG]-(message1)-[:REPLYOF]->(post1:POST)<-[:CONTAINEROF]-(forum1:FORUM)-[:HASMEMBER]->(person3:PERSON)<-[:HASCREATOR]-(message2:COMMENT)
WHERE tag2 = tag
RETURN count(*);