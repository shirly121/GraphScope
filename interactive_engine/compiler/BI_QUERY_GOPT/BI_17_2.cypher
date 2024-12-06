:param tag => 'Wendy_Turnbull';

MATCH
    (comment:COMMENT)-[:HASTAG]->(tag:TAG {name: $tag}),
    (comment:COMMENT)-[:REPLYOF]->(message2:COMMENT),
    (message2:COMMENT)-[:HASTAG]->(tag:TAG),
    (message1:COMMENT)-[:HASTAG]->(tag:TAG {name: $tag}),
    (message1:COMMENT)-[:REPLYOF]->(post1:POST)<-[:CONTAINEROF]-(forum1:FORUM),
    (forum1:FORUM)-[:HASMEMBER]->(person3:PERSON)<-[:HASCREATOR]-(message2:COMMENT)
RETURN count(*);
