MATCH (tag1:TAG)<-[:HASTAG]-(message:COMMENT|POST)<-[:REPLYOF]-(comment:COMMENT)-[:HASTAG]->(tag2:TAG) 
WHERE tag1 <> tag2 
RETURN count(tag1) AS count