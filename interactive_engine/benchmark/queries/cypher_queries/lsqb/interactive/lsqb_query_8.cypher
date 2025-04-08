MATCH (tag1:TAG)<-[:HASTAG]-(:POST|COMMENT)<-[:REPLYOF]-(comment:COMMENT)-[:HASTAG]->(tag2:TAG) 
WHERE NOT (comment)-[:HASTAG]->(tag1) AND tag1 <> tag2 
RETURN count(tag1) AS count