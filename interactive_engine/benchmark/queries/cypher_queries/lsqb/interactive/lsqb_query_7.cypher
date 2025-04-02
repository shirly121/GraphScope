MATCH (:TAG)<-[:HASTAG]-(message:POST|COMMENT)-[:HASCREATOR]->(creator:PERSON) 
OPTIONAL MATCH (message)<-[:LIKES]-(liker:PERSON) 
OPTIONAL MATCH (message)<-[:REPLYOF]-(comment:COMMENT) 
RETURN count(comment) AS count