MATCH (:TAG)<-[:HASTAG]-(message:POST|COMMENT)-[:HASCREATOR]->(creator:PERSON), 
  (message)<-[:LIKES]-(liker:PERSON), 
  (message)<-[:REPLYOF]-(comment:COMMENT) 
RETURN count(message) AS count