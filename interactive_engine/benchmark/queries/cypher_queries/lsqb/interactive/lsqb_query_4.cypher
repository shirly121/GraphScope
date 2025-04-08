MATCH (:TAG)<-[:HASTAG]-(message:POST|COMMENT)-[:HASCREATOR]->(:PERSON), 
  (message)<-[:LIKES]-(:PERSON), 
  (message)<-[:REPLYOF]-(:COMMENT) 
RETURN count(message) AS count