// glogs intersect order

Match (c:PLACE)<-[:ISLOCATEDIN]-(p1:PERSON),
      (c)<-[:ISLOCATEDIN]-(p2:PERSON)
WITH p1, p2
MATCH (p1)<-[:HASCREATOR]-(m1:COMMENT)<-[:LIKES]->(p2:PERSON)
RETURN count(p1);