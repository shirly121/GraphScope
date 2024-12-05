// gopt intersect order

Match (c:PLACE)<-[:ISLOCATEDIN]-(p1:PERSON),
      (c)<-[:ISLOCATEDIN]-(p2:PERSON),
      (p1)<-[:HASCREATOR]-(m1:COMMENT)<-[:LIKES]->(p2:PERSON)
RETURN count(c);