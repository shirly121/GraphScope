// union type join order
:param name => "Laos";

Match (c:PLACE {name: $name})<-[:ISLOCATEDIN]-(p1:PERSON),
      (c)<-[:ISLOCATEDIN]-(p2:PERSON),
      (p1)<-[:HASCREATOR]-(m1:COMMENT)<-[:LIKES]->(p2:PERSON)
WITH c
MATCH
       (c:PLACE {name: $name})<-[:ISLOCATEDIN]-(p3:PERSON),
       (c)<-[:ISLOCATEDIN]-(p4:PERSON),
       (p3)-[:KNOWS|HASMODERATOR]-(m2:FORUM|PERSON)-[:KNOWS|HASMODERATOR]-(p4:PERSON)
RETURN count(c)