:param country1 => 'Laos';
:param country2 => 'India';

MATCH
   (country1:PLACE {name: $country1})<-[:ISPARTOF]-(city1:PLACE)<-[:ISLOCATEDIN]-(person1:PERSON),
   (country2:PLACE {name: $country2})<-[:ISPARTOF]-(city2:PLACE)<-[:ISLOCATEDIN]-(person2:PERSON),
   (person1)-[knows:KNOWS]-(person2)
// Match1
MATCH (person1)<-[:HASCREATOR]-(c:COMMENT)-[:REPLYOF]->()-[:HASCREATOR]->(person2:PERSON)
WITH DISTINCT person1, person2, city1, 4 as score1 // Aggregate1
// Match2
MATCH (person1)-[:LIKES]->(m)-[:HASCREATOR]->(person2)
WITH DISTINCT person1, person2, city1, score1, 10 as score2 // Aggreate2
RETURN count(*);
