MATCH (country:COUNTRY),
 (person1:PERSON)-[:ISLOCATEDIN]->(:CITY)-[:ISPARTOF]->(country),
 (person2:PERSON)-[:ISLOCATEDIN]->(:CITY)-[:ISPARTOF]->(country),
 (person3:PERSON)-[:ISLOCATEDIN]->(:CITY)-[:ISPARTOF]->(country),
 (person1)-[:KNOWS]-(person2)-[:KNOWS]-(person3),
 (person3)-[:KNOWS]-(person1) 
RETURN count(country) AS count