MATCH (country:COUNTRY),
 (person1:PERSON)-[:ISLOCATEDIN]->(city1:CITY)-[:ISPARTOF]->(country),
 (person2:PERSON)-[:ISLOCATEDIN]->(city2:CITY)-[:ISPARTOF]->(country),
 (person3:PERSON)-[:ISLOCATEDIN]->(city3:CITY)-[:ISPARTOF]->(country),
 (person1)-[:KNOWS]->(person2)-[:KNOWS]->(person3),
 (person3)-[:KNOWS]->(person1) 
RETURN count(country) AS count