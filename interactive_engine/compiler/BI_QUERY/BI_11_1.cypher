:param country=> 'Laos';
:param startDate => 1275393600000;
:param endDate => 1277812800000;

MATCH (a:PERSON)-[:ISLOCATEDIN]->(:PLACE)-[:ISPARTOF]->(country:PLACE {name: $country}),
      (b)-[:ISLOCATEDIN]->(:PLACE)-[:ISPARTOF]->(country),
      (c)-[:ISLOCATEDIN]->(:PLACE)-[:ISPARTOF]->(country),
      (a:PERSON)-[k1:KNOWS]-(b:PERSON),
      (b:PERSON)-[k2:KNOWS]-(c:PERSON),
      (c:PERSON)-[k3:KNOWS]-(a:PERSON)
WHERE a.id < b.id
  AND b.id < c.id
  AND $startDate <= k1.creationDate AND k1.creationDate <= $endDate
  AND $startDate <= k2.creationDate AND k2.creationDate <= $endDate
  AND $startDate <= k3.creationDate AND k3.creationDate <= $endDate
WITH DISTINCT country, a, b
RETURN count(*) AS count;
