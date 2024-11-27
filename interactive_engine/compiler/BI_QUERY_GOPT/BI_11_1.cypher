:param country => "Laos";
:param startDate => 20100601000000000;
:param endDate => 20100630000000000;

MATCH (p1:PLACE)-[:ISPARTOF]->(country:PLACE {name: $country}),
      (p2:PLACE)-[:ISPARTOF]->(country),
      (p3:PLACE)-[:ISPARTOF]->(country)
WITH p1, p2, p3
MATCH (p3)<-[:ISLOCATEDIN]-(c:PERSON)-[k3:KNOWS]-(a:PERSON)-[:ISLOCATEDIN]->(p12)
WHERE $startDate <= k3.creationDate AND k3.creationDate <= $endDate
      AND p12 = p1
WITH a, c, p2
MATCH (b:PERSON)-[k2:KNOWS]-(c:PERSON)
WHERE $startDate <= k2.creationDate AND k2.creationDate <= $endDate
WITH b, a, c, p2
MATCH (a:PERSON)-[k1:KNOWS]-(b2:PERSON)
WHERE $startDate <= k1.creationDate AND k1.creationDate <= $endDate
      AND a.id < b.id
      AND b.id < c.id
      AND b2 = b
WITH a, b, p2
MATCH (b)-[:ISLOCATEDIN]->(p22)
WHERE p22 = p2
WITH DISTINCT a, b
RETURN count(*) AS count;
