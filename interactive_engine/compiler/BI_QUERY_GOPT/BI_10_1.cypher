:param personId => 933;
:param country => "India";
:param tagClass => "Person";

MATCH (p1:PERSON {id : $personId})-[:KNOWS*1..4]-(expert:PERSON)
WITH expert
MATCH (expert)-[:ISLOCATEDIN]->(:PLACE)-[:ISPARTOF]->(country:PLACE {name: $country})
EXPAND MATCH (expert)<-[:HASCREATOR]-(message)-[:HASTAG]->(:TAG)-[:HASTYPE]->(:TAGCLASS {name: $tagClass})
WITH DISTINCT expert, message
MATCH (message)-[:HASTAG]->(tag:TAG)
RETURN
  expert.id as id,
  tag.name as name,
  count(message) AS messageCount
ORDER BY
  messageCount DESC,
  name ASC,
  id ASC
LIMIT 100;
