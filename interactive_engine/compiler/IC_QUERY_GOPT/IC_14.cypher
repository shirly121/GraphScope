:param person1Id => 933;
:param person2Id => 4242;

MATCH (person1:PERSON { id: $person1Id })-[path:KNOWS*0..10]-(person2:PERSON { id: $person2Id })
WITH path, gs.function.relationships(path) as rels_in_path
UNWIND rels_in_path as rel
WITH path,  gs.function.startNode(rel) as rel0, gs.function.endNode(rel) as rel1
OPTIONAL MATCH (rel0:PERSON)<-[:HASCREATOR]-(n)-[:REPLYOF]-(m)-[:HASCREATOR]->(rel1:PERSON)
With path, 
    CASE WHEN labels(m) = labels(n) THEN 0 ELSE 1 END as ra,
    CASE WHEN labels(m) <> labels(n) THEN 0 ELSE 1 END as rb
With path, gs.function.nodes(path) as nodes_in_path,  SUM(ra) AS weight1Count, SUM(rb) as weight2Count
UNWIND nodes_in_path as node
WITH path, COLLECT(node.id) as personIdsInPath, weight1Count, weight2Count
RETURN personIdsInPath, (weight1Count + gs.function.toFloat(weight2Count) / 2) AS pathWeight
ORDER BY pathWeight DESC;
