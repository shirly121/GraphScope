:param tagName => "Meryl_Streep";
:param date => 20111204001031095;

MATCH (person:PERSON)<-[:HASCREATOR]-(msg)-[:HASTAG]->(tag:TAG {name:$tagName})
WHERE msg.creationDate > $date
WITH person, count(msg) as aCount
CALL {
  WITH person, aCount
  RETURN person as person1 , aCount as cm, -4 as degree
}
UNION
CALL {
  WITH person
  MATCH (person)-[:KNOWS]-(person2:PERSON)
  RETURN person2 as person1, 0 as cm, count(person) as degree
}
WITH person1, sum(cm) as cm, sum(degree) as degree
WHERE degree <= 0
RETURN person1, cm;
