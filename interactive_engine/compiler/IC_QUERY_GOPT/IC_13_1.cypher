:param person1Id => 933;
:param person2Id => 4242;

MATCH (person1: PERSON{id:$person1Id})-[:KNOWS*5..6]-(person2: PERSON {id: $person2Id})
RETURN count(person1);
