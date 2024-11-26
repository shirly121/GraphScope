:param person1Id => 933;
:param person2Id => 4242;

MATCH (person1: PERSON{id:$person1Id})-[:KNOWS*2..3]-(person3: PERSON)
WITH person3
MATCH (person3: PERSON)-[:KNOWS*3..4]-(person2: PERSON {id: $person2Id})
RETURN count(person3);
