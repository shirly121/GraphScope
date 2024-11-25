:param person1Id => 933;
:param person2Id => 4242;

MATCH  (person1: PERSON {id: $person1Id})
OPTIONAL MATCH (person1: PERSON{id:$person1Id})-[k:KNOWS*0..32]-(person2: PERSON {id: $person2Id})
WITH
    CASE 
        WHEN k is null THEN -1
        ELSE length(k)
    END as len
RETURN len;
