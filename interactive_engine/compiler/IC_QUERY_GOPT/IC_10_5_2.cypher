:param personId => 933;
:param month => 11;

MATCH (person:PERSON {id: $personId})-[:KNOWS*2..3]-(friend: PERSON)
WHERE
       NOT friend=person
       AND NOT (friend:PERSON)-[:KNOWS]-(person :PERSON {id: $personId})
       // AND date(datetime({epochMillis: friend.birthday})).month=$month
MATCH (friend:PERSON)<-[:HASCREATOR]-(post1:POST)-[:HASTAG]->(tag:TAG)<-[:HASINTEREST]-(p2: PERSON {id: $personId})

RETURN count(friend);
