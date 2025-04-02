MATCH (p:PERSON {id: 19791209300317})-[:PERSON_KNOWS_PERSON*1..3]-(friend:PERSON)
WITH distinct friend
where friend.id <>  19791209300317
MATCH  (message:POST:COMMENT)-[e:POST_HASCREATOR_PERSON|:COMMENT_HASCREATOR_PERSON]->(friend)
WITH friend, message, message.creationDate AS messageCreationDate, message.id AS messageId
ORDER BY 
   messageCreationDate DESC, 
messageId ASC 
LIMIT 20
RETURN 
    friend.id AS personId, 
    friend.firstName AS personFirstName, 
    friend.lastName AS personLastName, 
    messageId, 
    messageCreationDate;
