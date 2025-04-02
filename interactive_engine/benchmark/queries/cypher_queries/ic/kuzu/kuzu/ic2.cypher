MATCH (p :PERSON {id: 28587302476540})-[:PERSON_KNOWS_PERSON]-(friend:PERSON)<-[:COMMENT_HASCREATOR_PERSON|:POST_HASCREATOR_PERSON]-(message : COMMENT:POST) 
WHERE 
    message.creationDate <= 1324252800000
WITH 
    friend, 
    message 
ORDER BY 
    message.creationDate DESC, 
    message.id ASC LIMIT 20 
return 
    friend.id AS personId,
    friend.firstName AS personFirstName,
    friend.lastName AS personLastName, 
    message.id AS messageId,
    message.creationDate AS messageCreationDate;