MATCH (p :PERSON {id: 28587302476540L})-[:KNOWS]-(friend:PERSON)<-[:HASCREATOR]-(message : COMMENT|POST) 
WHERE 
    message.creationDate <= 1324252800000L
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