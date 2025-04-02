MATCH (person:PERSON {id: 2199023256816L})<-[:HASCREATOR]-(message: POST | COMMENT)<-[like:LIKES]-(liker:PERSON)
WITH liker, message, like.creationDate AS likeTime
OPTIONAL MATCH (liker: PERSON)-[k:KNOWS]-(person: PERSON {id: 2199023256816L})
WITH liker, message, likeTime,
  CASE
      WHEN k is null THEN true
      ELSE false
     END AS isNew
ORDER BY likeTime DESC, message.id ASC
LIMIT 20
RETURN
    liker.id as personId,
    liker.firstName AS personFirstName,
    liker.lastName AS personLastName,
    message.id AS messageId;