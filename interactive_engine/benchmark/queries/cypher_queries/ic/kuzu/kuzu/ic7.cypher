MATCH (person:PERSON {id: 2199023256816})<-[:POST_HASCREATOR_PERSON|:COMMENT_HASCREATOR_PERSON]-(message: POST:COMMENT)<-[like:PERSON_LIKES_POST|:PERSON_LIKES_COMMENT]-(liker:PERSON)
WITH liker, message, like.creationDate AS likeTime
OPTIONAL MATCH (liker: PERSON)-[k:PERSON_KNOWS_PERSON]-(person: PERSON {id: 2199023256816})
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
