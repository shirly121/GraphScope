MATCH 
    (unused:PERSON {id: 10995116278647 })-[:PERSON_KNOWS_PERSON]-(friend:PERSON)<-[:COMMENT_HASCREATOR_PERSON]-(comments:COMMENT)-[:COMMENT_REPLYOF_POST]->(:POST)-[:POST_HASTAG_TAG]->(tags:TAG)
WITH friend, tags, comments
MATCH (tags:TAG)-[:TAG_HASTYPE_TAGCLASS]->(:TAGCLASS)-[:TAGCLASS_ISSUBCLASSOF_TAGCLASS*0..7]->(:TAGCLASS {name: "Chancellor"})
WITH 
    friend AS friend, 
    collect(DISTINCT tags.name) AS tagNames, 
    count(DISTINCT comments) AS replyCount 
ORDER BY 
    replyCount DESC, 
    friend.id ASC 
LIMIT 20 
RETURN 
    friend.id AS personId, 
    friend.firstName AS personFirstName, 
    friend.lastName AS personLastName, 
    tagNames, 
    replyCount