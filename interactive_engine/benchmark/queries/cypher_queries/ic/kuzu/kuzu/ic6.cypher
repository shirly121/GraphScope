MATCH (p_:PERSON {id: 19791209300317})-[:PERSON_KNOWS_PERSON*1..3]-(other:PERSON)
WITH distinct other
WHERE  other.id <> 19791209300317

MATCH (other)<-[:POST_HASCREATOR_PERSON]-(p:POST)-[:POST_HASTAG_TAG]->(t:TAG {name: "Nat_King_Cole"})
MATCH    (p:POST)-[:POST_HASTAG_TAG]->(otherTag:TAG)

WITH otherTag, t,count(distinct p) as postCount
WHERE 
    otherTag <> t 
RETURN
    otherTag.name as tagName,
    postCount
ORDER BY 
    postCount desc, 
    tagName asc 
LIMIT 10;