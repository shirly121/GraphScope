MATCH 
    (p:PERSON {id: 15393162790207})-[:PERSON_KNOWS_PERSON*1..3]-(otherP:PERSON)
WITH distinct otherP
WHERE otherP.id <> 15393162790207
MATCH (country:PLACE)<-[:POST_ISLOCATEDIN_PLACE|:COMMENT_ISLOCATEDIN_PLACE]-(message : POST:COMMENT)-[:POST_HASCREATOR_PERSON|:COMMENT_HASCREATOR_PERSON]->(otherP:PERSON)-[ISLOCATEDIN]->(city:PLACE)-[:PLACE_ISPARTOF_PLACE]-> (country2:PLACE)
WHERE 
     (country.name = "Puerto_Rico" OR country.name = "Republic_of_Macedonia")
      AND message.creationDate >= 1291161600000
      AND message.creationDate < 1293753600000
WITH 

     message,
    otherP, 
    country,country2
WHERE (country2.name <> "Puerto_Rico" AND country2.name <> "Republic_of_Macedonia")
    
WITH otherP,
     CASE WHEN country.name="Puerto_Rico" THEN 1 ELSE 0 END AS messageX,
     CASE WHEN country.name="Republic_of_Macedonia" THEN 1 ELSE 0 END AS messageY
WITH otherP, sum(messageX) AS xCount, sum(messageY) AS yCount
WHERE xCount > 0 AND yCount > 0
RETURN
    otherP.id as id,
    otherP.firstName as firstName,
    otherP.lastName as lastName,
    xCount,
    yCount,
    xCount + yCount as total 
ORDER BY total DESC, id ASC LIMIT 20;