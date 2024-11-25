:param personId => 933;
:param countryXName => 'Laos';
:param countryYName => 'United_States';
:param startDate => 20100601120000000;
:param endDate => 20100629120000000;
// :param startDate => 1275393600000;
// :param endDate => 1277812800000;

MATCH
    (p:PERSON {id: $personId})-[:KNOWS*1..3]-(otherP:PERSON)
WITH DISTINCT otherP
MATCH (country:PLACE)<-[:ISLOCATEDIN]-(message)-[:HASCREATOR]->(otherP:PERSON)-[ISLOCATEDIN]->(city:PLACE)
WHERE
     otherP.id<> $personId
     AND (country.name = $countryXName OR country.name = $countryYName)
     AND message.creationDate >= $startDate
     AND message.creationDate < $endDate
WITH otherP
Limit 20000
RETURN count(otherP);
