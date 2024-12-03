:param country => "India";
:param endDate1 => 20100130000000000;
:param endDate2 => 20100630000000000;

MATCH (country:PLACE {name: $country})<-[:ISPARTOF]-(:PLACE)<-[:ISLOCATEDIN]-(zombie:PERSON)
WITH zombie
LIMIT 1000
MATCH // Match1
  (zombie)<-[:HASCREATOR]-()<-[:LIKES]-(likerZombie:PERSON)
WHERE likerZombie.creationDate < $endDate1
MATCH // Match2
  (zombie)<-[:HASCREATOR]-()<-[:LIKES]-(likerPerson:PERSON)
WHERE likerPerson.creationDate < $endDate2
WITH
  zombie,
  count(distinct likerZombie) AS zombieLikeCount, // Aggregate1
  count(distinct likerPerson) AS totalLikeCount // Aggregate2
RETURN
  zombie.id AS zid,
  zombieLikeCount,
  totalLikeCount,
  CASE totalLikeCount
    WHEN 0 THEN 0.0
    ELSE zombieLikeCount / totalLikeCount
    END AS zombieScore
ORDER BY
  zombieScore DESC,
  zid ASC
LIMIT 100;
