:param country => "India";
:param endDate1 => 20100130000000000;
:param endDate2 => 20100630000000000;

MATCH (country:PLACE {name: $country})<-[:ISPARTOF]-(:PLACE)<-[:ISLOCATEDIN]-(zombie:PERSON)
WITH zombie
LIMIT 1000
MATCH
  (zombie)<-[:HASCREATOR]-()<-[:LIKES]-(likerZombie:PERSON) // Match1
WHERE likerZombie.creationDate < $endDate1
WITH
  zombie,
  count(distinct likerZombie) AS zombieLikeCount // Aggregate1
MATCH
  (zombie)<-[:HASCREATOR]-()<-[:LIKES]-(likerPerson:PERSON) // Match2
WHERE likerPerson.creationDate < $endDate2
WITH
  zombie,
  zombieLikeCount,
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
