:param country => "India";
:param endDate => 20100630000000000;

MATCH (country:PLACE {name: $country})<-[:ISPARTOF]-(:PLACE)<-[:ISLOCATEDIN]-(zombie:PERSON)
WHERE zombie.creationDate < $endDate
OPTIONAL MATCH (zombie)<-[:HASCREATOR]-(message)
WHERE message.creationDate < $endDate
WITH
  country,
  zombie,
  $endDate as idate,
  zombie.creationDate as zdate,
  count(message) AS messageCount
WITH
  country,
  zombie,
  12 * ( idate / 10000000000000  - zdate / 10000000000000 )
  + ( idate / 100000000000 % 100 ) - ( zdate / 100000000000 % 100 )
  + 1 AS months,
  messageCount
WHERE messageCount / months < 1
WITH
  country,
  collect(zombie) AS zombies
UNWIND zombies AS zombie
MATCH // Match1
  (zombie)<-[:HASCREATOR]-()<-[:LIKES]-(likerZombie:PERSON)
WHERE likerZombie IN zombies
MATCH // Match2
  (zombie)<-[:HASCREATOR]-()<-[:LIKES]-(likerPerson:PERSON)
WHERE likerPerson.creationDate < $endDate
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
