:param country => 'India';
:param endDate => 1277812800000;

MATCH (country:PLACE {name: $country})<-[:ISPARTOF]-(:PLACE)<-[:ISLOCATEDIN]-(zombie:PERSON)
WHERE zombie.creationDate < $endDate
OPTIONAL MATCH (zombie)<-[:HASCREATOR]-(message)
WHERE message.creationDate < $endDate
WITH
  country,
  zombie,
  date(datetime({epochMillis: $endDate})) as idate,
  date(datetime({epochMillis: zombie.creationDate})) as zdate,
  count(message) AS messageCount
WITH
  country,
  zombie,
  12 * (idate.year  - zdate.year )
  + (idate.month - zdate.month)
  + 1 AS months,
  messageCount
WHERE messageCount / months < 1
WITH
  country,
  collect(zombie) AS zombies
UNWIND zombies AS zombie
MATCH
  (zombie)<-[:HASCREATOR]-()<-[:LIKES]-(likerZombie:PERSON) // Match1
WHERE likerZombie IN zombies
WITH
  zombie,
  count(distinct likerZombie) AS zombieLikeCount // Aggregate1
MATCH
  (zombie)<-[:HASCREATOR]-()<-[:LIKES]-(likerPerson:PERSON) // Match2
WHERE likerPerson.creationDate < $endDate
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
