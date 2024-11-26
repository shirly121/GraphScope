:param country => "India";
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
  (zombie)<-[:HASCREATOR]-()<-[:LIKES]-(likerPerson:PERSON) // Match2
WHERE likerPerson.creationDate < $endDate
WITH
  zombie,
  likerPerson,
  CASE WHEN likerPerson IN zombies THEN 1
  ELSE 0
  END as likerZombie
WITH
  zombie,
  count(likerZombie) as zombieLikeCount,
  count(likerPerson) AS totalLikeCount
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
