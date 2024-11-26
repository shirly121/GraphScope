:param tagClass => 'Person';
:param date => 20120502000000000;
:param dateEnd1 => 20120910120000000;
:param dateEnd2 => 20121218000000000;
// :param date => 1338508800000;
// :param dateEnd1 => 1347148800000;
// :param dateEnd2 => 1355788800000;

MATCH (tag:TAG)-[:HASTYPE]->(:TAGCLASS {name: $tagClass}), (tag:TAG)<-[:HASTAG]-(message)
WITH
  tag,
  CASE
    WHEN message.creationDate <  $dateEnd1
  AND message.creationDate >= $date THEN 1
    ELSE                                     0
    END AS count1,
  CASE
    WHEN message.creationDate <  $dateEnd2
  AND message.creationDate >= $dateEnd1 THEN 1
    ELSE                                     0
    END AS count2
WITH
  tag,
  sum(count1) AS countWindow1,
  sum(count2) AS countWindow2
RETURN
  tag.name as name,
  countWindow1,
  countWindow2,
  abs(countWindow1 - countWindow2) AS diff
ORDER BY
diff DESC,
name ASC
LIMIT 100;
