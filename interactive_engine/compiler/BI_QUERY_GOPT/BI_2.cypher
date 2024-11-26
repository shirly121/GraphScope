:param tagClass => "Person";
:param date1 => 20120502000000000;
:param date2 => 20120910120000000;
:param date3 => 20121218000000000;

MATCH (tag:TAG)-[:HASTYPE]->(:TAGCLASS {name: $tagClass}), (tag:TAG)<-[:HASTAG]-(message)
WITH
  tag,
  CASE
    WHEN message.creationDate <  $date2
  AND message.creationDate >= $date1 THEN 1
    ELSE                                     0
    END AS count1,
  CASE
    WHEN message.creationDate <  $date3
  AND message.creationDate >= $date2 THEN 1
    ELSE                                     0
    END AS count2
WITH
  tag,
  sum(count1) AS countWindow1,
  sum(count2) AS countWindow2
RETURN
  tag.name as name,
  countWindow1,
  countWindow2
ORDER BY
name ASC
LIMIT 100;
