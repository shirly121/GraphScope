:param datetime => 20100601000000000;
// :param datetime => '2011-12-01T00:00:00.000';

MATCH (message:COMMENT)
WHERE message.creationDate < $datetime
AND message.length > 0
WITH
  message,
  message.creationDate / 10000000000000 as year
  // date(datetime({epochMillis: message.creationDate})) AS date
RETURN
  year,
  CASE
    WHEN 'POST' in labels(message)  THEN 0
    ELSE                                 1
    END AS isComment,
  CASE
    WHEN message.length <  40 THEN 0
    WHEN message.length <  80 THEN 1
    WHEN message.length < 160 THEN 2
    ELSE                           3
    END AS lengthCategory,
  count(message) AS messageCount,
  sum(message.length) / count(message) AS averageMessageLength,
  count(message.length) AS sumMessageLength;
