----------------------------------------------
-- First create system.user on Kafka Topics --
----------------------------------------------
CREATE STREAM system_user(
    id VARCHAR,
    name VARCHAR
) WITH (
    KAFKA_TOPIC = 'system.user',
    VALUE_FORMAT = 'avro'
);

CREATE TABLE latest_user AS
  SELECT id, 
         LATEST_BY_OFFSET(name) as user_name
  FROM system_user
  GROUP BY id  -- No WINDOW clause
  EMIT CHANGES;

-----------------------------------------
-- Run producer2.py and producer1.py.  --
-----------------------------------------

-----------------------------------------
-- Join non-windowed stream with table --
-----------------------------------------
SELECT 
s.id,
latest_user.user_name,
AVG(s.cpu_usage) as avg_cpu,
AVG(s.memory_usage) as avg_mem
FROM  system_usage_stream s
LEFT JOIN latest_user
ON latest_user.id = s.id
WINDOW HOPPING (SIZE 10 SECONDS, ADVANCE BY 3 SECONDS)
GROUP BY s.id, latest_user.user_name
EMIT CHANGES;



-------------------
-- Join streams  --
-------------------
SELECT 
    s.id,
    system_user.name,
    AVG(s.cpu_usage) AS avg_cpu,
    AVG(s.memory_usage) AS avg_mem
FROM system_usage_stream s
LEFT JOIN system_user 
WITHIN 10 SECONDS
ON system_user.id = s.id
GROUP BY s.id, system_user.name
EMIT CHANGES;


SELECT 
    s.id,
    system_user.name,
    AVG(s.cpu_usage) AS avg_cpu,
    AVG(s.memory_usage) AS avg_mem
FROM system_usage_stream s
LEFT JOIN system_user 
WITHIN 10 SECONDS
ON system_user.id = s.id
WINDOW HOPPING (SIZE 10 SECONDS, ADVANCE BY 3 SECONDS)
GROUP BY s.id, system_user.name
EMIT CHANGES;