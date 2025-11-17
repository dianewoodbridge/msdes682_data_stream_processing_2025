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


-------------------
-- Join streams  --
-------------------
