-- Return continuously id and Top-3 cpu_usage within
-- A tumbling window with the size of 10 seconds.
SELECT id, TOPK(cpu_usage, 3)
FROM system_usage_stream
WINDOW TUMBLING (SIZE 10 SECOND) 
GROUP BY id
EMIT CHANGES;

-- Return continuously id and Top-3 cpu_usage within
-- A hopping window with the size of 10 seconds which moves by 5 seconds.
SELECT id, TOPK(cpu_usage, 3)
FROM system_usage_stream
WINDOW HOPPING (SIZE 10 SECOND, ADVANCE BY 5 SECOND) 
GROUP BY id
EMIT CHANGES;

-- Return continuously id and Top-3 cpu_usage within
-- A session window with the size of 10 seconds.
SELECT id, TOPK(cpu_usage, 3)
FROM system_usage_stream
WINDOW SESSION (10 SECONDS) 
GROUP BY id
EMIT CHANGES;
