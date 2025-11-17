CREATE TABLE system_usage_metric AS
SELECT id, AVG(cpu_usage) AS avg_cpu_usage,
        AVG(memory_usage) AS avg_memory_usage
FROM system_usage_table
--FROM system_usage_stream -- DOES IT WORK?
GROUP BY id
EMIT CHANGES;