-- Question : Can you query source and derieved tables?
SELECT *
FROM system_usage_table;

SELECT *
FROM system_usage_metric;

SELECT *
FROM system_int_usage_stream;

SELECT ROWOFFSET, ROWTIME, id, avg_memory_usage 
FROM system_usage_metric
-- EMIT CHANGES; -- What happens if we do/don't have EMIT CHANGES

