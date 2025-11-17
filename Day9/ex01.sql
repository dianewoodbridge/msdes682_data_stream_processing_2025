CREATE STREAM system_usage_stream (id VARCHAR,
                                timestamp DOUBLE,        
                                cpu_usage DOUBLE,
                                cpu_stats ARRAY<DOUBLE>,
                                memory_usage DOUBLE
                                )   
WITH (KAFKA_TOPIC='system.usage',
      VALUE_FORMAT='avro');

CREATE TABLE system_usage_table (id VARCHAR PRIMARY KEY,
                                timestamp DOUBLE,      
                                cpu_usage DOUBLE,
                                cpu_stats ARRAY<DOUBLE>,
                                memory_usage DOUBLE
                                )
WITH (KAFKA_TOPIC='system.usage',
      VALUE_FORMAT='avro');

CREATE STREAM system_int_usage_stream AS
    SELECT id, ROUND(cpu_usage) AS cpu_usage_int,
           ROUND(memory_usage) AS memory_usage_int,
           timestamp
    FROM system_usage_stream
    --FROM system_usage_table -- DOES IT WORK?
    EMIT CHANGES;