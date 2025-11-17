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

