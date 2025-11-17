from dotenv import load_dotenv
import json
import os
import requests

load_dotenv()

class KsqlDBClient:
    def __init__(self, url):
        self.url = url
        self.headers = {"Content-Type": "application/json"}
        self.auth = (os.getenv("KSQLDB_API_KEY"), os.getenv("KSQLDB_API_SECRET"))
    
    def execute_statement(self, ksql_string, streams_properties=None):
        """Execute CREATE, DROP, INSERT statements"""
        payload = {
            "ksql": ksql_string,
            "streamsProperties": streams_properties or {}
        }
        
        response = requests.post(
            f"{self.url}/ksql",
            headers=self.headers,
            data=json.dumps(payload),
            auth=self.auth
        )
        
        if response.status_code == 200:
            return response.json()
        else:
            raise Exception(f"Error: {response.status_code} - {response.text}")
    
    def pull_query(self, ksql_string, streams_properties=None):
        """Execute SELECT queries (pull queries)"""
        payload = {
            "ksql": ksql_string,
            "streamsProperties": streams_properties or {}
        }
        
        response = requests.post(
            f"{self.url}/query",
            headers=self.headers,
            data=json.dumps(payload),
            auth=self.auth
        )
        
        if response.status_code == 200:
            # Parse newline-delimited JSON
            results = []
            for line in response.text.strip().split('\n'):
                if line:
                    results.append(json.loads(line))
            return results
        else:
            raise Exception(f"Error: {response.status_code} - {response.text}")

    
    def push_query(self, ksql_string, streams_properties=None):
        """Execute streaming queries (push queries)"""
        payload = {
            "sql": ksql_string,
            "properties": streams_properties or {
                "auto.offset.reset": "latest"
            }
        }
        
        response = requests.post(
            f"{self.url}/query-stream",
            headers=self.headers,
            data=json.dumps(payload),
            auth=self.auth,
            stream=True
        )
        
        if response.status_code == 200:
            for line in response.iter_lines():
                if line:
                    try:
                        yield json.loads(line)
                    except json.JSONDecodeError:
                        continue
        else:
            raise Exception(f"Error {response.status_code}: {response.text}")

if __name__ == "__main__":
    client = KsqlDBClient(os.getenv("KSQLDB_URL"))

    # Example 1: Create source stream
    result = client.execute_statement("""
    CREATE STREAM IF NOT EXISTS system_usage_stream (
        id VARCHAR,
        timestamp DOUBLE,        
        cpu_usage DOUBLE,
        cpu_stats ARRAY<DOUBLE>,
        memory_usage DOUBLE
    )   
    WITH (KAFKA_TOPIC='system.usage',
        VALUE_FORMAT='avro');
    """)
    print(result)
    print("==================")

    # Example 2: Create derived stream with rounded values
    result = client.execute_statement("""
    CREATE STREAM IF NOT EXISTS system_int_usage_stream AS
        SELECT id, 
            ROUND(cpu_usage) AS cpu_usage_int,
            ROUND(memory_usage) AS memory_usage_int,
            timestamp
        FROM system_usage_stream
        EMIT CHANGES;
    """)
    print(result)
    print("==================")

    # Example 3: Create table with latest values (can be queried with pull queries)
    result = client.execute_statement("""
    CREATE TABLE IF NOT EXISTS system_usage_metric AS
    SELECT id, 
        LATEST_BY_OFFSET(cpu_usage) AS latest_cpu_usage,
        LATEST_BY_OFFSET(memory_usage) AS latest_memory_usage,
        COUNT(*) AS record_count
    FROM system_usage_stream 
    GROUP BY id
    EMIT CHANGES;
    """)
    print(result)
    print("==================")

    # Example 4: Pull Query - Query the table with correct column names
    result = client.pull_query("""
    SELECT id, latest_cpu_usage, latest_memory_usage, record_count 
    FROM system_usage_metric;
    """)
    print(result)
    print("==================")

    # Example 5: Push Query - Stream results as they arrive
    print("Starting push query (press Ctrl+C to stop)...")
    count = 0
    try:
        for row in client.push_query("""SELECT * FROM system_int_usage_stream EMIT CHANGES;"""):
            print(row)
            count += 1
            if count >= 10:  # Limit to 10 rows for testing
                break
    except KeyboardInterrupt:
        print("\nStopped push query")
    except Exception as e:
        print(f"Push query error: {e}")