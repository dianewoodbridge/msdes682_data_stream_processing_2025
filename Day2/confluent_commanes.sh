# installation
brew install confluentinc/tap/cli

# login
confluent login

# creating a confluent cloud environment
confluent environment create environment_name -o json

# activating the confluent cloud environment
confluent environment use environment_id

# creating a Kafka cluster on GCP
confluent kafka cluster create cluster_name --cloud gcp --region us-west1

# creating an API Key/Secret for authorization to produce topics
confluent api-key create --description "description" --resource cluster_id -o json

# specifying the API Key for the cluster to use
confluent api-key use API_KEY --resource cluster_id

# choosing a cluster to use
confluent kafka cluster use cluster_id

# creating a Kafka topic within the cluster
confluent kafka topic create topic_name

# starting producing messages to the topic 
confluent kafka topic produce topic_name

####################################
# in a separate terminal           # 
####################################
# consuming messages from topic
confluent kafka topic consume topic_name -b
