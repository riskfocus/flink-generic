# Flink Canary Application

This is a simple application that can:
1. Verify successful connection to the Kafka Broker with the provided Kafka configs.
2. Verify if the Kafka Topic Exists in the Broker.

## Build Jar
Build jar using maven from the root directory
```bash
$ mvn clean install
```

## Start Local Kafka Cluster
Bring up a kafka cluster with default port 'localhost:19093' and create a topic in the cluster called 'test-topic'.
 ```bash
$ cd docker
$ docker-compose up -d
 ```

## Input Data
Go to the configuration file titled application.yml, which is located in the src/main/resources/ folder. <br>
Here, you can update the target broker and topic that you want flink-canary to test.
#### Kafka Config Sample Values
```yaml
# Canary target broker host should be configured here
kafka:
  bootstrap.servers: your-target-broker (i.e. localhost:19093)

# Canary target topic name should be configured here
canary.test.source:
  topic: your-target-topic-name (i.e. test-topic)
```


## Expected Output

### Verify Broker Connection
##### Successful
In case of Successful Broker connection, you find a INFO message in the logs with the broker details. Then, it proceeds with Topic verification.
###### Sample Log
`Kafka Broker Connection: Successful - localhost:19093`

##### Failure
In case of Connection Failure, you find a ERROR message in the logs with the broker details. No Topic Verification will happen as the broker connection failed.
###### Sample Log
`Kafka Broker Connection: Failed - localhost:12345`



### Topic Verification
##### Topic Found
If the given topic exists in the broker, you find a INFO message in the logs with the topic name.
###### Sample Log
`Kafka Topic: Exists - test-topic`

##### No Topic Found
If the given topic does not exist in the broker, you find a ERROR message in the logs with the topic name.
###### Sample Log
`Kafka Topic: Not Found: - test-topic123`