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
 ```bash
$ cd docker
$ docker-compose up -d
 ```

## Input Data

Modify run configuration of FlinkCanaryJob. Pass the following Kafka Config details in the program arguments.
#### Kafka Config Sample Values
`--bootstrap.servers localhost:19093`
`--topic test-topic`


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