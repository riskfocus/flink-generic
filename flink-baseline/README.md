# Modules description baseline
## flink-baseline
For usage you need to specify `parent` in your own project `pom.xml` file: 
```
<parent>
    <groupId>com.riskfocus.flink.generic</groupId>
    <artifactId>flink-baseline</artifactId>
    <version>1.0-master-SNAPSHOT</version>
</parent>
```
## flink-common module
Flink common contain classes for configuration of Flink Application.
Provides:
 - Reading parameters from command line/environment variables
 - Kafka consumer/producer functions
 - Redis Sink function
 - Redis configuration (Redis lettuce client: https://lettuce.io) 
 - Provides basic interfaces Source/Sink.
 - Provides `Window` based tools (generator etc)
 - Provides TimestampAssigner `EventTimeAssigner` for event based time characteristic
## flink-snapshot module
Provides common snapshot functions (in atomic way):
 - Ability to Sink data into Redis
 - Ability to use that data from Redis
For now Module has Beta status. 