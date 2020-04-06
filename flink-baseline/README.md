# Modules description baseline
## flink-baseline
For usage you need to specify `<parent>` in project pom.xml file: 
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
 - Provides Window based tools
 - Provides common `EventTimeAssigner` for event based approach
## flink-snapshot module
Module has Beta status. Provides common snapshot functions.