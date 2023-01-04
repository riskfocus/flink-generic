# Common domain & utility classes 

Flink application works with [Time](https://nightlies.apache.org/flink/flink-docs-release-1.16/docs/concepts/time/)

This module contains useful interfaces and utility classes which could be used in your Flink pipeline 

See details in classes documentation:
- Time related
  - [Event](src/main/java/com/riskfocus/flink/domain/Event.java)
  - [IncomingEvent](src/main/java/com/riskfocus/flink/domain/IncomingEvent.java)
  - [EventUtils](src/main/java/com/riskfocus/flink/util/EventUtils.java)
- Pipeline related  
  - [KafkaKeyedAware](src/main/java/com/riskfocus/flink/domain/KafkaKeyedAware.java)
  - [FlinkKeyedAware](src/main/java/com/riskfocus/flink/domain/FlinkKeyedAware.java)

Usage of these classes you could find in [flink-example-domain](../flink-example-domain/src/main/java/com/riskfocus/flink/example/pipeline/domain) module
