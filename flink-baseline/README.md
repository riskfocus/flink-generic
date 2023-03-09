# Modules description baseline
## flink-baseline

Contains Maven BOM dependencies related to Flink

For usage, you need to specify `BOM` in your Flink project `pom.xml` file: 
```xml
<dependencyManagement>
    <dependencies>
        <dependency>
            <groupId>com.ness.flink.generic</groupId>
            <artifactId>flink-baseline</artifactId>
            <version>${flink.generic.version}</version>
            <type>pom</type>
            <scope>import</scope>
        </dependency>
    </dependencies>
</dependencyManagement>
```
## flink-common module
Contains Tools for configurable Flink Application.

For usage, you need to provide `dependency` in your Flink project `pom.xml` file:
```xml
<dependencies>
    <!-- Flink shared library -->
    <dependency>
        <groupId>com.ness.flink.generic</groupId>
        <artifactId>flink-common</artifactId>
        <version>${flink.generic.version}</version>
    </dependency>
</dependencies>    
```

Provides:
 - Reading parameters from [yaml configuration](flink-common/src/test/resources/application.yml) / command line / environment variables
 - Configurable Kafka Source/Sink functions
   - Supports POJO / AVRO formats 
   - Supports Confluent Cloud
   - Supports AWS Cloud
 - Configurable Flink Operators
 - Redis Sink function example
   - Redis configuration (based on [Redis lettuce client](https://lettuce.io)) 
 - Provides common interfaces for Source/Sink Flink functions.
 - Provides `Window` based tools (event time processing)
 - Provides TimestampAssigner `EventTimeAssigner` for event based time characteristic

More details [README.md](flink-common/README.md)

## SmoothingPrices module
Flink PriceSmoothing application built on top on flink-common module  more details 
[SmoothingPrices](flink-example/README.md)

## flink-snapshot module
Provides common snapshot functions (in atomic way):
 - Ability to Sink data into Redis
 - Ability to use that data from Redis
 - For now Module has Beta status. 