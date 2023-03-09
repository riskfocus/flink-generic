# Apache Flink pipeline builder functions

Currently, Flink project suffers from lack of high-level abstractions, reducing number of boilerplate code. Developers need to copy-paste same things over and over in every Flink based project, including:

1. Each operator parallelism and name
2. Execution environment configuration, related to metrics, checkpointing, time characteristics, etc
3. Sink and source configuration, related to its type: Kafka, HDFS, S3, etc
4. Passing environment variables and/or properties from files and define overriding strategy. Flink `ParameterTool` class allows to create properties from each source, but doesn't provide override/fallback functionality.

The idea is to encapsulate an `StreamExecutionEnvironment` instance in one class, providing different abstractions to perform different sets of actions on this instance. This allows chaining job building steps, similar to Kafka StreamsBuilder in conjunction with Spring Boot Configuration.

## Example
<pre>
StreamBuilder.from(env, params)
    .stream()
    .source(testSource)
    .addKeyedProcessor(new KeyedProcessorDefinition<>(
        OperatorProperties.from("test.processor", params), v -> v, new TestProcessFunction()))
    .addToStream(stream -> stream.map(v -> {
        log.info("Read: value={}", v);
        return v;
    }))
    .addSink(() -> new SinkFunction<>() {
        @Override
        public void invoke(TestEventString value, Context context) {
            VALUES.add(value);
        }
    }).build().run("test.sink");
</pre>
