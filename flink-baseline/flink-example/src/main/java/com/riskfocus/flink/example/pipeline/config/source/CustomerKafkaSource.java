package com.riskfocus.flink.example.pipeline.config.source;

import com.riskfocus.flink.assigner.EventTimeAssigner;
import com.riskfocus.flink.config.channel.kafka.KafkaSource;
import com.riskfocus.flink.example.pipeline.domain.Customer;
import com.riskfocus.flink.schema.EventDeserializationSchema;
import com.riskfocus.flink.util.ParamUtils;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

/**
 * @author Khokhlov Pavel
 */
public class CustomerKafkaSource extends KafkaSource<Customer> {

    public CustomerKafkaSource(ParamUtils paramUtils) {
        super(paramUtils);
    }

    @Override
    public DataStream<Customer> build(StreamExecutionEnvironment env) {
        String topic = paramUtils.getString("customer.inbound.topic", "customer");
        FlinkKafkaConsumer<Customer> sourceFunction = new FlinkKafkaConsumer<>(topic, new EventDeserializationSchema<>(Customer.class), buildConsumerProps());

        sourceFunction.assignTimestampsAndWatermarks(new EventTimeAssigner<>(getMaxLagTimeMs(), 0));
        env.registerType(Customer.class);

        int parallelism = env.getParallelism();
        return env.addSource(sourceFunction)
                .setParallelism(parallelism)
                .name("customerSource").uid("customerSource");
    }
}
