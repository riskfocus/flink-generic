package com.riskfocus.flink.example.pipeline.config.source;

import com.riskfocus.flink.assigner.EventTimeAssigner;
import com.riskfocus.flink.config.channel.kafka.KafkaSource;
import com.riskfocus.flink.example.pipeline.domain.Customer;
import com.riskfocus.flink.example.pipeline.domain.Order;
import com.riskfocus.flink.schema.EventDeserializationSchema;
import com.riskfocus.flink.util.ParamUtils;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

/**
 * @author Khokhlov Pavel
 */
public class OrderKafkaSource extends KafkaSource<Order> {

    public OrderKafkaSource(ParamUtils paramUtils) {
        super(paramUtils);
    }

    @Override
    public DataStream<Order> build(StreamExecutionEnvironment env) {
        String topic = paramUtils.getString("order.inbound.topic", "order-request");
        FlinkKafkaConsumer<Order> sourceFunction = new FlinkKafkaConsumer<>(topic, new EventDeserializationSchema<>(Order.class), buildConsumerProps());

        sourceFunction.assignTimestampsAndWatermarks(new EventTimeAssigner<>(getMaxLagTimeMs(), 0));
        env.registerType(Order.class);

        int parallelism = env.getParallelism();
        return env.addSource(sourceFunction)
                .setParallelism(parallelism)
                .name("orderSource").uid("orderSource");
    }
}
