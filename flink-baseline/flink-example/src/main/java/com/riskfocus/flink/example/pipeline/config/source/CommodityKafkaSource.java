package com.riskfocus.flink.example.pipeline.config.source;

import com.riskfocus.flink.assigner.EventTimeAssigner;
import com.riskfocus.flink.config.channel.kafka.KafkaSource;
import com.riskfocus.flink.schema.EventDeserializationSchema;
import com.riskfocus.flink.util.ParamUtils;
import org.apache.flink.streaming.api.datastream.DataStream;
import com.riskfocus.flink.example.pipeline.domain.Commodity;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

/**
 * @author Khokhlov Pavel
 */
public class CommodityKafkaSource extends KafkaSource<Commodity> {

    public CommodityKafkaSource(ParamUtils paramUtils) {
        super(paramUtils);
    }

    @Override
    public DataStream<Commodity> build(StreamExecutionEnvironment env) {
        String topic = paramUtils.getString("commodity.inbound.topic", "commodity");
        FlinkKafkaConsumer<Commodity> sourceFunction = new FlinkKafkaConsumer<>(topic, new EventDeserializationSchema<>(Commodity.class), buildConsumerProps());

        sourceFunction.assignTimestampsAndWatermarks(new EventTimeAssigner<>(getMaxLagTimeMs(), 0));
        env.registerType(Commodity.class);

        int parallelism = env.getParallelism();
        return env.addSource(sourceFunction)
                .setParallelism(parallelism)
                .name("commoditySource").uid("commoditySource");
    }
}
