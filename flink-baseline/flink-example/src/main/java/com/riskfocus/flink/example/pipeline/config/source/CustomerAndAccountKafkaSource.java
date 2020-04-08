package com.riskfocus.flink.example.pipeline.config.source;

import com.riskfocus.flink.assigner.EventTimeAssigner;
import com.riskfocus.flink.config.channel.kafka.KafkaSource;
import com.riskfocus.flink.example.pipeline.domain.Account;
import com.riskfocus.flink.example.pipeline.domain.intermediate.CustomerAndAccount;
import com.riskfocus.flink.schema.EventDeserializationSchema;
import com.riskfocus.flink.util.ParamUtils;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

/**
 * @author Khokhlov Pavel
 */
public class CustomerAndAccountKafkaSource extends KafkaSource<CustomerAndAccount>  {

    public CustomerAndAccountKafkaSource(ParamUtils paramUtils) {
        super(paramUtils);
    }

    @Override
    public DataStream<CustomerAndAccount> build(StreamExecutionEnvironment env) {
        String topic = paramUtils.getString("customerAndAccount.output.topic", "customerAndAccount");
        FlinkKafkaConsumer<CustomerAndAccount> sourceFunction = new FlinkKafkaConsumer<>(topic, new EventDeserializationSchema<>(CustomerAndAccount.class),
                buildConsumerProps());

        sourceFunction.assignTimestampsAndWatermarks(new EventTimeAssigner<>(getMaxLagTimeMs(), 0));
        env.registerType(Account.class);

        int parallelism = env.getParallelism();
        return env.addSource(sourceFunction)
                .setParallelism(parallelism)
                .name("customerAndAccountSource").uid("customerAndAccountSource");
    }
}
