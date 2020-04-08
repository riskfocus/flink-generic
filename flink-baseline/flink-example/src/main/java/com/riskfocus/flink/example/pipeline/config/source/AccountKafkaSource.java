package com.riskfocus.flink.example.pipeline.config.source;

import com.riskfocus.flink.assigner.EventTimeAssigner;
import com.riskfocus.flink.config.channel.kafka.KafkaSource;
import com.riskfocus.flink.schema.EventDeserializationSchema;
import com.riskfocus.flink.util.ParamUtils;
import org.apache.flink.streaming.api.datastream.DataStream;
import com.riskfocus.flink.example.pipeline.domain.Account;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

/**
 * @author Khokhlov Pavel
 */
public class AccountKafkaSource extends KafkaSource<Account> {

    public AccountKafkaSource(ParamUtils paramUtils) {
        super(paramUtils);
    }

    @Override
    public DataStream<Account> build(StreamExecutionEnvironment env) {
        String topic = paramUtils.getString("account.inbound.topic", "account");
        FlinkKafkaConsumer<Account> sourceFunction = new FlinkKafkaConsumer<>(topic, new EventDeserializationSchema<>(Account.class), buildConsumerProps());

        sourceFunction.assignTimestampsAndWatermarks(new EventTimeAssigner<>(getMaxLagTimeMs(), 0));
        env.registerType(Account.class);

        int parallelism = env.getParallelism();
        return env.addSource(sourceFunction)
                .setParallelism(parallelism)
                .name("accountSource").uid("accountSource");
    }
}