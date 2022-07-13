package com.ness.flink.dsl.test;

import com.riskfocus.flink.domain.Event;
import com.riskfocus.flink.domain.FlinkKeyedAware;
import com.riskfocus.flink.domain.KafkaKeyedAware;
import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

@AllArgsConstructor
@Getter
@Setter
@EqualsAndHashCode(exclude = "timestamp")
@ToString
public class TestEventString implements Event, FlinkKeyedAware<String>, KafkaKeyedAware {

    private String key;
    private long timestamp;
    private String value;

    public static TestEventString fromKey(String key) {
        return new TestEventString(key, System.currentTimeMillis(), key);
    }

    @Override
    public byte[] kafkaKey() {
        return key.getBytes();
    }

    @Override
    public String flinkKey() {
        return key;
    }

}
