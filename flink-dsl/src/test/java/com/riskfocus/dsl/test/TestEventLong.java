package com.riskfocus.dsl.test;

import com.riskfocus.flink.domain.Event;
import com.riskfocus.flink.domain.FlinkKeyedAware;
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
public class TestEventLong implements Event, FlinkKeyedAware<String> {

    private String key;
    private long timestamp;
    private final Long value = 100L;

    @Override
    public String flinkKey() {
        return key;
    }

}
