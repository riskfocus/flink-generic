package com.ness.flink.stream.test;

import com.ness.flink.domain.Event;
import com.ness.flink.domain.FlinkKeyedAware;
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
    private static final long serialVersionUID = -3684299070243123053L;
    private String key;
    private long timestamp;
    private final Long value = 100L;

    @Override
    public String flinkKey() {
        return key;
    }

}
