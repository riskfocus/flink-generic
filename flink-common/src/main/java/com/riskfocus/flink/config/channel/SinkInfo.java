package com.riskfocus.flink.config.channel;

import lombok.AllArgsConstructor;
import lombok.Getter;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

/**
 *
 * @author Khokhlov Pavel
 */
@AllArgsConstructor
@Getter
public class SinkInfo<S> {

    /**
     * Name of Sink required for pipeline
     */
    private final String name;

    /**
     * Flink Sink function
     */
    private final SinkFunction<S> function;
}
