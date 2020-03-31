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
     * Sink registration (addSink) in general requires a name and uuid of sink.
     */
    private final String name;

    /**
     * Flink Sink function
     */
    private final SinkFunction<S> function;
}
