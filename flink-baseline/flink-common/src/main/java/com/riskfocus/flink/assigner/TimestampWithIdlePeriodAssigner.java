package com.riskfocus.flink.assigner;

import java.util.function.Function;

/**
 * Timestamp assigner with custom {@code Function<T, Long> timestampExtractor} function.
 * Made for a classes where timestamp is not defined by an interface and should be extracted manually.
 * <br>Suitable for Avro classes for example
 * <br>When initialized, <i>timestampExtractor</i> function should be defined as <b>Serializable</b>, for example:
 * <br>{@code (Function<TradeDto, Long> & Serializable) TradeDto::getTimestamp}
 * @author NIakovlev
 */
public class TimestampWithIdlePeriodAssigner<T> extends AscendingTimestampExtractorWithIdlePeriod<T> {

    private static final long serialVersionUID = 2121133396445336882L;

    private final Function<T, Long> timestampExtractor;

    public TimestampWithIdlePeriodAssigner(Function<T, Long> timestampExtractor, long maxIdlePeriod, long delay) {
        super(maxIdlePeriod, delay);
        this.timestampExtractor = timestampExtractor;
    }

    @Override
    public long extractTimestamp(T event) {
        return timestampExtractor.apply(event);
    }
}
