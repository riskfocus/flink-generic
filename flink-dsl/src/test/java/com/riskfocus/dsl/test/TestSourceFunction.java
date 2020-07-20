package com.riskfocus.dsl.test;

import lombok.SneakyThrows;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.streaming.api.functions.source.FromElementsFunction;

import java.io.IOException;

/**
 * Provides static factory method to hide exception handling.
 *
 * @inheritDoc
 */
public class TestSourceFunction extends FromElementsFunction<String> {

    private TestSourceFunction(String... elements) throws IOException {
        super(new StringSerializer(), elements);
    }

    @SneakyThrows
    public static TestSourceFunction from(String... elements) {
        return new TestSourceFunction(elements);
    }

}
