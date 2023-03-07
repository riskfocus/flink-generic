//Copyright 2021-2023 Ness Digital Engineering
//
//Licensed under the Apache License, Version 2.0 (the "License");
//you may not use this file except in compliance with the License.
//You may obtain a copy of the License at
//
//http://www.apache.org/licenses/LICENSE-2.0
//
//Unless required by applicable law or agreed to in writing, software
//distributed under the License is distributed on an "AS IS" BASIS,
//WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//See the License for the specific language governing permissions and
//limitations under the License.

package com.ness.flink.test.example.receiver;

import lombok.AllArgsConstructor;
import org.apache.kafka.streams.processor.AbstractProcessor;
import org.apache.kafka.streams.processor.ProcessorContext;

import java.util.function.BiFunction;

/**
 * @author Khokhlov Pavel
 */
@AllArgsConstructor
public class ResultProcessor<T, R> extends AbstractProcessor<String, T> {

    private final ResultService<R> resultService;
    private final BiFunction<ProcessorContext, T, R> transformFunction;
    private final BiFunction<String, R, String> keyTransformFunction;

    /**
     *
     * @param resultService service registers new message from Kafka
     * @param transformFunction transformation of incoming message: accepts ProcessorContext and original Kafka message,
     *                          should return transformed message
     */
    public ResultProcessor(ResultService<R> resultService, BiFunction<ProcessorContext, T, R> transformFunction) {
        this(resultService, transformFunction, null);
    }

    @Override
    public void process(String key, T value) {
        R transformed = transformFunction.apply(context(), value);
        if (keyTransformFunction != null) {
            key = keyTransformFunction.apply(key, transformed);
        }
        resultService.process(key, transformed);
    }
}
