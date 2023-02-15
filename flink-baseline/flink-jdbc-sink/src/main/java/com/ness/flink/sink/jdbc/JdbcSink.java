/*
 * Copyright 2020-2023 Ness USA, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.ness.flink.sink.jdbc;

import com.ness.flink.sink.jdbc.core.output.AbstractJdbcOutputFormat;
import java.io.IOException;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.SinkWriter;

/**
 * @author Khokhlov Pavel
 */
@Slf4j
@AllArgsConstructor
class JdbcSink<T> implements Sink<T> {
    private static final long serialVersionUID = 362373966141992666L;
    private final AbstractJdbcOutputFormat<T> outputFormat;

    @Override
    public SinkWriter<T> createWriter(InitContext context) throws IOException {
        outputFormat.open(context);
        return outputFormat;
    }

}
