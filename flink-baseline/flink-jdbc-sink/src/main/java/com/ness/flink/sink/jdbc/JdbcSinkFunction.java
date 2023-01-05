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
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.io.IOException;

/**
 * @author Khokhlov Pavel
 */
@Slf4j
@AllArgsConstructor
class JdbcSinkFunction<T> extends RichSinkFunction<T> implements CheckpointedFunction {
    private static final long serialVersionUID = 362373966141992666L;

    private final AbstractJdbcOutputFormat<T> outputFormat;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        RuntimeContext ctx = getRuntimeContext();
        outputFormat.setRuntimeContext(ctx);
        outputFormat.open(ctx.getIndexOfThisSubtask(), ctx.getNumberOfParallelSubtasks());
    }

    @Override
    public void invoke(T value, Context context) throws IOException {
        outputFormat.writeRecord(value);
    }

    @Override
    public void initializeState(FunctionInitializationContext context) {
    }

    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {
        log.debug("Flush data");
        outputFormat.flush();
    }

    @Override
    public void close() {
        outputFormat.close();
    }
}
