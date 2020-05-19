package com.riskfocus.flink.sink.jdbc;

import com.riskfocus.flink.sink.jdbc.core.output.AbstractJdbcOutputFormat;
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
