/*
 * Copyright (c) 2020 Risk Focus, Inc.
 */

package com.riskfocus.flink.example.pipeline.manager.stream.function;

import com.riskfocus.flink.example.pipeline.config.SmoothingConfig;
import com.riskfocus.flink.example.pipeline.domain.InterestRate;
import com.riskfocus.flink.example.pipeline.domain.intermediate.InterestRates;
import com.riskfocus.flink.util.ParamUtils;
import com.riskfocus.flink.window.WindowAware;
import com.riskfocus.flink.window.WindowContext;
import com.riskfocus.flink.window.generator.WindowGeneratorProvider;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import static com.riskfocus.flink.util.EventUtils.updateRequired;

/**
 * @author Khokhlov Pavel
 */
@Slf4j
public class ProcessRatesFunction extends KeyedProcessFunction<String, InterestRate, InterestRates> {
    private static final long serialVersionUID = -1673971835941156836L;

    private transient ValueState<Boolean> updateRequired;
    private transient MapState<String, InterestRate> latest;

    private transient WindowAware windowAware;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        String opName = getClass().getName();
        MapStateDescriptor<String, InterestRate> ratesValueStateDescriptor =
                new MapStateDescriptor<>(opName + "-interestRates", String.class, InterestRate.class);
        latest = getRuntimeContext().getMapState(ratesValueStateDescriptor);
        updateRequired = getRuntimeContext().getState(new ValueStateDescriptor<>(opName + "UpdateRequiredState", Boolean.class));

        ParameterTool globalParams = (ParameterTool) getRuntimeContext().getExecutionConfig().getGlobalJobParameters();
        ParamUtils paramUtils = new ParamUtils(globalParams);

        windowAware = WindowGeneratorProvider.create(paramUtils);

        log.info("WindowAware: {}", windowAware);
    }

    @Override
    public void processElement(InterestRate rate, Context ctx, Collector<InterestRates> out) throws Exception {
        if (!updateRates(rate)) {
            return;
        }

        if (!Boolean.TRUE.equals(updateRequired.value())) {
            updateRequired.update(true);
            // Register timer
            long timestamp = rate.getTimestamp();
            WindowContext context = windowAware.generateWindowPeriod(timestamp);

            long fireTime = context.endOfWindow();
            log.debug("WindowPeriod: {}, registered timer at: {}", context, fireTime);
            ctx.timerService().registerEventTimeTimer(fireTime);
        }

    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<InterestRates> out) throws Exception {
        super.onTimer(timestamp, ctx, out);
        log.info("Fired at: {}, ctx.timestamp: {}, watermark: {}, processingTime: {}", timestamp, ctx.timestamp(),
                ctx.timerService().currentWatermark(), ctx.timerService().currentProcessingTime());
        updateRequired.update(false);

        collect(out, timestamp);
    }

    private void collect(Collector<InterestRates> out, long timestamp) throws Exception {
        InterestRates interestRates = load();
        interestRates.setTimestamp(timestamp);
        out.collect(interestRates);
    }

    private boolean updateRates(InterestRate currentRate) throws Exception {
        boolean updateRequired = false;
        final String maturity = currentRate.getMaturity();
        InterestRate fromStorage = latest.get(maturity);
        if (updateRequired(fromStorage, currentRate)) {
            latest.put(maturity, currentRate);
            updateRequired = true;
        }
        return updateRequired;
    }

    private InterestRates load() throws Exception {
        InterestRates rates = new InterestRates();
        latest.values().forEach(rates::add);
        return rates;
    }

}
