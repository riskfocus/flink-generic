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

package com.ness.flink.example.pipeline.manager.stream.function;

import com.ness.flink.config.properties.WatermarkProperties;
import com.ness.flink.example.pipeline.domain.InterestRate;
import com.ness.flink.example.pipeline.domain.intermediate.InterestRates;
import com.ness.flink.util.EventUtils;
import com.ness.flink.window.WindowAware;
import com.ness.flink.window.WindowContext;
import com.ness.flink.window.generator.WindowGeneratorProvider;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

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

        ParameterTool parameterTool = (ParameterTool) getRuntimeContext().getExecutionConfig().getGlobalJobParameters();
        WatermarkProperties watermarkProperties = WatermarkProperties.from(parameterTool);

        windowAware = WindowGeneratorProvider.create(watermarkProperties);

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
        if (log.isInfoEnabled()) {
            log.info("Fired at: {}, ctx.timestamp: {}, watermark: {}, processingTime: {}", timestamp, ctx.timestamp(),
                ctx.timerService().currentWatermark(), ctx.timerService().currentProcessingTime());
        }
        updateRequired.update(false);

        collect(out, timestamp);
    }

    private void collect(Collector<InterestRates> out, long timestamp) throws Exception {
        InterestRates interestRates = load();
        interestRates.setTimestamp(timestamp);
        out.collect(interestRates);
    }

    private boolean updateRates(InterestRate currentRate) throws Exception {
        boolean updateNeeded = false;
        final String maturity = currentRate.getMaturity();
        InterestRate fromStorage = latest.get(maturity);
        if (EventUtils.updateRequired(fromStorage, currentRate)) {
            latest.put(maturity, currentRate);
            updateNeeded = true;
        }
        return updateNeeded;
    }

    private InterestRates load() throws Exception {
        InterestRates rates = new InterestRates();
        latest.values().forEach(rates::add);
        return rates;
    }

}
