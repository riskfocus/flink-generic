/*
 * Copyright 2021-2023 Ness Digital Engineering
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.ness.flink.example.pipeline.manager.stream.function;

import static com.ness.flink.example.pipeline.manager.stream.OptionPriceStream.CONFIGURATION_DESCRIPTOR;

import com.ness.flink.config.properties.WatermarkProperties;
import com.ness.flink.domain.Event;
import com.ness.flink.example.pipeline.config.properties.ApplicationProperties;
import com.ness.flink.example.pipeline.domain.InterestRate;
import com.ness.flink.example.pipeline.domain.JobConfig;
import com.ness.flink.example.pipeline.domain.OptionPrice;
import com.ness.flink.example.pipeline.domain.SmoothingRequest;
import com.ness.flink.example.pipeline.domain.Underlying;
import com.ness.flink.example.pipeline.domain.intermediate.InterestRates;
import com.ness.flink.example.pipeline.snapshot.InterestRatesLoader;
import com.ness.flink.example.pipeline.snapshot.SnapshotSourceFactory;
import com.ness.flink.snapshot.context.ContextMetadata;
import com.ness.flink.snapshot.context.ContextService;
import com.ness.flink.snapshot.context.ContextServiceProvider;
import com.ness.flink.snapshot.context.properties.ContextProperties;
import com.ness.flink.snapshot.redis.SnapshotData;
import com.ness.flink.util.EventUtils;
import com.ness.flink.util.SupplierThrowsException;
import com.ness.flink.window.WindowAware;
import com.ness.flink.window.WindowContext;
import com.ness.flink.window.generator.WindowGeneratorProvider;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import org.apache.flink.util.Collector;


/**
 * @author Khokhlov Pavel
 */
@Slf4j
@SuppressWarnings({"PMD.ExcessiveImports", "PMD.TooManyMethods"})
public class ProcessSmoothingFunction extends KeyedBroadcastProcessFunction<String, OptionPrice, JobConfig, SmoothingRequest> {

    private static final long serialVersionUID = -4848961500498134626L;

    private static final String CONFIG_KEY = "configKey";
    private transient MapState<String, OptionPrice> pricesState;
    private transient MapState<String, InterestRate> ratesState;
    private transient ValueState<Boolean> pricesUpdateRequiredState;

    private transient WindowAware windowAware;
    private transient ContextService contextService;
    private transient InterestRatesLoader loader;

    private transient boolean debugLogging;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);

        String opName = getClass().getName();

        MapStateDescriptor<String, OptionPrice> stateDescription = new MapStateDescriptor<>(opName + "-optionPriceByUnderlyingState", String.class, OptionPrice.class);
        pricesState = getRuntimeContext().getMapState(stateDescription);

        MapStateDescriptor<String, InterestRate> stateRatesDescription = new MapStateDescriptor<>(opName + "-interestRatesState", String.class, InterestRate.class);
        ratesState = getRuntimeContext().getMapState(stateRatesDescription);

        pricesUpdateRequiredState = getRuntimeContext().getState(new ValueStateDescriptor<>(opName + "-pricesUpdateRequiredState", Boolean.class));

        ParameterTool parameterTool = (ParameterTool) getRuntimeContext().getExecutionConfig().getGlobalJobParameters();
        
        WatermarkProperties watermarkProperties = WatermarkProperties.from(parameterTool);
        ApplicationProperties applicationProperties = ApplicationProperties.from(parameterTool);
        debugLogging = applicationProperties.isEnabledExtendedLogging();

        windowAware = WindowGeneratorProvider.create(watermarkProperties);
        contextService = ContextServiceProvider.create(ContextProperties.from(parameterTool), watermarkProperties);
        contextService.init();
        loader = SnapshotSourceFactory.buildInterestRatesLoader(applicationProperties.getSnapshotType());
        loader.init(parameterTool);

        log.debug("WindowAware: {}", windowAware);

    }

    @Override
    public void processBroadcastElement(JobConfig value,
        KeyedBroadcastProcessFunction<String, OptionPrice, JobConfig, SmoothingRequest>.Context ctx,
        Collector<SmoothingRequest> out) throws Exception {
        log.info("Got updated configuration: {}", value);
        ctx.getBroadcastState(CONFIGURATION_DESCRIPTOR).put(CONFIG_KEY, value);
    }

    private boolean extendedLoggingEnabled(SupplierThrowsException<JobConfig> supplier) throws Exception {
        boolean enabled = debugLogging;
        JobConfig jobConfig = supplier.get();
        if (jobConfig != null) {
            // we got state, should use it
            enabled = jobConfig.isExtendedLogging();
        }
        return enabled;
    }

    @Override
    public void processElement(OptionPrice value,
        KeyedBroadcastProcessFunction<String, OptionPrice, JobConfig, SmoothingRequest>.ReadOnlyContext ctx,
        Collector<SmoothingRequest> out) throws Exception {
        long start = System.currentTimeMillis();
        long timestamp = value.getTimestamp();
        String underlying = value.getUnderlying().getName();

        if (extendedLoggingEnabled(() -> ctx.getBroadcastState(CONFIGURATION_DESCRIPTOR).get(CONFIG_KEY))) {
            log.info("{} processing, timestamp: {}", underlying, timestamp);
        }
        boolean pricesUpdateRequired = updateOptionPrice(value);

        if (!Boolean.TRUE.equals(pricesUpdateRequiredState.value())) {
            pricesUpdateRequiredState.update(true);
            log.debug("{} pricesUpdateRequired: {} , duration: {} ms", underlying, pricesUpdateRequired,
                System.currentTimeMillis() - start);
            // Register timer
            WindowContext context = windowAware.generateWindowPeriod(timestamp);
            final long fireTime = context.endOfWindow();
            ctx.timerService().registerEventTimeTimer(fireTime);
        }
    }

    @Override
    public void onTimer(final long timestamp, OnTimerContext ctx, Collector<SmoothingRequest> out) throws Exception {
        super.onTimer(timestamp, ctx, out);

        boolean enabledLogging = extendedLoggingEnabled(() -> ctx.getBroadcastState(CONFIGURATION_DESCRIPTOR).get(CONFIG_KEY));

        final String underlying = ctx.getCurrentKey();
        final WindowContext context = windowAware.generateWindowPeriod(timestamp);
        long nextFireTime = timestamp + context.duration();
        if (log.isDebugEnabled()) {
            log.debug("WindowPeriod: {}, fired timer: {}, nextFireTime: {}, currentWatermark{}", context, timestamp,
                nextFireTime, ctx.timerService().currentWatermark());
        }
        final long windowId = context.getId();
        // Was prices updated for that specific Underlying?
        boolean updatePrices = pricesUpdateRequiredState.value();

        InterestRates loadedRates = InterestRates.EMPTY_RATES;
        ContextMetadata ctxMetadata = contextService.generate(() -> timestamp, InterestRates.class.getSimpleName());
        Optional<SnapshotData<InterestRates>> ratesHolder = loader.loadInterestRates(ctxMetadata);
        if (ratesHolder.isPresent()) {
            loadedRates = ratesHolder.get().getElement();
            if (log.isDebugEnabled()) {
                log.debug(
                    "InterestRates has been loaded from redis, provided contextId: {}, InterestRates belongs to contextId: {}",
                    windowId, ratesHolder.get().getContextId());
            }
        }
        boolean updateRates = updateRates(loadedRates.getRates());

        if (updatePrices || updateRates) {
            if (log.isDebugEnabled()) {
                log.debug("{} Fired. Underlying: {}, updatePrices: {}, updateRates: {}", windowId, ctx.getCurrentKey(),
                    updatePrices, updateRates);
            }
            produce(underlying, windowId, loadedRates, out, enabledLogging);
        }

        // For this Underlying we've finished processing data
        pricesUpdateRequiredState.update(false);
        // Register Timer for this Underlying for periodic check Rates
        ctx.timerService().registerEventTimeTimer(nextFireTime);
    }

    private void produce(String underlyingKey, Long windowId, InterestRates currentRates, Collector<SmoothingRequest> out,
        boolean enabledLogging) throws Exception {
        log.debug("Emitting for Underlying: {}", underlyingKey);
        if (currentRates.empty()) {
            log.debug("{} Underlying: {} rates are empty. Nothing to do", windowId, underlyingKey);
            return;
        }

        Map<String, OptionPrice> prices = loadPrices();
        collect(windowId, underlyingKey, currentRates.getRates(), prices, out, currentRates.getTimestamp(), enabledLogging);
    }

    private Map<String, OptionPrice> loadPrices() throws Exception {
        Map<String, OptionPrice> prices = new HashMap<>();
        for (Map.Entry<String, OptionPrice> entry : pricesState.entries()) {
            prices.put(entry.getKey(), entry.getValue());
        }
        return prices;
    }

    private void collect(Long windowId, String underlying,
                         Map<String, InterestRate> stringInterestRateMap, Map<String, OptionPrice> prices,
                         Collector<SmoothingRequest> out, long timestamp, boolean enabledLogging) {
        SmoothingRequest request = createFrom(underlying, prices, stringInterestRateMap, timestamp);
        if (enabledLogging) {
            log.info("Emit message, key: W{}", windowId);
        }
        out.collect(request);
    }

    private boolean updateRates(Map<String, InterestRate> currentRates) throws Exception {
        boolean updateRequired = false;
        for (String maturity : currentRates.keySet()) {
            InterestRate fromStore = ratesState.get(maturity);
            InterestRate fromCurrentWindow = currentRates.get(maturity);
            if (updateRequired(fromStore, fromCurrentWindow)) {
                log.debug("Updated rate for maturity: {}", maturity);
                // In case of InterestRate wasn't found or data outdated, we need to update storage with latest value
                ratesState.put(maturity, fromCurrentWindow);
                updateRequired = true;
            }
        }
        return updateRequired;
    }

    private boolean updateOptionPrice(OptionPrice currentPrice) throws Exception {
        boolean updateRequired = false;
        String instrumentId = currentPrice.getInstrumentId();
        OptionPrice fromStorage = pricesState.get(instrumentId);

        if (EventUtils.updateRequired(fromStorage, currentPrice)) {
            if (log.isDebugEnabled()) {
                log.debug("Updated price for instrumentId={}, underlyerName={}, keys so far={}",
                    instrumentId, currentPrice.getUnderlying().getName(), pricesState.keys());
            }
            // In case of OptionPrice wasn't found or data outdated, we need to update storage with latest value
            pricesState.put(instrumentId, currentPrice);
            updateRequired = true;
        }

        return updateRequired;
    }

    private boolean updateRequired(Event fromStorage, Event fromCurrentWindow) {
        if (fromStorage == null) {
            return true;
        }
        return !fromStorage.equals(fromCurrentWindow);
    }

    private SmoothingRequest createFrom(String underlyingKey, Map<String, OptionPrice> prices,
                                        Map<String, InterestRate> rates, long timestamp) {
        SmoothingRequest request = new SmoothingRequest();
        request.setTimestamp(timestamp);
        Underlying underlying = new Underlying(underlyingKey);
        request.setUnderlying(underlying);
        request.setOptionPrices(prices);
        request.setInterestRates(rates);
        return request;
    }

    @Override
    public void close() throws Exception {
        super.close();
        if (loader != null) {
            loader.close();
        }
    }
}
