package com.riskfocus.flink.example.pipeline.manager.stream.function;

import com.riskfocus.flink.config.redis.RedisProperties;
import com.riskfocus.flink.domain.Event;
import com.riskfocus.flink.example.pipeline.config.SmoothingConfig;
import com.riskfocus.flink.example.pipeline.domain.InterestRate;
import com.riskfocus.flink.example.pipeline.domain.OptionPrice;
import com.riskfocus.flink.example.pipeline.domain.SmoothingRequest;
import com.riskfocus.flink.example.pipeline.domain.Underlying;
import com.riskfocus.flink.example.pipeline.domain.intermediate.InterestRates;
import com.riskfocus.flink.example.pipeline.snapshot.InterestRatesLoader;
import com.riskfocus.flink.example.pipeline.snapshot.SnapshotSourceFactory;
import com.riskfocus.flink.snapshot.context.ContextMetadata;
import com.riskfocus.flink.snapshot.context.ContextService;
import com.riskfocus.flink.snapshot.context.ContextServiceProvider;
import com.riskfocus.flink.snapshot.redis.SnapshotData;
import com.riskfocus.flink.util.EventUtils;
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

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;


/**
 * @author Khokhlov Pavel
 */
@Slf4j
public class ProcessSmoothingFunction extends KeyedProcessFunction<String, OptionPrice, SmoothingRequest> {

    private static final long serialVersionUID = -4848961500498134626L;

    private transient MapState<String, OptionPrice> pricesState;
    private transient MapState<String, InterestRate> ratesState;
    private transient ValueState<Boolean> pricesUpdateRequiredState;

    private transient WindowAware windowAware;
    private transient ContextService contextService;
    private transient InterestRatesLoader loader;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);

        String opName = getClass().getName();

        MapStateDescriptor<String, OptionPrice> stateDescription = new MapStateDescriptor<>(opName + "-optionPriceByUnderlyingState", String.class, OptionPrice.class);
        pricesState = getRuntimeContext().getMapState(stateDescription);

        MapStateDescriptor<String, InterestRate> stateRatesDescription = new MapStateDescriptor<>(opName + "-interestRatesState", String.class, InterestRate.class);
        ratesState = getRuntimeContext().getMapState(stateRatesDescription);

        pricesUpdateRequiredState = getRuntimeContext().getState(new ValueStateDescriptor<>(opName + "-pricesUpdateRequiredState", Boolean.class));

        ParameterTool globalParams = (ParameterTool) getRuntimeContext().getExecutionConfig().getGlobalJobParameters();
        ParamUtils paramUtils = new ParamUtils(globalParams);

        SmoothingConfig smoothingConfig = new SmoothingConfig(paramUtils);
        RedisProperties redisProperties = new RedisProperties(paramUtils);

        windowAware = WindowGeneratorProvider.create(paramUtils);
        contextService = ContextServiceProvider.create(paramUtils);
        contextService.init();
        loader = SnapshotSourceFactory.buildInterestRatesLoader(smoothingConfig.getSnapshotSinkType(), redisProperties);
        loader.init();

        log.debug("WindowAware: {}", windowAware);

    }

    @Override
    public void processElement(OptionPrice value, Context ctx, Collector<SmoothingRequest> out) throws Exception {
        long start = System.currentTimeMillis();
        long timestamp = value.getTimestamp();
        String underlying = value.getUnderlying().getName();
        log.debug("{} processing, timestamp: {}", underlying, timestamp);
        boolean pricesUpdateRequired = updateOptionPrice(value);

        long fireTime;
        if (!Boolean.TRUE.equals(pricesUpdateRequiredState.value())) {
            pricesUpdateRequiredState.update(true);

            log.debug("{} pricesUpdateRequired: {} , duration: {} ms", underlying, pricesUpdateRequired, System.currentTimeMillis() - start);
            // Register timer
            WindowContext context = windowAware.generateWindowPeriod(timestamp);
            fireTime = context.endOfWindow();
            ctx.timerService().registerEventTimeTimer(fireTime);
        }
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<SmoothingRequest> out) throws Exception {
        super.onTimer(timestamp, ctx, out);

        if (timestamp == Long.MAX_VALUE) {
            timestamp = System.currentTimeMillis();
        }

        final String underlying = ctx.getCurrentKey();
        final WindowContext context = windowAware.generateWindowPeriod(timestamp);
        long nextFireTime = timestamp + context.duration();
        log.debug("WindowPeriod: {}, fired timer: {}, nextFireTime: {}, currentWatermark{}", context, timestamp, nextFireTime, ctx.timerService().currentWatermark());
        final long windowId = context.getId();
        // Was prices updated for that specific Underlying?
        boolean updatePrices = pricesUpdateRequiredState.value();

        InterestRates loadedRates = InterestRates.EMPTY;
        final long ctxTime = timestamp;
        ContextMetadata ctxMetadata = contextService.generate(() -> ctxTime, InterestRates.class.getSimpleName());
        Optional<SnapshotData<InterestRates>> ratesHolder = loader.loadInterestRates(ctxMetadata);
        if (ratesHolder.isPresent()) {
            loadedRates = ratesHolder.get().getElement();
            log.debug("InterestRates has been loaded from redis, provided contextId: {}, InterestRates belongs to contextId: {}", windowId, ratesHolder.get().getContextId());
        }
        boolean updateRates = updateRates(loadedRates.getRates());

        if (updatePrices || updateRates) {
            log.debug("{} Fired. Underlying: {}, updatePrices: {}, updateRates: {}", windowId, ctx.getCurrentKey(), updatePrices, updateRates);
            produce(underlying, windowId, loadedRates, out);
        }

        // For this Underlying we've finished processing data
        pricesUpdateRequiredState.update(false);
        // Register Timer for this Underlying for periodic check Rates
        if (ctx.timerService().currentWatermark() != Long.MAX_VALUE) {
            ctx.timerService().registerEventTimeTimer(nextFireTime);
        }
    }

    private void produce(String underlyingKey, Long windowId, InterestRates currentRates, Collector<SmoothingRequest> out) throws Exception {
        log.debug("Emitting for Underlying: {}", underlyingKey);

        if (currentRates.empty()) {
            log.debug("{} Underlying: {} rates are empty. Nothing to do", windowId, underlyingKey);
            return;
        }

        Map<String, OptionPrice> prices = loadPrices();
        collect(windowId, underlyingKey, currentRates.getRates(), prices, out);
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
                         Collector<SmoothingRequest> out) {
        SmoothingRequest request = createFrom(underlying, prices, stringInterestRateMap);
        log.info("Emit message, key: W{}", windowId);
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
            log.debug("Updated price for instrumentId: {}, Underlyer Id : {}, thread = {}, keys so far = {}", instrumentId, currentPrice.getUnderlying().getName(), Thread.currentThread().getId(), pricesState.keys());
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
                                        Map<String, InterestRate> rates) {
        SmoothingRequest request = new SmoothingRequest();
        Underlying underlying = Underlying.of(underlyingKey);
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
