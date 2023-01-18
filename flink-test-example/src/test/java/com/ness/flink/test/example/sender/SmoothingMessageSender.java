package com.ness.flink.test.example.sender;

import com.ness.flink.example.pipeline.domain.OptionPrice;
import com.ness.flink.test.common.metrics.MetricsService;
import com.ness.flink.example.pipeline.domain.InterestRate;
import com.ness.flink.test.common.util.Throttle;
import com.ness.flink.test.common.kafka.KafkaJsonMessageSender;
import com.ness.flink.test.common.kafka.SendInfoHolder;
import com.ness.flink.test.example.SmoothingIT;
import com.ness.flink.test.example.config.TestProperties;
import com.ness.flink.test.example.data.MockDataGenerator;
import com.ness.flink.window.WindowAware;
import com.ness.flink.window.WindowContext;
import com.ness.flink.window.generator.impl.BasicGenerator;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * @author Khokhlov Pavel
 */
@Component
@Slf4j
public class SmoothingMessageSender extends KafkaJsonMessageSender {

    private final Random random = new Random();

    private final TestProperties properties;
    private final List<String> underliers;
    private final List<String> maturities;
    private final WindowAware windowIdGenerator;
    private final Throttle throttle;

    public SmoothingMessageSender(@Qualifier("expandedProducer") KafkaProducer producer, MetricsService metricsService, TestProperties properties) {
        super(producer, metricsService);
        this.properties = properties;
        this.underliers = IntStream.range(0, properties.getNumberOfUnderliers())
                .mapToObj(String::valueOf).collect(Collectors.toList());
        this.maturities = IntStream.range(0, properties.getRangeOfMaturitiesInterestRates())
                .mapToObj(p -> p + "Y").collect(Collectors.toList());

        this.windowIdGenerator = new BasicGenerator(properties.getOptionPricesWindowDurationMs());
        // defines throttling for throughput management
        this.throttle = Throttle.ofPerSec(properties.getMessagesPerSec());
    }

    public Map<String, ExpectedResultHolder> sendMessages(ExpectedResultHolder expectedInitial) {
        // windowId-> instrumentId -> price
        Map<String, OptionPrice> latestPrices = expectedInitial.getData();
        Map<String, InterestRate> latestRates = expectedInitial.getRates();
        Map<String, ExpectedResultHolder> resultsWrapper = new TreeMap<>();

        long start = System.currentTimeMillis();
        log.info("Start sending: properties={}", properties);
        if (properties.getNumberOfInterestRates() > 0) {
            log.info("Interest rate generation enabled");
        }

        int windowsSent = 0;
        long messagesSent = 0;
        while (windowsSent < properties.getNumberOfWindows()) {
            ExpectedResultHolder sendResult = sendWindow(latestPrices, latestRates);
            resultsWrapper.put(sendResult.getKey(), sendResult);
            messagesSent += sendResult.getCount();
            windowsSent++;
        }

        double producingSeconds = (System.currentTimeMillis() - start) / 1000;
        log.info("Messages sent: count={}, seconds passed={}, raw throughput={}", messagesSent, producingSeconds, messagesSent / producingSeconds);

        if (properties.isStrictWindowCheck()) {
            log.info("Checking: keys={}", resultsWrapper.keySet());
        }

        return resultsWrapper;
    }

    public ExpectedResultHolder sendInitialMessages() {
        long timestamp = System.currentTimeMillis();
        log.info("Sending initial prices & rates: enabled={}, timestamp={}", properties.isSendInitialPrices(), timestamp);

        SendInfoHolder<String, InterestRate, InterestRate> ratesHolder = new SendInfoHolder<>();
        //create the full set of Interest rates and send them
        maturities.forEach(m -> {
            InterestRate rate = MockDataGenerator.generateRate(m, timestamp);
            rate.setTimestamp(timestamp);
            sendMessage(properties.getIrInbound(), timestamp, m, rate, ratesHolder);
        });

        SendInfoHolder<String, OptionPrice, OptionPrice> pricesHolder = new SendInfoHolder<>();
        underliers.forEach(u ->
                IntStream.range(1, properties.getNumberOfInstruments() + 1).forEach(i -> {
                    String instrumentId = u + "_" + i;
                    OptionPrice optionPrice = MockDataGenerator.generatePrice(u, instrumentId, timestamp);
                    //send message
                    sendMessage(properties.getOptionPricesInbound(), timestamp, instrumentId, optionPrice, pricesHolder);
                }));
        //wait until all prices have been sent (or we got some errors)
        pricesHolder.waitForAll();
        ratesHolder.waitForAll();
        log.info("All initial prices & rates have been sent in {} ms", System.currentTimeMillis() - timestamp);

        String key = properties.isStrictWindowCheck() ? generate(timestamp) : SmoothingIT.LATEST;
        return ExpectedResultHolder.of(key, pricesHolder.getLastSendTime(), pricesHolder.getSent(),
                pricesHolder.getSentMessages(), ratesHolder.getSentMessages()
        );
    }

    private ExpectedResultHolder sendWindow(Map<String, OptionPrice> latestPrices, Map<String, InterestRate> latestRates) {
        float interestRateGenerationPosibility = 0.7f;
        final String currentWindow = generate(System.currentTimeMillis());

        //structure of InstrumentId - OptionPrice
        SendInfoHolder<String, OptionPrice, OptionPrice> pricesHolder = new SendInfoHolder<>();
        SendInfoHolder<String, InterestRate, InterestRate> ratesHolder = new SendInfoHolder<>();
        //send control prices
        sendControlPrices(currentWindow, pricesHolder);
        for (long ts = System.currentTimeMillis(); currentWindow.equals(generate(ts)); ts = System.currentTimeMillis()) {

            //throttle message production
            try {
                throttle.increment();
            } catch (InterruptedException e) {
                throw new RuntimeException("Interrupted: " + e.getMessage(), e);
            }

            OptionPrice optionPrice = MockDataGenerator.generateRandomPrice(underliers, properties.getNumberOfInstruments(), ts);
            sendMessage(properties.getOptionPricesInbound(), ts, optionPrice.getInstrumentId(), optionPrice, pricesHolder);

            if (ratesHolder.getExpected() < properties.getNumberOfInterestRates() && getRandomBoolean(interestRateGenerationPosibility, random)) {
                InterestRate interestRate = MockDataGenerator.generateRandomRate(maturities, ts);
                sendMessage(properties.getIrInbound(), ts, interestRate.getMaturity(), interestRate, ratesHolder);
            }
        }
        //wait for all messages to be sent
        pricesHolder.waitForAll();
        ratesHolder.waitForAll();

        log.info("Window has been sent: id={}, InterestRates count per window: {}", currentWindow, ratesHolder.getSent());
        //update rates with the latest sent
        latestRates.putAll(ratesHolder.getSentMessages());

        Map<String, OptionPrice> expectedPrices;
        String resultKey;
        if (properties.isStrictWindowCheck()) {
            //if strictWindowCheck -> put in expected prices only those which were sent in the current window
            expectedPrices = pricesHolder.getSentMessages();
            resultKey = currentWindow;
        } else {
            //otherwise update latest prices with prices from the current window
            latestPrices.putAll(pricesHolder.getSentMessages());
            expectedPrices = latestPrices;
            resultKey = SmoothingIT.LATEST;
        }

        return ExpectedResultHolder.of(resultKey, pricesHolder.getLastSendTime(), pricesHolder.getSent(),
                expectedPrices, new HashMap<>(latestRates));
    }

    /**
     * Sends window related prices to each partition
     */
    private void sendControlPrices(String windowId, SendInfoHolder<String, OptionPrice, OptionPrice> sendInfoHolder) {
        IntStream.range(0, properties.getNumberOfPartitions()).forEach(i -> {
            String instrumentId = windowId + "_" + i;
            long ts = System.currentTimeMillis();
            OptionPrice price = MockDataGenerator.generatePrice(windowId, instrumentId, ts);
            sendMessage(properties.getOptionPricesInbound(), ts, instrumentId, price, sendInfoHolder);
        });
    }

    private boolean getRandomBoolean(float posibility, Random random) {
        return random.nextFloat() < posibility;
    }

    private String generate(long timestamp) {
        WindowContext windowContext = windowIdGenerator.generateWindowPeriod(timestamp);
        return "W" + windowContext.getId();
    }
}
