package com.riskfocus.flink.test.example.config;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * @author Khokhlov Pavel
 */
@ConfigurationProperties(prefix = "smoothing")
@Data
public class TestProperties {

    /**
     * Option prices inbound {@link com.riskfocus.flink.example.pipeline.domain.OptionPrice}
     */
    private String optionPricesInbound = "optionsPricesLive";

    /**
     * Interest rates inbound {@link com.riskfocus.flink.example.pipeline.domain.InterestRate}
     */
    private String irInbound = "ycInputsLive";

    /**
     * Stores messages prepared for smoothing. Easch message is an underlier with its latest option prices
     * and IR for current window {@link com.riskfocus.flink.example.pipeline.domain.SmoothingRequest}
     */
    private String smoothingInput = "smoothingInputsLatest";


    /**
     * Duration of Window in ms
     */
    private long optionPricesWindowDurationMs;
    /**
     * Throttling parameter for {@link com.riskfocus.flink.test.example.SmoothingIT}
     */
    private int messagesPerSec;
    /**
     * Controls when to stop sending messages in {@link com.riskfocus.flink.test.example.SmoothingIT}
     */
    private int numberOfWindows;
    /**
     * How many unique underliers to generate
     */
    private int numberOfUnderliers;
    /**
     * How many instruments (options) per underlier
     */
    private int numberOfInstruments;
    /**
     * How many InterestRates generates per Window
     */
    private int numberOfInterestRates;
    /**
     * Range of Maturities which are using for generates InterestRates
     */
    private int rangeOfMaturitiesInterestRates;
    /**
     * Shall we check that application emits more then one window with the same Underlyer/Maturity
     */
    private boolean strictWindowCheck;
    /**
     * Shall we send initial price array
     */
    private boolean sendInitialPrices;
    /**
     * Number of price input topic partitions for control prices send.
     * Should be aligned with actual number of partitions
     */
    private int numberOfPartitions;

    /**
     * How long test should wait.
     * Windows duration + this extra time
     */
    private long waitExtraDurationMs;

    /**
     * Shall test ignore intermediate results or not (filter out duplicated messages based on timestamp)
     */
    private boolean checkTimestamp;

}
