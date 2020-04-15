package com.riskfocus.flink.test.common.metrics;

import lombok.extern.slf4j.Slf4j;

import java.util.Random;
import java.util.Timer;
import java.util.TimerTask;
import java.util.function.Supplier;

@Slf4j
public class TestUtils {

    /**
     * Creates Timer object which logs some progress (Count and rate/sec) represented by {@code counterSupplier} lambda.
     * <br>It logs progress each 10s with provided {@code message} in a "[count] [message]. Rate is [count/period] messages/sec"
     * @param message to be used in log string
     * @param counterSupplier function where {@code count} is derived from
     * @return
     */
    public static Timer logProgressTimer(String message, Supplier<Integer> counterSupplier) {
        long initialTs = System.currentTimeMillis();
        Timer timer = new Timer();
        timer.schedule(new TimerTask() {
            @Override
            public void run() {
                long period = (System.currentTimeMillis() - initialTs) / 1000;
                int count = counterSupplier.get();
                log.info("{} {}. Rate is {} messages/sec", count, message, count / period);
            }
        }, 10000, 10000);

        return timer;
    }

}
