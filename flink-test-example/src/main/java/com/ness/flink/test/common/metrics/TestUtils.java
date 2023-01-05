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

package com.ness.flink.test.common.metrics;

import lombok.extern.slf4j.Slf4j;

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
     * @return timer
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
