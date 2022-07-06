/*
 * Copyright 2020-2022 Ness USA, Inc.
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

package com.riskfocus.flink.config;

import com.riskfocus.flink.util.ParamUtils;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author Khokhlov Pavel
 */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public final class CheckpointingConfiguration {

    public static void configure(final ParamUtils params, StreamExecutionEnvironment env) {
        long intervalCheckpointing = params.getLong("checkpointing.interval.ms", 0);
        long minPauseBetweenCheckpoints = params.getLong("min.pause.between.checkpoints.ms", CheckpointConfig.DEFAULT_MIN_PAUSE_BETWEEN_CHECKPOINTS);
        long checkpointTimeout = params.getLong("checkpoint.timeout.ms", CheckpointConfig.DEFAULT_TIMEOUT);
        int maxConcurrentCheckpoints = params.getInt("max.concurrent.checkpoints", CheckpointConfig.DEFAULT_MAX_CONCURRENT_CHECKPOINTS);
        boolean enableExternalizedCheckpoints = params.getBoolean("enable.externalized.checkpoints", false);
        boolean checkpointingTransactionMode = params.getBoolean("checkpointing.transaction.mode", false);
        if (intervalCheckpointing > 0) {
            env.enableCheckpointing(intervalCheckpointing);
            CheckpointConfig checkpointConfig = env.getCheckpointConfig();
            if (checkpointingTransactionMode) {
                checkpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
            } else {
                checkpointConfig.setCheckpointingMode(CheckpointingMode.AT_LEAST_ONCE);
            }
            checkpointConfig.setMinPauseBetweenCheckpoints(minPauseBetweenCheckpoints);
            checkpointConfig.setCheckpointTimeout(checkpointTimeout);
            checkpointConfig.setMaxConcurrentCheckpoints(maxConcurrentCheckpoints);
            if (enableExternalizedCheckpoints) {
                checkpointConfig.enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.DELETE_ON_CANCELLATION);
            }
            checkpointConfig.setPreferCheckpointForRecovery(true);
        }

    }

}
