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

package com.riskfocus.flink.example.pipeline.snapshot;

import com.riskfocus.flink.example.pipeline.domain.intermediate.InterestRates;
import com.riskfocus.flink.snapshot.context.ContextMetadata;
import com.riskfocus.flink.snapshot.redis.SnapshotData;

import java.io.IOException;
import java.io.Serializable;
import java.util.Optional;

/**
 * @author Khokhlov Pavel
 */
public interface InterestRatesLoader extends Serializable, AutoCloseable {

    /**
     * Here you can implement any initialization steps
     * (open connection to Redis etc)
     * @throws IOException
     */
    void init() throws IOException;

    /**
     * Get InterestRates by Context
     * @param context context
     * @return InterestRates
     * @throws IOException
     */
    Optional<SnapshotData<InterestRates>> loadInterestRates(ContextMetadata context) throws IOException;
}
