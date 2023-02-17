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

package com.ness.flink.example.pipeline.config;

import lombok.AllArgsConstructor;
import lombok.Getter;

/**
 * @author Khokhlov Pavel
 */
@Getter
@AllArgsConstructor
public enum JobMode {

    INTEREST_RATES_ONLY("smoothing-interest-rates"),
    OPTION_PRICES_ONLY("smoothing-option-prices"),
    FULL("smoothing-full");

    private final String jobName;
}