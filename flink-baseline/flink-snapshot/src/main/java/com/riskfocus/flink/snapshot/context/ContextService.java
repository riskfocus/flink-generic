/*
 * Copyright 2020 Risk Focus Inc
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

package com.riskfocus.flink.snapshot.context;

import com.riskfocus.flink.domain.TimeAware;

/**
 * @author Khokhlov Pavel
 */
public interface ContextService extends AutoCloseable {

    /**
     * See create method.
     *
     * @param timeAware   element which can provide timestamp
     * @param contextName name of provided context
     * @return context ready for use
     */
    ContextMetadata generate(TimeAware timeAware, String contextName);

    /**
     * Service initialization
     */
    void init();

}
