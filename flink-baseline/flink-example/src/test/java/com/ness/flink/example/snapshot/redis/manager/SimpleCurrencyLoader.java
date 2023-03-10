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

package com.ness.flink.example.snapshot.redis.manager;

import com.ness.flink.example.snapshot.redis.domain.SimpleCurrency;
import com.ness.flink.snapshot.context.ContextMetadata;
import com.ness.flink.snapshot.redis.SnapshotData;

import java.io.IOException;
import java.io.Serializable;
import java.util.Optional;

/**
 * @author Khokhlov Pavel
 */
public interface SimpleCurrencyLoader extends Serializable, AutoCloseable {

    /**
     * Here you can implement any initialization steps
     * (open connection to Database/Redis etc)
     *
     * @throws IOException
     */
    void init() throws IOException;


    Optional<SnapshotData<SimpleCurrency>> loadSimpleCurrency(ContextMetadata contextMetadata, String code) throws IOException;

}