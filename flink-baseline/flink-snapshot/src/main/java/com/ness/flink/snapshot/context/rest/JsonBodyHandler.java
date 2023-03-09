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

package com.ness.flink.snapshot.context.rest;

import com.ness.flink.snapshot.context.rest.util.JsonConverter;
import lombok.AllArgsConstructor;

import java.net.http.HttpResponse;
import java.util.function.Supplier;

/**
 * @author Khokhlov Pavel
 */
@AllArgsConstructor
public class JsonBodyHandler<T> implements HttpResponse.BodyHandler<Supplier<T>> {

    private final Class<T> wClass;

    @Override
    public HttpResponse.BodySubscriber<Supplier<T>> apply(HttpResponse.ResponseInfo responseInfo) {
        return JsonConverter.asJSON(wClass);
    }

}