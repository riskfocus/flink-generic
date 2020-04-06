package com.riskfocus.flink.snapshot.context.rest;

import lombok.AllArgsConstructor;

import java.net.http.HttpResponse;
import java.util.function.Supplier;

import static com.riskfocus.flink.snapshot.context.rest.util.JsonConverter.asJSON;

/**
 * @author Khokhlov Pavel
 */
@AllArgsConstructor
public class JsonBodyHandler<T> implements HttpResponse.BodyHandler<Supplier<T>> {

    private final Class<T> wClass;

    @Override
    public HttpResponse.BodySubscriber<Supplier<T>> apply(HttpResponse.ResponseInfo responseInfo) {
        return asJSON(wClass);
    }

}