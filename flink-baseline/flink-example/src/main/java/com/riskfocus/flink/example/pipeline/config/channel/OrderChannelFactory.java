package com.riskfocus.flink.example.pipeline.config.channel;

import com.riskfocus.flink.config.channel.SinkInfo;
import com.riskfocus.flink.config.channel.SinkUtils;
import com.riskfocus.flink.config.channel.Source;
import com.riskfocus.flink.example.pipeline.config.sink.PurchaseKafkaSink;
import com.riskfocus.flink.example.pipeline.config.source.OrderKafkaSource;
import com.riskfocus.flink.example.pipeline.domain.Order;
import com.riskfocus.flink.example.pipeline.domain.Purchase;
import com.riskfocus.flink.util.ParamUtils;

import java.util.Collection;

/**
 * @author Khokhlov Pavel
 */
public class OrderChannelFactory {
    public Source<Order> buildCustomerSource(ParamUtils paramUtils) {
        return new OrderKafkaSource(paramUtils);
    }

    public Collection<SinkInfo<Purchase>> buildSinks(ParamUtils paramUtils) {
        SinkUtils<Purchase> sinkUtils = new SinkUtils<>();
        return sinkUtils.build(new PurchaseKafkaSink(paramUtils));
    }
}
