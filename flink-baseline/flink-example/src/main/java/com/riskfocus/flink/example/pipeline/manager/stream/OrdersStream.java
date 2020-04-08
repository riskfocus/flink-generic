package com.riskfocus.flink.example.pipeline.manager.stream;

import com.riskfocus.flink.example.pipeline.config.channel.OrderChannelFactory;
import com.riskfocus.flink.example.pipeline.domain.Order;
import com.riskfocus.flink.example.pipeline.domain.Purchase;
import com.riskfocus.flink.example.pipeline.domain.intermediate.CustomerAndAccount;
import com.riskfocus.flink.util.ParamUtils;
import com.riskfocus.flink.window.generator.WindowGeneratorProvider;
import lombok.Builder;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.util.ArrayList;
import java.util.List;

/**
 * @author Khokhlov Pavel
 */
@Slf4j
@Builder
public class OrdersStream {

    public static void build(@NonNull ParamUtils paramUtils, StreamExecutionEnvironment env, @NonNull OrderChannelFactory channelFactory,
                             DataStream<CustomerAndAccount> customerAndAccountDataStream) {
        long windowSize = WindowGeneratorProvider.getWindowSize(paramUtils);
        DataStream<Order> orderDataStream = channelFactory.buildCustomerSource(paramUtils).build(env);

        DataStream<Purchase> purchaseDataStream = customerAndAccountDataStream
                .join(orderDataStream)
                .where(value -> value.getCustomer().getCustomerId())
                .equalTo(Order::getCustomerId)
                .window(TumblingEventTimeWindows.of(Time.milliseconds(windowSize)))
                .apply((JoinFunction<CustomerAndAccount, Order, Purchase>) (customerAndAccount, order) -> {
                    // todo
                    List<Order> orders = new ArrayList<>();
                    orders.add(order);
                    return Purchase.builder()
                            .customerId(customerAndAccount.getCustomer().getCustomerId())
                            .orders(orders)
                            .build();
                });
        channelFactory.buildSinks(paramUtils).forEach(sink -> purchaseDataStream.addSink(sink.getFunction())
                .name(sink.getName()).uid(sink.getName()));

    }
}
