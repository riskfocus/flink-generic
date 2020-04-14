package com.riskfocus.flink.example.pipeline.manager.stream;

import com.riskfocus.flink.example.pipeline.config.channel.OrderChannelFactory;
import com.riskfocus.flink.example.pipeline.domain.Order;
import com.riskfocus.flink.example.pipeline.domain.Purchase;
import com.riskfocus.flink.example.pipeline.domain.intermediate.CustomerAndAccount;
import com.riskfocus.flink.example.pipeline.domain.intermediate.CustomerAndAccountAndOrder;
import com.riskfocus.flink.example.pipeline.manager.stream.function.OrderAggregator;
import com.riskfocus.flink.util.ParamUtils;
import com.riskfocus.flink.window.generator.WindowGeneratorProvider;
import lombok.Builder;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

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

        DataStream<CustomerAndAccountAndOrder> customerAndAccountAndOrderStream = customerAndAccountDataStream
                .join(orderDataStream)
                .where(value -> value.getCustomer().getCustomerId())
                .equalTo(Order::getCustomerId)
                .window(TumblingEventTimeWindows.of(Time.milliseconds(windowSize)))
                .apply((customerAndAccount, order) -> {
                    CustomerAndAccountAndOrder ordered = CustomerAndAccountAndOrder.builder().account(customerAndAccount.getAccount())
                            .customer(customerAndAccount.getCustomer()).order(order).build();
                    ordered.setTimestamp(order.getTimestamp());
                    return ordered;
                });

        DataStream<Purchase> aggregatedStream = customerAndAccountAndOrderStream
                .keyBy(value -> value.getCustomer().getCustomerId())
                .window(TumblingEventTimeWindows.of(Time.milliseconds(windowSize)))
                .aggregate(new OrderAggregator()).name("orderAggregator").uid("orderAggregator");

        channelFactory.buildSinks(paramUtils).forEach(sink -> aggregatedStream.addSink(sink.getFunction())
                .name(sink.getName()).uid(sink.getName()));

    }
}
