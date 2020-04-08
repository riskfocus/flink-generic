package com.riskfocus.flink.example.pipeline.manager.stream;

import com.riskfocus.flink.config.channel.SinkInfo;
import com.riskfocus.flink.config.channel.Source;
import com.riskfocus.flink.example.pipeline.config.channel.ChannelProperties;
import com.riskfocus.flink.example.pipeline.config.channel.CustomerAccountChannelFactory;
import com.riskfocus.flink.example.pipeline.domain.Account;
import com.riskfocus.flink.example.pipeline.domain.Customer;
import com.riskfocus.flink.example.pipeline.domain.intermediate.CustomerAndAccount;
import com.riskfocus.flink.util.ParamUtils;
import com.riskfocus.flink.window.generator.WindowGeneratorProvider;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.NonNull;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.util.Collection;

/**
 * @author Khokhlov Pavel
 */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class CustomerAccountStream {

    public static Source<CustomerAndAccount> build(@NonNull ParamUtils paramUtils,
                                                   @NonNull StreamExecutionEnvironment env,
                                                   @NonNull ChannelProperties properties,
                                                   @NonNull CustomerAccountChannelFactory channelFactory) {
        if (properties.isChannelSplit()) {
            return channelFactory.buildCustomerAndAccountSource(paramUtils);
        } else {
            return source -> {
                DataStream<Customer> customerSrc = channelFactory.buildCustomerSource(paramUtils).build(env);
                DataStream<Account> accountSrc = channelFactory.buildAccountSource(paramUtils).build(env);
                Collection<SinkInfo<CustomerAndAccount>> sinks = channelFactory.buildSinks(paramUtils);
                long windowSize = WindowGeneratorProvider.getWindowSize(paramUtils);
                DataStream<CustomerAndAccount> joined = customerSrc
                        .join(accountSrc).where((KeySelector<Customer, Integer>) Customer::getCustomerId)
                        .equalTo((KeySelector<Account, Integer>) Account::getCustomerId)
                        .window(TumblingEventTimeWindows.of(Time.milliseconds(windowSize)))
                        .apply((JoinFunction<Customer, Account, CustomerAndAccount>) (customer, account) -> {
                            CustomerAndAccount customerAndAccount = CustomerAndAccount.builder().account(account).customer(customer).build();
                            customerAndAccount.setTimestamp(customer.getTimestamp());
                            return customerAndAccount;
                        });
                sinks.forEach(sink -> joined.addSink(sink.getFunction())
                        .name(sink.getName()).uid(sink.getName()));
                return joined;
            };
        }
    }
}
