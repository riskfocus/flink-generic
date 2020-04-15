package com.riskfocus.flink.test.example.sender;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.riskfocus.flink.example.pipeline.domain.Account;
import com.riskfocus.flink.example.pipeline.domain.Commodity;
import com.riskfocus.flink.example.pipeline.domain.Customer;
import com.riskfocus.flink.test.common.kafka.KafkaJsonMessageSender;
import com.riskfocus.flink.test.common.kafka.SendInfoHolder;
import com.riskfocus.flink.test.common.metrics.MetricsService;
import com.riskfocus.flink.test.example.config.TestProperties;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;

import java.util.Collection;
import java.util.Map;

/**
 * @author Khokhlov Pavel
 */
@Component
@Slf4j
public class DataSender extends KafkaJsonMessageSender {

    private final TestProperties properties;

    public DataSender(@Qualifier("expandedProducer") KafkaProducer producer, MetricsService metricsService, TestProperties properties) {
        super(producer, metricsService);
        this.properties = properties;
    }

    public Map<String, Customer> sendCustomers(Collection<Customer> customers) throws JsonProcessingException {
        var sendInfoHolder = new SendInfoHolder<String, Customer, Customer>(true);
        for (var customer : customers) {
            sendMessage(properties.getCustomerTopic(), String.valueOf(customer.getCustomerId()), customer, sendInfoHolder);
        }
        sendInfoHolder.waitForAll();
        log.info("All customers have been sent");
        return sendInfoHolder.getSentMessages();
    }

    public Map<String, Account> sendAccounts(Collection<Account> accounts) throws JsonProcessingException {
        var sendInfoHolder = new SendInfoHolder<String, Account, Account>(true);
        for (var account : accounts) {
            sendMessage(properties.getAccountTopic(), String.valueOf(account.getCustomerId()), account, sendInfoHolder);
        }
        sendInfoHolder.waitForAll();
        log.info("All accounts have been sent");
        return sendInfoHolder.getSentMessages();
    }

    public Map<String, Commodity> sendCommodities(Collection<Commodity> commodities) throws JsonProcessingException {
        var sendInfoHolder = new SendInfoHolder<String, Commodity, Commodity>(true);
        for (var commodity : commodities) {
            sendMessage(properties.getCommodityTopic(), String.valueOf(commodity.getCommodityId()), commodity, sendInfoHolder);
        }
        sendInfoHolder.waitForAll();
        log.info("All commodities have been sent");
        return sendInfoHolder.getSentMessages();
    }

}
