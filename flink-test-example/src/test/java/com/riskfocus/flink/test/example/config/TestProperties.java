package com.riskfocus.flink.test.example.config;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * @author Khokhlov Pavel
 */
@ConfigurationProperties(prefix = "example.test")
@Data
public class TestProperties {

    private int customersCount;
    private int commoditiesCount;
    private int ordersCount;

    private String commodityTopic = "commodity";
    private String customerTopic = "customer";
    private String accountTopic = "account";
    private String orderRequestTopic = "order-request";

}
