package com.riskfocus.flink.test.example;

import com.riskfocus.flink.example.pipeline.domain.Account;
import com.riskfocus.flink.example.pipeline.domain.Commodity;
import com.riskfocus.flink.example.pipeline.domain.Customer;
import com.riskfocus.flink.test.example.config.TestConfig;
import com.riskfocus.flink.test.example.config.TestProperties;
import com.riskfocus.flink.test.example.data.MockDataGenerator;
import com.riskfocus.flink.test.example.sender.DataSender;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.testng.annotations.Test;

import java.time.ZoneId;
import java.util.Collection;

/**
 * @author Khokhlov Pavel
 */
@Slf4j
@SpringBootTest
@ContextConfiguration(classes = {TestConfig.class})
public class PurchaseIT extends AbstractTestNGSpringContextTests {

    @Autowired
    private TestProperties testProperties;
    @Autowired
    private DataSender sender;

    @Test
    public void purchaseTest() throws Exception {
        log.info("Time zone is: {}", ZoneId.systemDefault());
        int customersCount = testProperties.getCustomersCount();
        int commoditiesCount = testProperties.getCommoditiesCount();

        Collection<Customer> customers = MockDataGenerator.generateCustomers(customersCount);
        Collection<Account> accounts = MockDataGenerator.generateAccounts(customers);
        Collection<Commodity> commodities = MockDataGenerator.generateCommodities(commoditiesCount);

        sender.sendCustomers(customers);
        sender.sendAccounts(accounts);
        sender.sendCommodities(commodities);

        sender.close();


    }

}
