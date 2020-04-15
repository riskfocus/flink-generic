package com.riskfocus.flink.test.example;

import com.riskfocus.flink.test.example.config.TestConfig;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.testng.annotations.Test;

import java.time.ZoneId;

/**
 * @author Khokhlov Pavel
 */
@Slf4j
@SpringBootTest
@ContextConfiguration(classes = {TestConfig.class})
public class PurchaseIT extends AbstractTestNGSpringContextTests {

    @Test
    public void purchaseTest() throws Exception {
        log.info("Time zone is: {}", ZoneId.systemDefault());
    }

}
