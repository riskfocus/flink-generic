package com.riskfocus.flink.test.example.config;

import com.riskfocus.flink.test.common.metrics.MetricsService;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.springframework.boot.actuate.autoconfigure.metrics.KafkaMetricsAutoConfiguration;
import org.springframework.boot.actuate.autoconfigure.metrics.web.tomcat.TomcatMetricsAutoConfiguration;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.autoconfigure.data.redis.RedisAutoConfiguration;
import org.springframework.boot.autoconfigure.data.redis.RedisRepositoriesAutoConfiguration;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

import java.util.Map;
import java.util.Properties;

/**
 * @author Khokhlov Pavel
 */
@Configuration
@ComponentScan(basePackages = {"com.riskfocus.flink.test"})
@Import({TestProperties.class})
@EnableAutoConfiguration(exclude = {RedisAutoConfiguration.class, RedisRepositoriesAutoConfiguration.class, KafkaMetricsAutoConfiguration.class, TomcatMetricsAutoConfiguration.class,})
@Slf4j
public class TestConfig {

    @Bean("expandedProducer")
    public KafkaProducer<String, byte[]> createProducer(KafkaProperties kafkaProperties) {
        Map<String, Object> producerProperties = kafkaProperties.buildProducerProperties();
        Properties props = new Properties();
        props.putAll(producerProperties);

        return new KafkaProducer<>(props);
    }

    @Bean
    public MetricsService metricsService() {
        return new MetricsService();
    }

}
