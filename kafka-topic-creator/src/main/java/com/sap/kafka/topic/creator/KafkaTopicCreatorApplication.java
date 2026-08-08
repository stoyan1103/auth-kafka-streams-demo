package com.sap.kafka.topic.creator;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.ConfigurationPropertiesScan;

@SpringBootApplication
@ConfigurationPropertiesScan
public class KafkaTopicCreatorApplication {
    public static void main(String[] args) {
        SpringApplication.run(KafkaTopicCreatorApplication.class, args);
    }
}
