package com.sap.kafka.topic.creator.config;

import java.util.Properties;

import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.sap.kafka.topic.creator.admin.IKafkaTopicAdmin;
import com.sap.kafka.topic.creator.admin.impl.KafkaTopicAdmin;

import static org.apache.kafka.clients.admin.AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.kafka.clients.admin.AdminClientConfig.CLIENT_ID_CONFIG;

@Configuration
public class KafkaTopicCreatorConfig {

    @Bean(name = "kafkaAdminClientProperties")
    public Properties kafkaAdminClientProperties(KafkaAdminProperties kafkaAdminProperties) {
        Properties props = new Properties();
        props.put(BOOTSTRAP_SERVERS_CONFIG, kafkaAdminProperties.bootstrapServers());
        props.put(CLIENT_ID_CONFIG, kafkaAdminProperties.clientId());

        return props;
    }

    /*
     * This bean is intentionally initialized as part of the application context lifecycle.
     * The module is designed to run as a short-lived provisioning job:
     * create/update Kafka topics during init, then the main method closes the context.
     */
    @Bean(initMethod = "init", destroyMethod = "destroy")
    public IKafkaTopicAdmin kafkaTopicAdmin(
        @Qualifier("kafkaAdminClientProperties") Properties kafkaAdminClientProperties,
        KafkaTopicDefaultsProperties topicDefaultsProperties,
        KafkaAdminProperties kafkaAdminProperties
    ) {
        return new KafkaTopicAdmin(
            kafkaAdminClientProperties,
            topicDefaultsProperties,
            kafkaAdminProperties.operationTimeoutSeconds());
    }
}
