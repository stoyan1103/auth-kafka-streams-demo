package com.sap.kafka.config;

import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.google.protobuf.Message;
import com.sap.kafka.producer.IKafkaPublisher;
import com.sap.kafka.producer.KafkaPublisher;
import com.sap.kafka.serializer.ProtobufSerializer;

import static org.apache.kafka.clients.producer.ProducerConfig.ACKS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.BATCH_SIZE_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.CLIENT_ID_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.COMPRESSION_TYPE_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.LINGER_MS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.RETRIES_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG;

@Configuration
public class KafkaProducerConfig {

    @Bean(destroyMethod = "close")
    public Producer<String, Message> producer(
        @Value("${app.kafka.bootstrap-servers}") String bootstrapServers,
        KafkaProducerProperties properties
    ) {
        Properties props = new Properties();
        props.put(BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(CLIENT_ID_CONFIG, properties.clientId());
        props.put(ACKS_CONFIG, properties.acks());
        props.put(LINGER_MS_CONFIG, properties.lingerMs());
        props.put(BATCH_SIZE_CONFIG, properties.batchSize());
        props.put(RETRIES_CONFIG, properties.retries());
        props.put(COMPRESSION_TYPE_CONFIG, properties.compressionType());
        props.put(DELIVERY_TIMEOUT_MS_CONFIG, properties.deliveryTimeoutMs());
        props.put(REQUEST_TIMEOUT_MS_CONFIG, properties.requestTimeoutMs());
        props.put(ENABLE_IDEMPOTENCE_CONFIG, properties.enableIdempotence());
        props.put(KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(VALUE_SERIALIZER_CLASS_CONFIG, ProtobufSerializer.class.getName());

        return new KafkaProducer<>(props);
    }

    @Bean(destroyMethod = "close")
    public IKafkaPublisher<String, Message> kafkaPublisher(Producer<String, Message> producer) {
        return new KafkaPublisher<>(producer);
    }
}
