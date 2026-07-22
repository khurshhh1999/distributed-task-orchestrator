package com.orchestrator.api.config;

import com.orchestrator.common.dto.TaskEvent;
import com.orchestrator.common.kafka.KafkaTopics;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.support.serializer.JsonSerializer;

import java.util.HashMap;
import java.util.Map;

@Configuration
public class KafkaProducerConfig {

    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;

    @Bean
    public ProducerFactory<String, TaskEvent> taskEventProducerFactory() {
        Map<String, Object> config = new HashMap<>();
        config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);
        config.put(ProducerConfig.ACKS_CONFIG, "all");
        config.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        return new DefaultKafkaProducerFactory<>(config);
    }

    @Bean
    public KafkaTemplate<String, TaskEvent> taskEventKafkaTemplate() {
        return new KafkaTemplate<>(taskEventProducerFactory());
    }

    @Bean
    public NewTopic taskQueueTopic() {
        return new NewTopic(KafkaTopics.TASK_QUEUE, 6, (short) 1);
    }

    @Bean
    public NewTopic taskRetryTopic() {
        return new NewTopic(KafkaTopics.TASK_RETRY, 3, (short) 1);
    }

    @Bean
    public NewTopic taskDlqTopic() {
        return new NewTopic(KafkaTopics.TASK_DLQ, 3, (short) 1);
    }
}
