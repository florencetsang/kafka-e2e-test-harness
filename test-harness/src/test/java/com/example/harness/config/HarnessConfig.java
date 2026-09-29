package com.example.harness.config;

import com.example.common.Message;
import com.example.harness.kafka.OutputCollector;
import com.example.harness.kafka.OutputTopicListener;
import com.example.harness.lifecycle.AppLifecycleManager;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.KafkaListenerContainerFactory;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.support.serializer.JsonSerializer;

import java.util.HashMap;
import java.util.Map;

/**
 * Explicit Kafka wiring for the harness context.
 *
 * <p>Deliberately does NOT rely on Spring Boot's Kafka autoconfiguration:
 * the app modules' application.yml files are on the harness test classpath
 * too, so the harness defines its own producer, consumer factory, listener
 * container factory, admin and topics directly.</p>
 *
 * <p>Every Kafka bean resolves its bootstrap address from
 * {@link KafkaContainerHolder#startIfNeeded()} (idempotent), which sets the
 * {@code spring.kafka.bootstrap-servers} system property. Calling it from
 * the bean factory methods guarantees the container is up and the property
 * is set even if this context refreshes before the cucumber hooks run —
 * the beans can never silently bind to the {@code localhost:9092} default.</p>
 */
@Configuration
@EnableKafka
public class HarnessConfig {

    /** Consumer group of the harness output collector (distinct from app1/app2). */
    public static final String COLLECTOR_GROUP = "harness-collector";

    @Bean
    public ProducerFactory<String, Message> producerFactory() {
        Map<String, Object> props = new HashMap<>();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaContainerHolder.startIfNeeded());
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);
        return new DefaultKafkaProducerFactory<>(props);
    }

    /** Publishes scenario inputs to topic a. */
    @Bean
    public KafkaTemplate<String, Message> kafkaTemplate(ProducerFactory<String, Message> producerFactory) {
        return new KafkaTemplate<>(producerFactory);
    }

    @Bean
    public ConsumerFactory<String, Message> consumerFactory() {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaContainerHolder.startIfNeeded());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, COLLECTOR_GROUP);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JsonDeserializer.class);
        props.put(JsonDeserializer.TRUSTED_PACKAGES, "com.example.common");
        props.put(JsonDeserializer.VALUE_DEFAULT_TYPE, Message.class);
        return new DefaultKafkaConsumerFactory<>(props);
    }

    /**
     * Listener container factory used by the harness collector only
     * (referenced by name from OutputTopicListener).
     */
    @Bean(name = "harnessListenerContainerFactory")
    public KafkaListenerContainerFactory<?> harnessListenerContainerFactory(
            ConsumerFactory<String, Message> consumerFactory) {
        ConcurrentKafkaListenerContainerFactory<String, Message> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory);
        return factory;
    }

    @Bean
    public KafkaAdmin kafkaAdmin() {
        Map<String, Object> props = Map.of(
                AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaContainerHolder.startIfNeeded());
        return new KafkaAdmin(props);
    }

    // Topics a, b, c are created explicitly (1 partition, replication factor 1)
    // before the apps start, to avoid first-publish races.

    @Bean
    public NewTopic topicA() {
        return TopicBuilder.name("a").partitions(1).replicas(1).build();
    }

    @Bean
    public NewTopic topicB() {
        return TopicBuilder.name("b").partitions(1).replicas(1).build();
    }

    @Bean
    public NewTopic topicC() {
        return TopicBuilder.name("c").partitions(1).replicas(1).build();
    }

    @Bean
    public OutputCollector outputCollector() {
        return new OutputCollector();
    }

    @Bean
    public OutputTopicListener outputTopicListener(OutputCollector outputCollector) {
        return new OutputTopicListener(outputCollector);
    }

    @Bean
    public AppLifecycleManager appLifecycleManager() {
        return new AppLifecycleManager();
    }
}
