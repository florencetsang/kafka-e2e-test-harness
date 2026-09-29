package com.example.harness.kafka;

import com.example.common.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;

/**
 * Drains topic c into the {@link OutputCollector}.
 *
 * <p>Runs on Spring's listener container thread (group
 * {@code harness-collector}); the step thread reads the collector via
 * Awaitility. Uses the harness-specific container factory, never an app
 * autoconfiguration factory.</p>
 */
public class OutputTopicListener {

    private static final Logger log = LoggerFactory.getLogger(OutputTopicListener.class);

    private final OutputCollector collector;

    public OutputTopicListener(OutputCollector collector) {
        this.collector = collector;
    }

    @KafkaListener(
            topics = "c",
            groupId = "harness-collector",
            containerFactory = "harnessListenerContainerFactory")
    public void onMessage(Message message) {
        log.info("[Harness] collected from topic 'c': {}", message);
        collector.add(message);
    }
}
