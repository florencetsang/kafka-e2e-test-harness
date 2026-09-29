package com.example.harness.config;

import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Holds the single Kafka broker shared by every context in this JVM
 * (harness, App1, App2).
 *
 * <p>{@link #startIfNeeded()} is idempotent and immediately sets the
 * {@code spring.kafka.bootstrap-servers} system property on start, so that
 * every Spring context launched afterwards (the harness context and the
 * two app contexts) inherits the real broker address instead of the
 * {@code localhost:9092} defaults.</p>
 */
public final class KafkaContainerHolder {

    private static final DockerImageName KAFKA_IMAGE = DockerImageName.parse("confluentinc/cp-kafka:7.9.10");

    /** One shared broker for the whole suite. */
    private static final KafkaContainer KAFKA = new KafkaContainer(KAFKA_IMAGE);

    private static final AtomicBoolean STARTED = new AtomicBoolean(false);

    private KafkaContainerHolder() {
    }

    /**
     * Starts the shared broker if it is not running yet, publishes the real
     * bootstrap address via the system property and returns it.
     *
     * <p>Idempotent: safe to call from multiple places (hooks, bean factory
     * methods) — the first call wins, later calls are no-ops.</p>
     */
    public static synchronized String startIfNeeded() {
        if (!STARTED.get()) {
            KAFKA.start();
            System.setProperty("spring.kafka.bootstrap-servers", KAFKA.getBootstrapServers());
            STARTED.set(true);
        }
        return KAFKA.getBootstrapServers();
    }

    /** The bootstrap address of the (already started) shared broker. */
    public static String getBootstrapServers() {
        return KAFKA.getBootstrapServers();
    }
}
