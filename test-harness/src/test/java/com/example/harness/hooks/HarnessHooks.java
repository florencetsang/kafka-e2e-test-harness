package com.example.harness.hooks;

import com.example.harness.config.KafkaContainerHolder;
import com.example.harness.kafka.OutputCollector;
import com.example.harness.lifecycle.AppLifecycleManager;
import io.cucumber.java.After;
import io.cucumber.java.AfterAll;
import io.cucumber.java.Before;
import io.cucumber.java.BeforeAll;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * Suite lifecycle for the Cucumber run.
 *
 * <p>Ordering guarantees:</p>
 * <ol>
 *   <li>Kafka container started and the {@code spring.kafka.bootstrap-servers}
 *       system property set BEFORE any context binds Kafka beans.</li>
 *   <li>Topics a, b, c created explicitly BEFORE the apps start (no
 *       first-publish races).</li>
 *   <li>Apps started once, after container + topics are up.</li>
 *   <li>Collector cleared per scenario so Scenario Outline examples stay
 *       independent.</li>
 *   <li>Apps stopped in reverse start order; container cleanup left to
 *       Testcontainers/Ryuk.</li>
 * </ol>
 *
 * <p>Note: Cucumber requires {@code @BeforeAll}/{@code @AfterAll} methods to
 * be static and they run outside any Spring-managed instance. Container
 * startup, topic creation and teardown are therefore static; the injected
 * {@link AppLifecycleManager} bean is reached through a static reference
 * bridged from the constructor, and app startup (which needs the injected
 * bean after the harness context is refreshed) runs exactly once in the
 * lowest-order {@code @Before} hook of the first scenario.</p>
 */
public class HarnessHooks {

    private static final Logger log = LoggerFactory.getLogger(HarnessHooks.class);

    private static final List<String> TOPIC_NAMES = List.of("a", "b", "c");

    /** Bridge from the Spring-managed instance to the static @AfterAll hook. */
    private static AppLifecycleManager appLifecycleManagerRef;

    /** Whether the app contexts were already started (apps start exactly once). */
    private static boolean appsStarted = false;

    private final AppLifecycleManager appLifecycleManager;
    private final OutputCollector collector;

    public HarnessHooks(AppLifecycleManager appLifecycleManager, OutputCollector collector) {
        this.appLifecycleManager = appLifecycleManager;
        this.collector = collector;
        appLifecycleManagerRef = appLifecycleManager;
    }

    @BeforeAll
    public static void beforeAll() {
        // 1. Start the shared broker (idempotent) and set the system property
        //    BEFORE the harness context refreshes / apps launch.
        String bootstrap = KafkaContainerHolder.startIfNeeded();
        // 2. Create topics a, b, c explicitly before the apps start
        //    (KafkaAdmin + NewTopic beans in HarnessConfig also cover this on
        //    context refresh; doing it here makes the ordering deterministic).
        createTopics(bootstrap);
    }

    @Before(order = 0)
    public void startAppsOnce() {
        if (!appsStarted) {
            appLifecycleManager.startAll(KafkaContainerHolder.startIfNeeded());
            appsStarted = true;
        }
    }

    /** Per scenario: drop stale messages so examples are independent. */
    @Before(order = 1000)
    public void clearCollectorBeforeScenario() {
        collector.clear();
    }

    @After
    public void clearCollectorAfterScenario() {
        collector.clear();
    }

    @AfterAll
    public static void afterAll() {
        if (appLifecycleManagerRef != null) {
            appLifecycleManagerRef.stopAll();
        }
        // Kafka container cleanup is left to Testcontainers Ryuk.
    }

    private static void createTopics(String bootstrap) {
        try (Admin admin = Admin.create(Map.of(
                AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap))) {
            Set<String> existing = admin.listTopics().names().get(15, TimeUnit.SECONDS);
            List<NewTopic> toCreate = new ArrayList<>();
            for (String name : TOPIC_NAMES) {
                if (!existing.contains(name)) {
                    toCreate.add(new NewTopic(name, 1, (short) 1));
                }
            }
            if (!toCreate.isEmpty()) {
                admin.createTopics(toCreate).all().get(30, TimeUnit.SECONDS);
                log.info("[Harness] created topics {} (1 partition, rf 1)", TOPIC_NAMES);
            } else {
                log.info("[Harness] topics {} already present", TOPIC_NAMES);
            }
        } catch (Exception e) {
            throw new IllegalStateException("Failed to create topics a, b, c", e);
        }
    }
}
