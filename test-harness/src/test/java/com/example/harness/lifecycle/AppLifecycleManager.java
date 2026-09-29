package com.example.harness.lifecycle;

import com.example.app1.App1Application;
import com.example.app2.App2Application;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.WebApplicationType;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.ConfigurableApplicationContext;

import java.util.ArrayList;
import java.util.List;

/**
 * Boots App1 and App2 as two separate in-JVM Spring contexts and shuts
 * them down again (in reverse start order).
 *
 * <p>Separate contexts + distinct consumer groups ({@code app1},
 * {@code app2}) guarantee isolation. The bootstrap address is passed
 * explicitly (belt-and-suspenders alongside the
 * {@code spring.kafka.bootstrap-servers} system property).</p>
 */
public class AppLifecycleManager {

    private static final Logger log = LoggerFactory.getLogger(AppLifecycleManager.class);

    private final List<ConfigurableApplicationContext> contexts = new ArrayList<>();

    public synchronized void startAll(String bootstrap) {
        log.info("[Harness] starting App1 context against bootstrap={}", bootstrap);
        contexts.add(new SpringApplicationBuilder(App1Application.class)
                .web(WebApplicationType.NONE)
                .properties("spring.kafka.bootstrap-servers=" + bootstrap)
                // Command-line arg (highest precedence): both app application.yml
                // files are on the harness test classpath and only ONE of them is
                // visible to every context, so pin each app's consumer group
                // explicitly when booting in-JVM. Standalone runs are unaffected.
                .run("--spring.kafka.consumer.group-id=app1"));
        log.info("[Harness] starting App2 context against bootstrap={}", bootstrap);
        contexts.add(new SpringApplicationBuilder(App2Application.class)
                .web(WebApplicationType.NONE)
                .properties("spring.kafka.bootstrap-servers=" + bootstrap)
                .run("--spring.kafka.consumer.group-id=app2"));
        log.info("[Harness] both app contexts are up");
    }

    /** Closes contexts in reverse start order, null-safe. */
    public synchronized void stopAll() {
        for (int i = contexts.size() - 1; i >= 0; i--) {
            ConfigurableApplicationContext context = contexts.get(i);
            try {
                if (context != null && context.isActive()) {
                    log.info("[Harness] closing context {}", context.getId());
                    context.close();
                }
            } catch (Exception e) {
                log.warn("[Harness] error while closing context {}", context.getId(), e);
            }
        }
        contexts.clear();
    }
}
