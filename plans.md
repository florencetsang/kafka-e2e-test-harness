Here's a complete `plans.md` ready to hand to the coding agent. I've consolidated everything into a single self-contained document.

````markdown
# plans.md — E2E Kafka Test Harness POC

> **For the implementing agent:** This is a prescriptive build spec. Follow the module layout, class responsibilities, and contracts exactly. Code snippets show intended shape, not necessarily byte-for-byte. Build in the order given in §13 and validate against the acceptance criteria in §11.

---

## 1. Goal

Build a Maven multi-module Java/Spring POC that proves an end-to-end Kafka pipeline works, driven by a Cucumber test harness. Two independent Spring Boot apps form a chain; the harness publishes an input, waits asynchronously, and asserts the transformed output.

**Topology (chained):**
```
publish → [topic a] → App1 → [topic b] → App2 → [topic c] → assert
```

- **App1**: consumes `a`, writes `value * multiplier` to `b`.
- **App2**: consumes `b`, writes `value + constant` to `c`.
- **End-to-end result on `c`**: `value * multiplier + constant`.
- Defaults: `multiplier = 2`, `constant = 100`. So input `5` → output `110`.

---

## 2. Confirmed design decisions

| Concern | Decision |
|---|---|
| Transform | `value * multiplier + constant` (split across the two apps), params externalized |
| Kafka | Testcontainers — one real broker shared by all contexts |
| Context isolation | In-JVM; each app booted via its own `SpringApplicationBuilder` → N `ConfigurableApplicationContext` in one JVM |
| Async sync | Harness `@KafkaListener` drains `topic c` into a thread-safe collection; assert with Awaitility + explicit timeout, matched by correlation `id` |
| Test framework | Cucumber (cucumber-spring), serial execution |

---

## 3. Tech stack / versions

- Java 17
- Maven (multi-module)
- Spring Boot 3.2.x (`spring-boot-starter`, `spring-kafka`)
- Testcontainers 1.19.x (`kafka`, `junit-jupiter`)
- Cucumber 7.15.x (`cucumber-java`, `cucumber-spring`, `cucumber-junit-platform-engine`)
- Awaitility 4.2.x
- Jackson (via `spring-boot-starter-json`)
- JUnit 5 platform (Cucumber runner)

Pin all versions in parent `dependencyManagement` using imported BOMs: `spring-boot-dependencies`, `testcontainers-bom`, `cucumber-bom`.

---

## 4. Module layout

```
kafka-e2e-poc/
├── pom.xml                         # parent (packaging=pom)
├── common/
│   ├── pom.xml
│   └── src/main/java/com/example/common/Message.java
├── app1/
│   ├── pom.xml
│   └── src/main/
│       ├── java/com/example/app1/
│       │   ├── App1Application.java
│       │   ├── App1Listener.java
│       │   └── App1Properties.java
│       └── resources/application.yml
├── app2/
│   ├── pom.xml
│   └── src/main/
│       ├── java/com/example/app2/
│       │   ├── App2Application.java
│       │   ├── App2Listener.java
│       │   └── App2Properties.java
│       └── resources/application.yml
└── test-harness/
    ├── pom.xml
    └── src/test/
        ├── java/com/example/harness/
        │   ├── RunCucumberTest.java
        │   ├── CucumberSpringConfig.java
        │   ├── config/HarnessConfig.java
        │   ├── config/KafkaContainerHolder.java
        │   ├── kafka/OutputCollector.java
        │   ├── kafka/OutputTopicListener.java
        │   ├── lifecycle/AppLifecycleManager.java
        │   ├── hooks/HarnessHooks.java
        │   └── steps/PipelineSteps.java
        └── resources/
            ├── features/pipeline.feature
            └── junit-platform.properties
```

---

## 5. Parent `pom.xml`

- `packaging=pom`; list all four modules.
- Properties: `java.version=17`, UTF-8 encoding.
- `dependencyManagement` imports: `spring-boot-dependencies`, `testcontainers-bom`, `cucumber-bom`; declare Awaitility version.
- `maven-compiler-plugin` targeting Java 17.
- `spring-boot-maven-plugin` declared in **app modules only** (repackage goal).
- Harness tests run under **Surefire in the `test` phase** (keep it simple).

---

## 6. `common` module

Plain POJO, no Spring dependency required.

```java
package com.example.common;

public class Message {
    private String id;      // correlation id
    private long value;

    public Message() {}
    public Message(String id, long value) { this.id = id; this.value = value; }

    // getters + setters for id, value
    // equals/hashCode based on id
    // toString
}
```

---

## 7. App modules (App1, App2)

Both are **standard Spring Boot apps** — own `@SpringBootApplication` main, own `application.yml`, own consumer group. Launchable standalone **and** programmatically. **No Testcontainers / harness code here.**

### Kafka serde (both apps, via `application.yml` autoconfig)
- Producer: `StringSerializer` key, `JsonSerializer` value.
- Consumer: `StringDeserializer` key, `JsonDeserializer` value; `spring.json.trusted.packages=com.example.common`, `spring.json.value.default.type=com.example.common.Message`.
- `bootstrap-servers` default `localhost:9092`, **overridable** (harness injects real address).

### App1

`application.yml`:
```yaml
spring:
  kafka:
    consumer:
      group-id: app1
      auto-offset-reset: earliest
      properties:
        spring.json.trusted.packages: com.example.common
        spring.json.value.default.type: com.example.common.Message
      key-deserializer: org.apache.kafka.common.serialization.StringDeserializer
      value-deserializer: org.springframework.kafka.support.serializer.JsonDeserializer
    producer:
      key-serializer: org.apache.kafka.common.serialization.StringSerializer
      value-serializer: org.springframework.kafka.support.serializer.JsonSerializer
app1:
  input-topic: a
  output-topic: b
  multiplier: 2
```

- `App1Properties`: `@ConfigurationProperties(prefix="app1")` → `inputTopic`, `outputTopic`, `multiplier`.
- `App1Listener`: `@KafkaListener(topics="${app1.input-topic}", groupId="${spring.kafka.consumer.group-id}")`; on message compute `value * multiplier`, send `new Message(id, result)` to `output-topic` with key = `id`; log in/out.
- `App1Application`: `@SpringBootApplication` + `@EnableConfigurationProperties(App1Properties.class)` + `main`.

### App2

Same structure. `application.yml` mirrors App1 with:
```yaml
spring:
  kafka:
    consumer:
      group-id: app2
      auto-offset-reset: earliest
      # (same json deserializer props as app1)
app2:
  input-topic: b
  output-topic: c
  constant: 100
```
- `App2Listener`: compute `value + constant`, send to `c`, key = `id`.

---

## 8. Topics

Create `a`, `b`, `c` **explicitly in the harness** (1 partition, replication factor 1) via `KafkaAdmin` + `NewTopic` beans, before starting the apps. Do not rely on auto-creation, to avoid first-publish races.

---

## 9. Test harness module (all under `src/test`)

Depends on `app1`, `app2`, `common`.

### 9.1 Kafka container (single shared broker)

`KafkaContainerHolder`:
- Holds a `static KafkaContainer` (`confluentinc/cp-kafka:7.5.x`), started once.
- On start, immediately: `System.setProperty("spring.kafka.bootstrap-servers", container.getBootstrapServers())` so **all** contexts (harness + apps) inherit it.
- Expose `getBootstrapServers()` and an idempotent `startIfNeeded()`.

### 9.2 Harness Spring context (Cucumber)

`CucumberSpringConfig`:
```java
@CucumberContextConfiguration
@SpringBootTest(classes = HarnessConfig.class)
public class CucumberSpringConfig { }
```

`HarnessConfig` (`@Configuration @EnableKafka`) — define beans **explicitly** (do not rely on app autoconfig for the harness serde):
- `ProducerFactory<String, Message>` + `KafkaTemplate<String, Message>` (JSON serializer) → publishes inputs to `a`.
- `ConsumerFactory<String, Message>` + `ConcurrentKafkaListenerContainerFactory` for the collector listener (group `harness-collector`, `auto-offset-reset=earliest`, JSON deserializer trusting `com.example.common`).
- `KafkaAdmin` + `NewTopic` beans for `a`, `b`, `c`.
- `OutputCollector` bean.
- `OutputTopicListener` bean.
- `AppLifecycleManager` bean.
- All Kafka beans read bootstrap from the system property set by `KafkaContainerHolder`; ensure the container is started before the harness context refreshes (see 9.5).

### 9.3 Output collection (sync mechanism)

`OutputCollector`:
- `ConcurrentHashMap<String, Message>` keyed by `id`.
- `void add(Message m)`, `Optional<Message> get(String id)`, `boolean contains(String id)`, `void clear()`.

`OutputTopicListener`:
- `@KafkaListener(topics="c", groupId="harness-collector", containerFactory="<the harness factory>")` → `collector.add(message)`.

Listener runs on Spring's container thread; step thread reads via Awaitility. No manual thread joining.

### 9.4 App lifecycle manager

`AppLifecycleManager`:
```java
public void startAll(String bootstrap) {
    contexts.add(new SpringApplicationBuilder(App1Application.class)
        .web(WebApplicationType.NONE)
        .properties("spring.kafka.bootstrap-servers=" + bootstrap)
        .run());
    contexts.add(new SpringApplicationBuilder(App2Application.class)
        .web(WebApplicationType.NONE)
        .properties("spring.kafka.bootstrap-servers=" + bootstrap)
        .run());
}
public void stopAll() { /* close in reverse order, null-safe */ }
```
Separate contexts + distinct group ids guarantee isolation. Pass bootstrap explicitly (belt-and-suspenders alongside the system property).

### 9.5 Cucumber hooks

`HarnessHooks` (constructor-inject `AppLifecycleManager`, `OutputCollector`):
- `@BeforeAll`: `KafkaContainerHolder.startIfNeeded()` (sets system property).
- App startup: start apps **once** in `@BeforeAll` (after container is up) via `AppLifecycleManager.startAll(bootstrap)`.
- `@Before` (per scenario): `collector.clear()` so scenarios don't see stale messages.
- `@After`: (optional) `collector.clear()`.
- `@AfterAll`: `appLifecycleManager.stopAll()`. Container cleanup left to Testcontainers Ryuk.

**Ordering:** container start → system property set → harness context (creates topics via `KafkaAdmin`) → apps start. If topic creation must precede app start deterministically, create topics in `@BeforeAll` before `startAll` (via injected `KafkaAdmin` or an `AdminClient`).

### 9.6 Step definitions

`PipelineSteps` (constructor-inject `KafkaTemplate<String,Message>`, `OutputCollector`):
- **Given** `a message with id "<id>" and value <input> is published to topic "a"`:
  `kafkaTemplate.send("a", id, new Message(id, input));`
- **When** `the pipeline processes it within <n> seconds`: store timeout (or treat as logging; Then can hold a default).
- **Then** `topic "c" should contain a message with id "<id>" and value <expected>`:
```java
Awaitility.await()
    .atMost(Duration.ofSeconds(timeoutSeconds))
    .pollInterval(Duration.ofMillis(200))
    .untilAsserted(() -> {
        Message m = collector.get(id)
            .orElseThrow(() -> new AssertionError("no message for id " + id));
        assertThat(m.getValue()).isEqualTo(expected);
    });
```

### 9.7 Feature file

`features/pipeline.feature`:
```gherkin
Feature: Value flows through App1 (x multiplier) and App2 (+ constant)

  Scenario Outline: value is transformed to value*2 + 100
    Given a message with id "<id>" and value <input> is published to topic "a"
    When the pipeline processes it within 15 seconds
    Then topic "c" should contain a message with id "<id>" and value <expected>

    Examples:
      | id    | input | expected |
      | t-001 | 5     | 110      |
      | t-002 | 0     | 100      |
      | t-003 | 50    | 200      |
```

### 9.8 Runner & platform config

`RunCucumberTest.java`:
```java
@Suite
@IncludeEngines("cucumber")
@SelectClasspathResource("features")
@ConfigurationParameter(key = GLUE_PROPERTY_NAME, value = "com.example.harness")
public class RunCucumberTest { }
```

`junit-platform.properties`:
```
cucumber.glue=com.example.harness
cucumber.plugin=pretty
cucumber.execution.parallel.enabled=false
```

Keep execution **serial**.

---

## 10. Key contracts / invariants

- Every `Message` carries a unique `id`; harness matches responses by `id`; `id` is also the Kafka key end-to-end (per-key ordering).
- Consumer groups all distinct: `app1`, `app2`, `harness-collector`.
- All consumers `auto-offset-reset=earliest` (no missed messages on startup).
- Bootstrap address flows via system property `spring.kafka.bootstrap-servers` (set once) **and** explicit builder override for apps.
- Harness serde configured explicitly, independent of app autoconfig.
- Contexts closed in reverse start order.

---

## 11. Acceptance criteria (definition of done)

1. `mvn clean install` builds all four modules with no errors.
2. With Docker available, running harness tests starts **one** Kafka container, boots App1 and App2 as **two separate in-JVM contexts**, and passes all three `Scenario Outline` examples.
3. Logs show: input published to `a` → App1 emits to `b` → App2 emits to `c` → harness collects from `c`.
4. A deliberately wrong `expected` value in the feature file makes the Awaitility assertion **fail** within the timeout (proves assertion is real).
5. All contexts and the container shut down cleanly after the suite.
6. No test/Testcontainers code leaks into `app1`, `app2`, or `common`.

---

## 12. Explicit non-goals

No separate-process launching, no schema registry/Avro (JSON only), no parallel Cucumber execution, no multiple partitions/consumers-per-app, no DLQ/retry/error handling beyond defaults, no security.

---

## 13. Build order for the agent

1. Parent POM.
2. `common` (`Message`).
3. `app1` — verify it starts standalone.
4. `app2`.
5. Harness: `KafkaContainerHolder` + `HarnessConfig` (container wiring, topics, producer).
6. `OutputCollector` + `OutputTopicListener`.
7. `AppLifecycleManager`.
8. Cucumber glue: `CucumberSpringConfig`, `HarnessHooks`, `PipelineSteps`, `RunCucumberTest`, feature file, `junit-platform.properties`.
9. Run and validate all acceptance criteria (including the deliberate-failure check in #4).

---

## 14. Notes / gotchas for the agent

- Ensure the Kafka container is started (and the system property set) **before** the harness Spring context refreshes and before apps launch — otherwise beans bind to `localhost:9092`.
- Because multiple app `application.yml` files sit on the harness classpath at test time, keep the harness's own Kafka beans **explicitly defined** in `HarnessConfig` to avoid picking up app profiles/config unexpectedly.
- Give the collector listener `auto-offset-reset=earliest` and a unique group so it never competes with app consumers.
- Clear the collector per scenario to keep `Scenario Outline` examples independent.
````

This is fully self-contained — the agent has the topology, module layout, every class's responsibility, config snippets, ordering constraints, and a definition of done including the deliberate-failure sanity check.

Want me to also generate the actual reference source (parent POM + all four modules) as a companion so the agent has working code to check against, or is the plan enough to dispatch?