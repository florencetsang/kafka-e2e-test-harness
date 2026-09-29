# kafka-e2e-poc

A proof-of-concept for driving an **end-to-end Kafka pipeline with a Cucumber test harness**. Two
independent Spring Boot apps form a processing chain; the harness publishes an input message to
Kafka, waits asynchronously, and asserts the transformed output — against a **single real Kafka
broker** started in Docker via Testcontainers.

The full build specification lives in [`plans.md`](plans.md).

## What it proves

```
publish → [topic a] → App1 (× multiplier) → [topic b] → App2 (+ constant) → [topic c] → assert
```

- **App1** consumes topic `a` and writes `value * multiplier` to topic `b` (default multiplier: 2).
- **App2** consumes topic `b` and writes `value + constant` to topic `c` (default constant: 100).
- **End-to-end result on `c`:** `value * multiplier + constant` — e.g. input `5` → output `110`.

The harness runs all three `Scenario Outline` examples as Cucumber scenarios:

| id    | input | expected |
|-------|-------|----------|
| t-001 | 5     | 110      |
| t-002 | 0     | 100      |
| t-003 | 50    | 200      |

Key properties of the setup:

- **One real broker, one JVM.** A single Testcontainers Kafka container is shared by all three
  Spring contexts (harness, App1, App2). The apps are booted **in-JVM** as separate contexts via
  `SpringApplicationBuilder` — no separate processes.
- **Correlation by id.** Every message carries a unique `id`, which is also the Kafka record key
  end-to-end, so responses are matched per message and per-key ordering is preserved.
- **Isolation by consumer group.** `app1`, `app2` and `harness-collector` are three distinct
  groups; `auto-offset-reset=earliest` everywhere so no message is missed on startup.
- **Asynchronous assertion.** A harness `@KafkaListener` drains topic `c` into a thread-safe
  collector; the step definitions assert with **Awaitility** (explicit timeout, matched by id).

## Module layout

```
kafka-e2e-poc/
├── pom.xml                         # parent (packaging=pom): versions, BOM imports, plugins
├── common/                         # Message POJO shared by everything (no Spring dep)
├── app1/                           # Spring Boot app: a → (×2) → b      (group app1)
├── app2/                           # Spring Boot app: b → (+100) → c     (group app2)
└── test-harness/                   # Cucumber harness (all sources under src/test)
    └── src/test/java/com/example/harness/
        ├── RunCucumberTest.java            # JUnit Platform suite entry point
        ├── CucumberSpringConfig.java       # @CucumberContextConfiguration
        ├── config/KafkaContainerHolder.java# single shared Kafka container + system property
        ├── config/HarnessConfig.java       # explicit harness Kafka beans + topics a, b, c
        ├── kafka/OutputCollector.java      # ConcurrentHashMap<id, Message>
        ├── kafka/OutputTopicListener.java  # @KafkaListener on topic c (group harness-collector)
        ├── lifecycle/AppLifecycleManager.java # boots/stops App1+App2 as in-JVM contexts
        ├── hooks/HarnessHooks.java         # container start, topic creation, per-scenario clear
        └── steps/PipelineSteps.java        # Given / When / Then
```

Tech stack: Java 17 · Maven multi-module · Spring Boot 3.2.5 (`spring-kafka`) · Testcontainers
1.19.8 · Cucumber 7.15.0 · Awaitility 4.2.0 · JSON over plain String keys.

## Getting started

### Prerequisites

- **Java 17+** (the build targets release 17; newer JDKs work)
- **Maven 3.9.x**
- **Docker** must be running and reachable — the harness starts `confluentinc/cp-kafka:7.5.3`
  (plus Testcontainers' `ryuk` reaper) through it.

  On Windows machine that means **Docker Desktop must be running with its engine started**
  (click the Docker Desktop tray icon and wait until it says "Docker Desktop is running").
  Nothing else is required — no `DOCKER_HOST` or other environment variables.

### Build and run the E2E tests

From the repo root:

```bash
mvn clean install
```

This builds all four modules and runs the Cucumber suite in the `test-harness` module via
Surefire. Expected result:

```
[INFO] kafka-e2e-poc ........ SUCCESS
[INFO] common ................ SUCCESS
[INFO] app1 .................. SUCCESS
[INFO] app2 .................. SUCCESS
[INFO] test-harness .......... SUCCESS
[INFO] Tests run: 3, Failures: 0, Errors: 0, Skipped: 0
[INFO] BUILD SUCCESS
```

The log shows the full chain for each scenario:

```
[Harness] published Message{id='t-001', value=5} to topic 'a'
[App1]    id=t-001 value=5 x multiplier=2 = 10 -> published to topic 'b'
[App2]    id=t-001 value=10 + constant=100 = 110 -> published to topic 'c'
[Harness] collected from topic 'c': Message{id='t-001', value=110}
[Harness] asserted id='t-001' on topic 'c' has value 110
```

### Other ways to run

#### Harness tests only (apps already built)

```bash
mvn -pl test-harness test
```

#### Run the apps standalone (outside the harness)

The app modules are ordinary Spring Boot apps. Each builds two artifacts: a plain jar (used as a
library by the harness) and a runnable `-exec` jar:

```bash
java -jar app1/target/app1-1.0.0-exec.jar --spring.kafka.bootstrap-servers=localhost:9092
java -jar app2/target/app2-1.0.0-exec.jar --spring.kafka.bootstrap-servers=localhost:9092
```

Defaults: `bootstrap-servers=localhost:9092`, topics `a`/`b`/`c`, multiplier `2`, constant `100`
(see `app1/src/main/resources/application.yml` and `app2/src/main/resources/application.yml`).
Point `--spring.kafka.bootstrap-servers` at any broker and the apps will happily join it — the
harness does exactly that with the container's random port.

### Troubleshooting

- `Could not find a valid Docker environment` — Docker isn't running. Start it and re-run.
- `client version 1.32 is too old. Minimum supported API version is 1.40` — the pinned
  Testcontainers client is older than your Docker engine. Add this line to
  `~/.docker-java.properties`:
  ```
  api.version=1.44
  ```

## How the harness works (lifecycle)

The ordering matters and is enforced in `HarnessHooks` / `KafkaContainerHolder`:

1. **Container starts first.** `KafkaContainerHolder.startIfNeeded()` (idempotent) starts the one
   shared broker and immediately sets the system property
   `spring.kafka.bootstrap-servers` to the container's real address, so every later context
   (harness + apps) inherits it instead of `localhost:9092`. Each Kafka bean in `HarnessConfig`
   also calls `startIfNeeded()` when it is created, so the beans can never bind to the default
   even if context refresh happens earlier than expected.
2. **Topics `a`, `b`, `c` are created explicitly** (1 partition, replication factor 1) via an
   `Admin` client before anything is published — no reliance on broker auto-creation, so there
   is no first-publish race.
3. **Harness Spring context refreshes** — Kafka producer (to `a`), collector listener factory,
   `KafkaAdmin`/`NewTopic` beans, all defined **explicitly in `HarnessConfig`** (not via app
   autoconfiguration, because both apps' `application.yml` files are on the shared test
   classpath).
4. **App1 and App2 start once**, each as its own in-JVM context
   (`AppLifecycleManager`), with the bootstrap address passed both via the system property and
   as an explicit builder override (belt and suspenders).
5. **Per scenario**, the collector is cleared so `Scenario Outline` examples stay independent.
   Scenarios execute strictly serially.
6. **On shutdown**, both app contexts are closed in reverse start order and Testcontainers'
   Ryuk reaper removes the Kafka container when the JVM exits.

## Design notes / gotchas handled

- **Shared test classpath shadowing.** With both apps on the harness's test classpath, only one
  of the two `application.yml` files is visible to any given context. Each app therefore uses
  placeholder defaults in its listener (`${app1.input-topic:a}` …), field defaults in its
  `@ConfigurationProperties` class, and its consumer group pinned at boot time by
  `AppLifecycleManager`. Result: the three groups are always `app1`, `app2`, `harness-collector`,
  while standalone runs keep reading their own `application.yml`.
- **Boot plugin classifier.** `spring-boot-maven-plugin` repackages with `<classifier>exec</classifier>`
  so the main artifact stays a plain jar that the harness can depend on; the runnable fat jar is
  attached alongside it.
- **No app/harness leakage.** `common`, `app1` and `app2` contain no test, Cucumber or
  Testcontainers code or dependencies — everything harness-related lives in `test-harness/src/test`.

## Non-goals (by design)

No separate-process launching, no schema registry/Avro (JSON only), no parallel Cucumber
execution, no multiple partitions/consumers per app, no DLQ/retry/error handling beyond
defaults, no security.