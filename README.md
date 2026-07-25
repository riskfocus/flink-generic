# Flink Generic Tools

A library of generic, **non project-specific** building blocks for [Apache Flink](https://flink.apache.org)
streaming applications. It encapsulates the boilerplate every Flink project re-writes — execution-environment
setup, checkpointing and metrics, source/sink wiring, layered configuration — behind a small, chainable
builder API so a pipeline reads as configuration rather than plumbing.

**Stack:** Apache Flink 2.3 · Java 17 · Maven · Kafka (POJO/Avro, Confluent & AWS MSK) · Redis (Lettuce) · JDBC

---

## Why this exists

Vanilla Flink leaves a lot of repetitive wiring to each project: naming and parallelism for every operator,
environment configuration for metrics/checkpointing/time characteristics, source and sink setup per connector
type, and merging properties from files, environment variables, and the command line with a sensible override
order. This library pulls that into reusable components so you can focus on business logic:

- **Fluent pipeline builder** — wrap `StreamExecutionEnvironment` and chain sources, keyed processors,
  transformations, and sinks (`StreamBuilder.from(env, params)...build().run(...)`).
- **Per-operator parallelism from config** — set the parallelism of each named operator declaratively in the
  config file, no code change or redeploy required (see the highlight below).
- **Layered configuration** — read parameters from a YAML file, command line, and environment variables with
  a defined override/fallback strategy (extends Flink's `ParameterTool`).
- **Configurable connectors** — Kafka source/sink (POJO & Avro, Confluent Cloud & AWS MSK), a Redis sink, and
  a bulk-insert JDBC sink that works around [FLINK-17488](https://issues.apache.org/jira/browse/FLINK-17488).
- **Event-time tooling** — window helpers, an idle-aware watermark generator, and event-time timestamp assigners.

### ⭐ Highlight: declarative per-operator parallelism

Native Flink has **no declarative, per-operator** parallelism: your only choices are a single **global**
default (`parallelism.default` / `-p`) or a hardcoded in-code `.setParallelism(n)` on each operator — which
means editing and redeploying the job to retune. This library fills that gap: each named operator's
parallelism is read from the layered config, so you can size operators independently by editing configuration
alone.

```yaml
# application.yml — keyed by operator name
interestRatesEnricher.operator:
  parallelism: 4

reduceByUSDCurrency.operator:
  parallelism: 1
```

The value resolves through the same override chain as every other property — YAML file → environment variables
→ command line (e.g. `-interestRatesEnricher.operator.parallelism 8`) — and falls back to the job default when
unset. Retuning a hot operator becomes a config change, not a code change.

---

## Modules

The reactor is organized around `flink-baseline` (a Maven BOM plus the core libraries) with supporting domain
and integration-test modules.

| Module | Description |
|--------|-------------|
| [`flink-baseline`](flink-baseline/README.md) | Maven **BOM** aggregating Flink and related dependencies; import it to align versions. |
| &nbsp;&nbsp;└ [`flink-common`](flink-baseline/flink-common/README.md) | Core library: the pipeline builder, layered config, Kafka/Redis connectors, operators, and event-time/window tooling. |
| &nbsp;&nbsp;└ [`flink-jdbc-sink`](flink-baseline/flink-jdbc-sink/README.md) | JDBC sink supporting true bulk inserts, offered as both a plain sink and a keyed processor. |
| &nbsp;&nbsp;└ [`flink-snapshot`](flink-baseline/flink-snapshot/README.md) | Atomic snapshotting of aggregated state into Redis and reading it back _(beta)_. |
| &nbsp;&nbsp;└ [`flink-example`](flink-baseline/flink-example/README.md) | **SmoothingPrices** — a runnable reference application built on `flink-common`, with a full Docker dev stack. |
| [`flink-domain`](flink-domain/README.md) | Shared time/pipeline interfaces and utilities (`Event`, `IncomingEvent`, `EventUtils`, keyed-aware markers). |
| [`flink-example-domain`](flink-example-domain/README.md) | Domain objects shared between the example app and its test (kept free of Flink dependencies). |
| [`flink-test-example`](flink-test-example/README.md) | **SmoothingIT** — end-to-end integration test that drives the SmoothingPrices pipeline and asserts output. |

---

## Usage

Import the BOM to align versions, then depend on the pieces you need:

```xml
<dependencyManagement>
    <dependencies>
        <dependency>
            <groupId>com.ness.flink.generic</groupId>
            <artifactId>flink-baseline</artifactId>
            <version>${flink.generic.version}</version>
            <type>pom</type>
            <scope>import</scope>
        </dependency>
    </dependencies>
</dependencyManagement>

<dependencies>
    <dependency>
        <groupId>com.ness.flink.generic</groupId>
        <artifactId>flink-common</artifactId>
        <version>${flink.generic.version}</version>
    </dependency>
</dependencies>
```

Building a pipeline:

```java
StreamBuilder.from(env, params)
    .stream()
    .source(source)
    .addKeyedProcessor(new KeyedProcessorDefinition<>(
        OperatorProperties.from("test.processor", params), v -> v, new MyProcessFunction()))
    .addToStream(stream -> stream.map(v -> v))
    .addSink(mySinkDefinition)
    .build()
    .run("my.sink");
```

See [`flink-common`](flink-baseline/flink-common/README.md) for the full API.

---

## Requirements

- JDK 17
- Maven 3.6.2+

## Build

```bash
mvn clean install
```

CI runs `mvn -B verify` on JDK 17 via [GitHub Actions](.github/workflows/maven.yml) — a full reactor build
including PMD (strict), Apache RAT license checks, unit tests, and JaCoCo coverage.

## Run the example

The `flink-example` module ships a runnable **SmoothingPrices** application with a Docker dev stack (Kafka,
Redis, Flink UI, Prometheus, Grafana). See its [README](flink-baseline/flink-example/README.md) for the
step-by-step, including the runtime "feature flag" configuration-reload demo.

## Test

- **Unit tests:** `mvn test`
- **Integration tests:** the module-level `*IT` tests use [Testcontainers](https://testcontainers.com)
  (MySQL, Redis) and require a running Docker daemon.
- **End-to-end:** the [`SmoothingIT`](flink-test-example/README.md) test (`-PIT-test` profile) drives the
  full SmoothingPrices pipeline against the Docker stack and asserts eventual-consistent output.
