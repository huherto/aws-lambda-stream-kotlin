# kopipes Spring Boot support

Optional module. `core` and `aws` have no Spring dependency, so a Lambda that does not use Spring keeps working as it
does today. Add this module only when the application runs on Spring Boot.

```kotlin
dependencies {
    implementation(project(":spring"))   // brings :aws and :core
    implementation(platform("org.springframework.boot:spring-boot-dependencies:4.1.2"))
    implementation("org.springframework.boot:spring-boot-autoconfigure")
    implementation(libs.aws.sdk.eventbridge) // enables the default EventBridgePublisher
}
```

## What it gives you

| Bean (replace it by declaring your own) | Default |
|---|---|
| `EnvironmentConfig` | `SpringEnvironmentConfig`, which reads the Spring `Environment` |
| `EventPublisher` | `EventBridgePublisher`, only when the EventBridge SDK is on the classpath |
| `FaultManager` | publishes faults through the `EventPublisher` bean, AWS-aware retry check |
| `UnitOfWorkSnapshotter` | `DefaultUnitOfWorkSnapshotter` with the AWS record snapshotters plus any `RecordSnapshotter` beans |

`KopipesRegistryBridge` pushes these beans, and any `*ClientFactory` beans, into `GlobalRegistry` / `AwsGlobalRegistry`.
Framework code that relies on registry defaults (`awsEnvConfig()`, `DynamoDbEventsMicrostore()`, `PipelineAssembler.builder().build()`)
therefore gets the same instances Spring injects. When there are several beans of one type, only the `@Primary` one is pushed.

Turn it all off with `kopipes.enabled=false`.

## Configuration

Each setting is read as a `kopipes.*` property first, then under its existing env var name, so deployed Lambdas need no change:

```yaml
kopipes:
  bus-name: payments-bus          # or env BUS_NAME, or KOPIPES_BUS_NAME
  event-table-name: payments-events
  stream-retry-enabled: true
  metrics: pipeline
```

See `SpringEnvironmentConfig` for the full key list.
