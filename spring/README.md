# kopipes Spring Boot support

Optional module. `core` and `aws` have no Spring dependency and no global state: an application builds an
`AwsKopipesContext` and passes it to the components it creates. Without Spring that is one line per Lambda container:

```kotlin
class ListenerContainer(val context: AwsKopipesContext) {
    val assembler = PipelineAssembler.builder().context(context).addPipeline(...).build()
    val kinesisAdapter = KinesisAdapter(context, MyEventCodec)

    companion object {
        fun build() = ListenerContainer(AwsKopipesContext())   // env vars, EventBridge
    }
}
```

With Spring Boot, add this module and the context becomes a bean you inject:

```kotlin
dependencies {
    implementation(project(":spring"))   // brings :aws and :core
    implementation(platform("org.springframework.boot:spring-boot-dependencies:4.1.2"))
    implementation("org.springframework.boot:spring-boot-autoconfigure")
    implementation(libs.aws.sdk.eventbridge) // enables the default EventBridgePublisher
}

@Bean
fun listenerContainer(kopipes: AwsKopipesContext) = ListenerContainer(kopipes)
```

## What it gives you

| Bean (replace it by declaring your own) | Default |
|---|---|
| `AwsEnvironmentConfig` | `SpringEnvironmentConfig`, which reads the Spring `Environment` |
| `DynamoDbClientFactory`, `S3ClientFactory`, `KmsClientFactory`, `CloudWatchClientFactory`, `EventBridgeClientFactory` | the `Default*ClientFactory`, each only when its AWS SDK is on the classpath |
| `EventPublisher` | `EventBridgePublisher`, only when the EventBridge SDK is on the classpath |
| `UnitOfWorkSnapshotter` | `DefaultUnitOfWorkSnapshotter` with the AWS record snapshotters plus any `RecordSnapshotter` beans |
| `FaultManager` | publishes faults through the `EventPublisher` bean; AWS throttling errors are retryable |
| `AwsKopipesContext` | assembled from the beans above |

To make more errors retryable (a gateway's "not known yet", say), declare your own `FaultManager` bean with
`FaultManager.fromConfig(envConfig, publisher, snapshotter, isRetryable = { it is NotKnownYet || isAwsRetryable(it) })`.
With several `EventPublisher` beans, mark the default one `@Primary`.

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
