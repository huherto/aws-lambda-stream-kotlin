package io.kopipes.spring

import io.kopipes.aws.AwsEnvironmentConfig
import io.kopipes.aws.AwsKopipesContext
import io.kopipes.aws.connectors.DynamoDbClientFactory
import io.kopipes.aws.sinks.DynamoDbEventsMicrostore
import io.kopipes.aws.sinks.EventBridgePublisher
import io.kopipes.core.faults.FaultManager
import io.kopipes.core.serialization.snapshots.RecordSnapshot
import io.kopipes.core.serialization.snapshots.RecordSnapshotter
import io.kopipes.core.serialization.snapshots.UnitOfWorkSnapshotter
import io.kopipes.core.sinks.EventPublisher
import io.kopipes.core.sinks.EventPublisherInMemory
import io.kotest.matchers.nulls.shouldNotBeNull
import io.kotest.matchers.shouldBe
import io.kotest.matchers.string.shouldContain
import io.kotest.matchers.types.shouldBeInstanceOf
import io.kotest.matchers.types.shouldBeSameInstanceAs
import io.mockk.mockk
import org.junit.jupiter.api.Test
import org.springframework.boot.autoconfigure.AutoConfigurations
import org.springframework.boot.test.context.FilteredClassLoader
import org.springframework.boot.test.context.runner.ApplicationContextRunner
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.context.annotation.Primary

class KopipesAutoConfigurationTest {

    private val runner = ApplicationContextRunner()
        .withConfiguration(AutoConfigurations.of(KopipesAutoConfiguration::class.java))

    @Test
    fun `defines the default beans from Spring properties`() {
        // arrange
        val withProps = runner.withPropertyValues("kopipes.bus-name=my-bus", "kopipes.stream-retry-enabled=true")

        // act & assert
        withProps.run { context ->
            val envConfig = context.getBean(AwsEnvironmentConfig::class.java)
            val publisher = context.getBean(EventPublisher::class.java)
            val faultManager = context.getBean(FaultManager::class.java)

            envConfig.shouldBeInstanceOf<SpringEnvironmentConfig>().streamRetryEnabled() shouldBe true
            publisher.shouldBeInstanceOf<EventBridgePublisher>().busName shouldBe "my-bus"
            faultManager.publisher() shouldBeSameInstanceAs publisher
            faultManager.unitOfWorkSnapshotter shouldBeSameInstanceAs context.getBean(UnitOfWorkSnapshotter::class.java)
        }
    }

    @Test
    fun `the context bean is assembled from the other beans`() {
        // arrange & act & assert
        runner.run { context ->
            val kopipes = context.getBean(AwsKopipesContext::class.java)

            kopipes.envConfig shouldBeSameInstanceAs context.getBean(AwsEnvironmentConfig::class.java)
            kopipes.faultManager shouldBeSameInstanceAs context.getBean(FaultManager::class.java)
            kopipes.eventPublisher shouldBeSameInstanceAs context.getBean(EventPublisher::class.java)
            kopipes.dynamoDbClientFactory shouldBeSameInstanceAs context.getBean(DynamoDbClientFactory::class.java)
        }
    }

    @Test
    fun `application beans replace the defaults everywhere`() {
        // arrange
        val withOverrides = runner.withUserConfiguration(OverridesConfig::class.java)

        // act & assert
        withOverrides.run { context ->
            val publisher = context.getBean(EventPublisher::class.java)
            val clientFactory = context.getBean(DynamoDbClientFactory::class.java)
            val kopipes = context.getBean(AwsKopipesContext::class.java)

            publisher.shouldBeInstanceOf<EventPublisherInMemory>()
            context.getBean(FaultManager::class.java).publisher() shouldBeSameInstanceAs publisher
            kopipes.eventPublisher shouldBeSameInstanceAs publisher
            kopipes.dynamoDbClientFactory shouldBeSameInstanceAs clientFactory
        }
    }

    @Test
    fun `components can be built from the context bean`() {
        // arrange
        val withMicrostore = runner
            .withPropertyValues("kopipes.event-table-name=events-table")
            .withUserConfiguration(MicrostoreConfig::class.java)

        // act & assert
        withMicrostore.run { context ->
            context.getBean(DynamoDbEventsMicrostore::class.java).shouldNotBeNull()
        }
    }

    @Test
    fun `with several publishers the primary one is used`() {
        // arrange
        val withTwoPublishers = runner.withUserConfiguration(TwoPublishersConfig::class.java)

        // act & assert
        withTwoPublishers.run { context ->
            val primary = context.getBean("primaryPublisher", EventPublisher::class.java)

            context.getBean(FaultManager::class.java).publisher() shouldBeSameInstanceAs primary
        }
    }

    @Test
    fun `record snapshotter beans are added to the default snapshotter`() {
        // arrange
        val withSnapshotter = runner.withUserConfiguration(SnapshotterConfig::class.java)

        // act & assert
        withSnapshotter.run { context ->
            val snapshot = context.getBean(UnitOfWorkSnapshotter::class.java)
                .snapshot(io.kopipes.core.UnitOfWork(record = "custom-record"))

            snapshot.record.shouldNotBeNull().kind shouldBe "custom"
        }
    }

    @Test
    fun `fails fast when there is no EventPublisher`() {
        // arrange
        val withoutEventBridge = runner.withClassLoader(
            FilteredClassLoader("aws.sdk.kotlin.services.eventbridge.EventBridgeClient")
        )

        // act & assert
        withoutEventBridge.run { context ->
            context.startupFailure.shouldNotBeNull().stackTraceToString() shouldContain "needs an EventPublisher bean"
        }
    }

    @Test
    fun `kopipes enabled false turns the auto-configuration off`() {
        // arrange
        val disabled = runner.withPropertyValues("kopipes.enabled=false")

        // act & assert
        disabled.run { context ->
            context.getBeanNamesForType(FaultManager::class.java).size shouldBe 0
            context.getBeanNamesForType(AwsKopipesContext::class.java).size shouldBe 0
        }
    }

    @Configuration(proxyBeanMethods = false)
    class OverridesConfig {
        @Bean
        fun inMemoryPublisher(): EventPublisher = EventPublisherInMemory()

        @Bean
        fun testDynamoDbClientFactory(): DynamoDbClientFactory = mockk()
    }

    @Configuration(proxyBeanMethods = false)
    class TwoPublishersConfig {
        @Bean
        @Primary
        fun primaryPublisher(): EventPublisher = EventPublisherInMemory()

        @Bean
        fun otherBusPublisher(): EventPublisher = EventPublisherInMemory()
    }

    @Configuration(proxyBeanMethods = false)
    class MicrostoreConfig {
        @Bean
        fun eventsMicrostore(kopipes: AwsKopipesContext): DynamoDbEventsMicrostore = DynamoDbEventsMicrostore(kopipes)
    }

    @Configuration(proxyBeanMethods = false)
    class SnapshotterConfig {
        @Bean
        fun customRecordSnapshotter(): RecordSnapshotter = object : RecordSnapshotter {
            override fun supports(record: Any): Boolean = record == "custom-record"
            override fun snapshot(record: Any): RecordSnapshot =
                RecordSnapshot(kind = "custom", payload = kotlinx.serialization.json.JsonObject(emptyMap()))
        }
    }
}
