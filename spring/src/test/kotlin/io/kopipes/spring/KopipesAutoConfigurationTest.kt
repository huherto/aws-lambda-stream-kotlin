package io.kopipes.spring

import io.kopipes.aws.AwsGlobalRegistry
import io.kopipes.aws.awsEnvConfig
import io.kopipes.aws.sinks.EventBridgePublisher
import io.kopipes.core.EnvironmentConfig
import io.kopipes.core.GlobalRegistry
import io.kopipes.core.faults.FaultManager
import io.kopipes.core.serialization.snapshots.RecordSnapshot
import io.kopipes.core.serialization.snapshots.RecordSnapshotter
import io.kopipes.core.serialization.snapshots.UnitOfWorkSnapshotter
import io.kopipes.core.sinks.EventPublisher
import io.kopipes.core.sinks.EventPublisherInMemory
import io.kotest.matchers.collections.shouldContain
import io.kotest.matchers.nulls.shouldNotBeNull
import io.kotest.matchers.shouldBe
import io.kotest.matchers.string.shouldContain
import io.kotest.matchers.types.shouldBeInstanceOf
import io.kotest.matchers.types.shouldBeSameInstanceAs
import org.junit.jupiter.api.AfterEach
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

    @AfterEach
    fun resetRegistry() {
        AwsGlobalRegistry.reset()
    }

    @Test
    fun `defines the default beans and mirrors them into the registry`() {
        // arrange
        val withProps = runner.withPropertyValues("kopipes.bus-name=my-bus", "kopipes.stream-retry-enabled=true")

        // act & assert
        withProps.run { context ->
            val envConfig = context.getBean(EnvironmentConfig::class.java)
            val publisher = context.getBean(EventPublisher::class.java)
            val faultManager = context.getBean(FaultManager::class.java)
            val snapshotter = context.getBean(UnitOfWorkSnapshotter::class.java)

            envConfig.shouldBeInstanceOf<SpringEnvironmentConfig>()
            publisher.shouldBeInstanceOf<EventBridgePublisher>().busName shouldBe "my-bus"
            faultManager.publisher() shouldBeSameInstanceAs publisher

            AwsGlobalRegistry.envConfig() shouldBeSameInstanceAs envConfig
            GlobalRegistry.eventPublisher() shouldBeSameInstanceAs publisher
            GlobalRegistry.faultManager() shouldBeSameInstanceAs faultManager
            GlobalRegistry.unitOfWorkSnapshotter() shouldBeSameInstanceAs snapshotter
            awsEnvConfig().streamRetryEnabled() shouldBe true
        }
    }

    @Test
    fun `an application EventPublisher replaces the default`() {
        // arrange
        val withUserPublisher = runner.withUserConfiguration(InMemoryPublisherConfig::class.java)

        // act & assert
        withUserPublisher.run { context ->
            val publisher = context.getBean(EventPublisher::class.java)

            publisher.shouldBeInstanceOf<EventPublisherInMemory>()
            context.getBean(FaultManager::class.java).publisher() shouldBeSameInstanceAs publisher
            GlobalRegistry.eventPublisher() shouldBeSameInstanceAs publisher
        }
    }

    @Test
    fun `with several publishers only the primary one goes to the registry`() {
        // arrange
        val withTwoPublishers = runner.withUserConfiguration(TwoPublishersConfig::class.java)

        // act & assert
        withTwoPublishers.run { context ->
            val primary = context.getBean("primaryPublisher", EventPublisher::class.java)

            GlobalRegistry.eventPublisher() shouldBeSameInstanceAs primary
            context.getBean(FaultManager::class.java).publisher() shouldBeSameInstanceAs primary
        }
    }

    @Test
    fun `beans built before the auto-configuration already see Spring properties`() {
        // arrange
        val withEarlyBean = runner
            .withPropertyValues("kopipes.event-table-name=events-table")
            .withUserConfiguration(InMemoryPublisherConfig::class.java, EarlyReaderConfig::class.java)

        // act & assert
        withEarlyBean.run { context ->
            context.getBean(EarlyReader::class.java).tableName shouldBe "events-table"
        }
    }

    @Test
    fun `record snapshotter beans are registered`() {
        // arrange
        val withSnapshotter = runner.withUserConfiguration(InMemoryPublisherConfig::class.java, SnapshotterConfig::class.java)

        // act & assert
        withSnapshotter.run { context ->
            val snapshotter = context.getBean(RecordSnapshotter::class.java)

            GlobalRegistry.registeredRecordSnapshotters() shouldContain snapshotter
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
            context.getBeanNamesForType(EnvironmentConfig::class.java).size shouldBe 0
        }
    }

    @Configuration(proxyBeanMethods = false)
    class InMemoryPublisherConfig {
        @Bean
        fun inMemoryPublisher(): EventPublisher = EventPublisherInMemory()
    }

    @Configuration(proxyBeanMethods = false)
    class TwoPublishersConfig {
        @Bean
        @Primary
        fun primaryPublisher(): EventPublisher = EventPublisherInMemory()

        @Bean
        fun otherBusPublisher(): EventPublisher = EventPublisherInMemory()
    }

    class EarlyReader(val tableName: String?)

    @Configuration(proxyBeanMethods = false)
    class EarlyReaderConfig {
        @Bean
        fun earlyReader(): EarlyReader = EarlyReader(awsEnvConfig().tableName())
    }

    @Configuration(proxyBeanMethods = false)
    class SnapshotterConfig {
        @Bean
        fun customRecordSnapshotter(): RecordSnapshotter = object : RecordSnapshotter {
            override fun supports(record: Any): Boolean = false
            override fun snapshot(record: Any): RecordSnapshot = error("unused")
        }
    }
}
