package io.kopipes.spring

import io.kopipes.aws.AwsEnvironmentConfig
import io.kopipes.aws.AwsGlobalRegistry
import io.kopipes.aws.connectors.EventBridgeClientFactory
import io.kopipes.aws.sinks.EventBridgePublisher
import io.kopipes.core.EnvironmentConfig
import io.kopipes.core.faults.FaultManager
import io.kopipes.core.serialization.snapshots.DefaultUnitOfWorkSnapshotter
import io.kopipes.core.serialization.snapshots.RecordSnapshotter
import io.kopipes.core.serialization.snapshots.UnitOfWorkSnapshotter
import io.kopipes.core.sinks.EventPublisher
import org.springframework.beans.factory.ObjectProvider
import org.springframework.beans.factory.config.BeanFactoryPostProcessor
import org.springframework.boot.autoconfigure.AutoConfiguration
import org.springframework.boot.autoconfigure.condition.ConditionalOnBooleanProperty
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.core.env.Environment

/**
 * Spring Boot auto-configuration for kopipes.
 *
 * Defines the framework's shared collaborators as beans, each replaceable by declaring a bean of the same type, and
 * mirrors them into the global registry through [KopipesRegistryBridge]. Disable it with `kopipes.enabled=false`.
 */
@AutoConfiguration
@ConditionalOnBooleanProperty(name = ["kopipes.enabled"], matchIfMissing = true)
class KopipesAutoConfiguration {

    companion object {
        /**
         * Points the registry at the Spring Environment before any singleton is created, so beans that call
         * `awsEnvConfig()` while they are being constructed already see `application.yml` values.
         */
        @Bean
        @JvmStatic
        fun kopipesEarlyEnvironmentConfig(environment: Environment): BeanFactoryPostProcessor =
            BeanFactoryPostProcessor { AwsGlobalRegistry.setEnvConfig(SpringEnvironmentConfig(environment)) }

        @Bean
        @JvmStatic
        fun kopipesRegistryBridge(): KopipesRegistryBridge = KopipesRegistryBridge()
    }

    @Bean
    @ConditionalOnMissingBean(EnvironmentConfig::class)
    fun kopipesEnvironmentConfig(environment: Environment): AwsEnvironmentConfig = SpringEnvironmentConfig(environment)

    @Bean
    @ConditionalOnMissingBean
    fun kopipesFaultManager(envConfig: EnvironmentConfig, eventPublisher: ObjectProvider<EventPublisher>): FaultManager {
        val publisher = eventPublisher.getIfAvailable {
            throw IllegalStateException(
                "kopipes needs an EventPublisher bean to publish faults: add aws.sdk.kotlin:eventbridge to the " +
                    "classpath for the default EventBridgePublisher, or declare your own EventPublisher bean."
            )
        }
        return FaultManager(
            eventPublisher = publisher,
            isStreamRetryEnabled = envConfig.streamRetryEnabled(),
            isItemLevelRetryEnabled = envConfig.itemLevelRetryEnabled(),
            awsLambdaFunctionName = envConfig.awsLambdaFunctionName() ?: "undefined",
            isRetryable = AwsGlobalRegistry.isAwsRetryable,
        )
    }

    @Bean
    @ConditionalOnMissingBean
    fun kopipesUnitOfWorkSnapshotter(recordSnapshotters: ObjectProvider<RecordSnapshotter>): UnitOfWorkSnapshotter {
        // Creating the RecordSnapshotter beans first lets the bridge register them before this snapshotter reads the list.
        val beans = recordSnapshotters.orderedStream().toList()
        return DefaultUnitOfWorkSnapshotter(
            recordSnapshotters = (beans + AwsGlobalRegistry.registeredRecordSnapshotters()).distinct(),
            awsOperationCapturer = AwsGlobalRegistry::captureAwsOperations,
        )
    }

    /** The default publisher, only when the EventBridge SDK is on the classpath. */
    @Configuration(proxyBeanMethods = false)
    @ConditionalOnClass(name = ["aws.sdk.kotlin.services.eventbridge.EventBridgeClient"])
    class EventBridgeConfiguration {

        @Bean
        @ConditionalOnMissingBean
        fun kopipesEventPublisher(
            envConfig: EnvironmentConfig,
            clientFactory: ObjectProvider<EventBridgeClientFactory>,
        ): EventPublisher {
            val cfg = envConfig as? AwsEnvironmentConfig ?: AwsEnvironmentConfig.defaultInstance
            return EventBridgePublisher(
                busName = cfg.busName() ?: "undefined",
                source = cfg.busSource() ?: "custom",
                maxPublishRequestSize = cfg.maxPublishRequestSize() ?: cfg.maxRequestSize() ?: (256 * 1024),
                batchSize = cfg.publishBatchSize() ?: cfg.batchSize() ?: 10,
                parallel = cfg.publishParallel() ?: cfg.parallel() ?: 8,
                endpointId = cfg.busEndPointId(),
                clientFactory = clientFactory.getIfAvailable { AwsGlobalRegistry.eventBridgeClientFactory() },
            )
        }
    }
}
