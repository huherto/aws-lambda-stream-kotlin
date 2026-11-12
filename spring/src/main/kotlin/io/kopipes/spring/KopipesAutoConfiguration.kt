package io.kopipes.spring

import io.kopipes.aws.AwsEnvironmentConfig
import io.kopipes.aws.AwsKopipesContext
import io.kopipes.aws.captureAwsOperations
import io.kopipes.aws.connectors.CloudWatchClientFactory
import io.kopipes.aws.connectors.DefaultCloudWatchClientFactory
import io.kopipes.aws.connectors.DefaultDynamoDbClientFactory
import io.kopipes.aws.connectors.DefaultEventBridgeClientFactory
import io.kopipes.aws.connectors.DefaultKmsClientFactory
import io.kopipes.aws.connectors.DefaultS3ClientFactory
import io.kopipes.aws.connectors.DynamoDbClientFactory
import io.kopipes.aws.connectors.EventBridgeClientFactory
import io.kopipes.aws.connectors.KmsClientFactory
import io.kopipes.aws.connectors.S3ClientFactory
import io.kopipes.aws.isAwsRetryable
import io.kopipes.aws.sinks.EventBridgePublisher
import io.kopipes.core.faults.FaultManager
import io.kopipes.core.serialization.snapshots.DefaultUnitOfWorkSnapshotter
import io.kopipes.core.serialization.snapshots.RecordSnapshotter
import io.kopipes.core.serialization.snapshots.UnitOfWorkSnapshotter
import io.kopipes.core.sinks.EventPublisher
import org.springframework.beans.factory.ObjectProvider
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
 * Defines the framework's collaborators as beans, each replaceable by declaring a bean of the same type, and assembles
 * them into an [AwsKopipesContext] bean for code that builds kopipes components from a context. kopipes keeps no global
 * state, so these beans are the only instances. Disable with `kopipes.enabled=false`.
 */
@AutoConfiguration
@ConditionalOnBooleanProperty(name = ["kopipes.enabled"], matchIfMissing = true)
class KopipesAutoConfiguration {

    @Bean
    @ConditionalOnMissingBean
    fun kopipesEnvironmentConfig(environment: Environment): AwsEnvironmentConfig = SpringEnvironmentConfig(environment)

    @Bean
    @ConditionalOnMissingBean
    fun kopipesUnitOfWorkSnapshotter(recordSnapshotters: ObjectProvider<RecordSnapshotter>): UnitOfWorkSnapshotter =
        DefaultUnitOfWorkSnapshotter(
            recordSnapshotters = recordSnapshotters.orderedStream().toList() + AwsKopipesContext.defaultRecordSnapshotters(),
            awsOperationCapturer = ::captureAwsOperations,
        )

    @Bean
    @ConditionalOnMissingBean
    fun kopipesFaultManager(
        envConfig: AwsEnvironmentConfig,
        eventPublisher: ObjectProvider<EventPublisher>,
        unitOfWorkSnapshotter: UnitOfWorkSnapshotter,
    ): FaultManager {
        val publisher = eventPublisher.getIfAvailable {
            throw IllegalStateException(
                "kopipes needs an EventPublisher bean to publish faults: add aws.sdk.kotlin:eventbridge to the " +
                    "classpath for the default EventBridgePublisher, or declare your own EventPublisher bean."
            )
        }
        return FaultManager.fromConfig(
            envConfig = envConfig,
            eventPublisher = publisher,
            unitOfWorkSnapshotter = unitOfWorkSnapshotter,
            isRetryable = ::isAwsRetryable,
        )
    }

    @Bean
    @ConditionalOnMissingBean
    fun kopipesContext(
        envConfig: AwsEnvironmentConfig,
        dynamoDbClientFactory: ObjectProvider<DynamoDbClientFactory>,
        eventBridgeClientFactory: ObjectProvider<EventBridgeClientFactory>,
        s3ClientFactory: ObjectProvider<S3ClientFactory>,
        kmsClientFactory: ObjectProvider<KmsClientFactory>,
        cloudWatchClientFactory: ObjectProvider<CloudWatchClientFactory>,
        unitOfWorkSnapshotter: UnitOfWorkSnapshotter,
        faultManager: FaultManager,
    ): AwsKopipesContext = AwsKopipesContext(
        envConfig = envConfig,
        // A factory whose SDK is not on the classpath has no bean; the default is created but never used.
        dynamoDbClientFactory = dynamoDbClientFactory.getIfAvailable { DefaultDynamoDbClientFactory(envConfig) },
        eventBridgeClientFactory = eventBridgeClientFactory.getIfAvailable { DefaultEventBridgeClientFactory(envConfig) },
        s3ClientFactory = s3ClientFactory.getIfAvailable { DefaultS3ClientFactory(envConfig) },
        kmsClientFactory = kmsClientFactory.getIfAvailable { DefaultKmsClientFactory(envConfig) },
        cloudWatchClientFactory = cloudWatchClientFactory.getIfAvailable { DefaultCloudWatchClientFactory(envConfig) },
        unitOfWorkSnapshotter = unitOfWorkSnapshotter,
        eventPublisher = faultManager.publisher(),
        faultManager = faultManager,
    )

    // Each client factory, and the default publisher, exists only when its AWS SDK is on the classpath.

    @Configuration(proxyBeanMethods = false)
    @ConditionalOnClass(name = ["aws.sdk.kotlin.services.dynamodb.DynamoDbClient"])
    class DynamoDbConfiguration {
        @Bean
        @ConditionalOnMissingBean
        fun kopipesDynamoDbClientFactory(envConfig: AwsEnvironmentConfig): DynamoDbClientFactory =
            DefaultDynamoDbClientFactory(envConfig)
    }

    @Configuration(proxyBeanMethods = false)
    @ConditionalOnClass(name = ["aws.sdk.kotlin.services.s3.S3Client"])
    class S3Configuration {
        @Bean
        @ConditionalOnMissingBean
        fun kopipesS3ClientFactory(envConfig: AwsEnvironmentConfig): S3ClientFactory = DefaultS3ClientFactory(envConfig)
    }

    @Configuration(proxyBeanMethods = false)
    @ConditionalOnClass(name = ["aws.sdk.kotlin.services.kms.KmsClient"])
    class KmsConfiguration {
        @Bean
        @ConditionalOnMissingBean
        fun kopipesKmsClientFactory(envConfig: AwsEnvironmentConfig): KmsClientFactory = DefaultKmsClientFactory(envConfig)
    }

    @Configuration(proxyBeanMethods = false)
    @ConditionalOnClass(name = ["aws.sdk.kotlin.services.cloudwatch.CloudWatchClient"])
    class CloudWatchConfiguration {
        @Bean
        @ConditionalOnMissingBean
        fun kopipesCloudWatchClientFactory(envConfig: AwsEnvironmentConfig): CloudWatchClientFactory =
            DefaultCloudWatchClientFactory(envConfig)
    }

    @Configuration(proxyBeanMethods = false)
    @ConditionalOnClass(name = ["aws.sdk.kotlin.services.eventbridge.EventBridgeClient"])
    class EventBridgeConfiguration {

        @Bean
        @ConditionalOnMissingBean
        fun kopipesEventBridgeClientFactory(envConfig: AwsEnvironmentConfig): EventBridgeClientFactory =
            DefaultEventBridgeClientFactory(envConfig)

        @Bean
        @ConditionalOnMissingBean
        fun kopipesEventPublisher(
            envConfig: AwsEnvironmentConfig,
            eventBridgeClientFactory: EventBridgeClientFactory,
        ): EventPublisher = EventBridgePublisher(envConfig, eventBridgeClientFactory)
    }
}
