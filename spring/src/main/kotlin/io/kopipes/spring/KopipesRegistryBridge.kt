package io.kopipes.spring

import io.kopipes.aws.AwsEnvironmentConfig
import io.kopipes.aws.AwsGlobalRegistry
import io.kopipes.aws.connectors.CloudWatchClientFactory
import io.kopipes.aws.connectors.DynamoDbClientFactory
import io.kopipes.aws.connectors.EventBridgeClientFactory
import io.kopipes.aws.connectors.KmsClientFactory
import io.kopipes.aws.connectors.S3ClientFactory
import io.kopipes.core.EnvironmentConfig
import io.kopipes.core.GlobalRegistry
import io.kopipes.core.faults.FaultManager
import io.kopipes.core.serialization.snapshots.RecordSnapshotter
import io.kopipes.core.serialization.snapshots.UnitOfWorkSnapshotter
import io.kopipes.core.sinks.EventPublisher
import mu.KotlinLogging
import org.springframework.beans.factory.BeanFactory
import org.springframework.beans.factory.BeanFactoryAware
import org.springframework.beans.factory.config.BeanPostProcessor
import org.springframework.beans.factory.config.ConfigurableListableBeanFactory

/**
 * Pushes kopipes beans into [GlobalRegistry] / [AwsGlobalRegistry] as Spring creates them.
 *
 * Much of the framework reads its collaborators from the global registry (default constructor arguments such as
 * `awsEnvConfig()`, `GlobalRegistry.faultManager()`, `AwsGlobalRegistry.dynamoDbClientFactory()`). Bridging means that code
 * gets the same instances Spring injects, whether it was written for Spring or not.
 *
 * When there are several beans of a type, only the `@Primary` one is pushed: a second [EventPublisher] for another bus
 * must not replace the application's default publisher.
 */
class KopipesRegistryBridge : BeanPostProcessor, BeanFactoryAware {

    private val logger = KotlinLogging.logger { }

    private var beanFactory: ConfigurableListableBeanFactory? = null

    override fun setBeanFactory(beanFactory: BeanFactory) {
        this.beanFactory = beanFactory as? ConfigurableListableBeanFactory
    }

    // The registry drops its cached publisher and fault manager when the env config changes, and its fault manager when
    // the publisher changes. Spring may create these beans in any order, so remember them and re-apply after a reset.
    private var publisher: EventPublisher? = null
    private var faultManager: FaultManager? = null

    override fun postProcessAfterInitialization(bean: Any, beanName: String): Any {
        if (bean is EnvironmentConfig && isDefault(beanName, EnvironmentConfig::class.java)) {
            if (bean is AwsEnvironmentConfig) AwsGlobalRegistry.setEnvConfig(bean) else GlobalRegistry.setEnvConfig(bean)
            publisher?.let { GlobalRegistry.setEventPublisher(it) }
            faultManager?.let { GlobalRegistry.setFaultManager(it) }
            logger.debug { "registered EnvironmentConfig bean '$beanName'" }
        }
        if (bean is EventPublisher && isDefault(beanName, EventPublisher::class.java)) {
            publisher = bean
            GlobalRegistry.setEventPublisher(bean)
            faultManager?.let { GlobalRegistry.setFaultManager(it) }
            logger.debug { "registered EventPublisher bean '$beanName'" }
        }
        if (bean is FaultManager && isDefault(beanName, FaultManager::class.java)) {
            faultManager = bean
            GlobalRegistry.setFaultManager(bean)
            logger.debug { "registered FaultManager bean '$beanName'" }
        }
        if (bean is RecordSnapshotter) {
            // Additive: every record snapshotter bean contributes.
            GlobalRegistry.registerRecordSnapshotter(bean)
        }
        if (bean is UnitOfWorkSnapshotter && isDefault(beanName, UnitOfWorkSnapshotter::class.java)) {
            GlobalRegistry.setUnitOfWorkSnapshotter(bean)
        }
        if (bean is DynamoDbClientFactory && isDefault(beanName, DynamoDbClientFactory::class.java)) {
            AwsGlobalRegistry.setDynamoDbClientFactory(bean)
        }
        if (bean is EventBridgeClientFactory && isDefault(beanName, EventBridgeClientFactory::class.java)) {
            AwsGlobalRegistry.setEventBridgeClientFactory(bean)
        }
        if (bean is S3ClientFactory && isDefault(beanName, S3ClientFactory::class.java)) {
            AwsGlobalRegistry.setS3ClientFactory(bean)
        }
        if (bean is KmsClientFactory && isDefault(beanName, KmsClientFactory::class.java)) {
            AwsGlobalRegistry.setKmsClientFactory(bean)
        }
        if (bean is CloudWatchClientFactory && isDefault(beanName, CloudWatchClientFactory::class.java)) {
            AwsGlobalRegistry.setCloudWatchClientFactory(bean)
        }
        return bean
    }

    /** True when [beanName] is the bean an unqualified injection of [type] would resolve to. */
    internal fun isDefault(beanName: String, type: Class<*>): Boolean {
        val factory = beanFactory ?: return true
        val names = factory.getBeanNamesForType(type, true, false)
        if (names.size <= 1) return true
        return factory.containsBeanDefinition(beanName) && factory.getBeanDefinition(beanName).isPrimary
    }
}
