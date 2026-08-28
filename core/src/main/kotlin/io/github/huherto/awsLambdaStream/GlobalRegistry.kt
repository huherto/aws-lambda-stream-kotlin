package io.github.huherto.awsLambdaStream

import io.github.huherto.awsLambdaStream.connectors.DefaultDynamoDbClientFactory
import io.github.huherto.awsLambdaStream.connectors.DefaultS3ClientFactory
import io.github.huherto.awsLambdaStream.connectors.DynamoDbClientFactory
import io.github.huherto.awsLambdaStream.connectors.S3ClientFactory
import io.github.huherto.awsLambdaStream.faults.FaultManager
import io.github.huherto.awsLambdaStream.sinks.EventBridgePublisher
import io.github.huherto.awsLambdaStream.sinks.EventPublisher

private class RegistrySingleton<T>(
    private val lock: Any,
    private val defaultFactory: () -> T,
    private val onChange: () -> Unit = {},
) {
    @Volatile
    private var factory: () -> T = defaultFactory

    @Volatile
    private var instance: T? = null

    fun get(): T {
        return instance ?: synchronized(lock) {
            instance ?: factory().also { instance = it }
        }
    }

    fun set(value: T) {
        synchronized(lock) {
            instance = value
            onChange()
        }
    }

    fun setFactory(factory: () -> T) {
        synchronized(lock) {
            this.factory = factory
            instance = null
            onChange()
        }
    }

    fun clear() {
        synchronized(lock) {
            instance = null
        }
    }

    fun reset() {
        synchronized(lock) {
            factory = defaultFactory
            instance = null
        }
    }
}


fun envConfig() : EnvironmentConfig {
    return GlobalRegistry.envConfig()
}

object GlobalRegistry {

    private val lock = Any()

    private val envConfigSingleton = RegistrySingleton(
        lock = lock,
        defaultFactory = { EnvironmentConfig() },
        onChange = {
            eventPublisherSingleton.clear()
            faultManagerSingleton.clear()
        },
    )

    private val eventPublisherSingleton = RegistrySingleton(
        lock = lock,
        defaultFactory = { EventBridgePublisher() as EventPublisher },
        onChange = {
            faultManagerSingleton.clear()
        },
    )

    private val faultManagerSingleton = RegistrySingleton(
        lock = lock,
        defaultFactory = { FaultManager(eventPublisher()) },
    )

    fun envConfig(): EnvironmentConfig {
        return envConfigSingleton.get()
    }

    fun setEnvConfig(config: EnvironmentConfig) {
        envConfigSingleton.set(config)
    }

    fun setEnvConfigFactory(factory: () -> EnvironmentConfig) {
        envConfigSingleton.setFactory(factory)
    }

    fun eventPublisher(): EventPublisher {
        return eventPublisherSingleton.get()
    }

    fun setEventPublisher(publisher: EventPublisher) {
        eventPublisherSingleton.set(publisher)
    }

    fun setEventPublisherFactory(factory: () -> EventPublisher) {
        eventPublisherSingleton.setFactory(factory)
    }

    fun faultManager(): FaultManager {
        return faultManagerSingleton.get()
    }

    fun setFaultManager(manager: FaultManager) {
        faultManagerSingleton.set(manager)
    }

    fun setFaultManagerFactory(factory: () -> FaultManager) {
        faultManagerSingleton.setFactory(factory)
    }

    private val dynamoDbClientFactorySingleton = RegistrySingleton(
        lock = lock,
        defaultFactory = { DefaultDynamoDbClientFactory() as DynamoDbClientFactory }
    )

    fun dynamoDbClientFactory() : DynamoDbClientFactory {
        return dynamoDbClientFactorySingleton.get()
    }

    fun setDynamoDbClientFactory(dynamoDbClientFactory: DynamoDbClientFactory) {
        dynamoDbClientFactorySingleton.set(dynamoDbClientFactory)
    }

    fun setDynamoDbClientFactory(factory: () -> DynamoDbClientFactory) {
        dynamoDbClientFactorySingleton.setFactory(factory)
    }

    private val s3ClientFactorySingleton = RegistrySingleton(
        lock = lock,
        defaultFactory = { DefaultS3ClientFactory() as S3ClientFactory }
    )

    fun s3ClientFactory() : S3ClientFactory {
        return s3ClientFactorySingleton.get()
    }

    fun setS3ClientFactory(s3ClientFactory: S3ClientFactory) {
        s3ClientFactorySingleton.set(s3ClientFactory)
    }

    fun setS3ClientFactory(factory: () -> S3ClientFactory) {
        s3ClientFactorySingleton.setFactory(factory)
    }

    fun reset() {
        synchronized(lock) {
            envConfigSingleton.reset()
            eventPublisherSingleton.reset()
            faultManagerSingleton.reset()
            dynamoDbClientFactorySingleton.reset()
            s3ClientFactorySingleton.reset()
        }
    }

}