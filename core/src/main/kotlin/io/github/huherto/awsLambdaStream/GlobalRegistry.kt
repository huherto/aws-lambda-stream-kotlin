package io.github.huherto.awsLambdaStream

import io.github.huherto.awsLambdaStream.connectors.DefaultDynamoDbClientFactory
import io.github.huherto.awsLambdaStream.connectors.DefaultS3ClientFactory
import io.github.huherto.awsLambdaStream.connectors.DynamoDbConnector
import io.github.huherto.awsLambdaStream.connectors.S3Connector
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

    private val dynamoDbConnectorSingleton = RegistrySingleton(
        lock = lock,
        defaultFactory = { DynamoDbConnector(
            {},
            false,
            DefaultDynamoDbClientFactory() )
        }
    )

    fun dynamoDbConnector() : DynamoDbConnector {
        return dynamoDbConnectorSingleton.get()
    }

    fun setDynamoDbConnector(dynamoDbConnector: DynamoDbConnector) {
        dynamoDbConnectorSingleton.set(dynamoDbConnector)
    }

    fun setDynamoDbConnectorFactory(factory: () -> DynamoDbConnector) {
        dynamoDbConnectorSingleton.setFactory(factory)
    }

    private val s3ConnectorSingleton = RegistrySingleton(
        lock = lock,
        defaultFactory = {
            S3Connector(
            clientFactory = DefaultS3ClientFactory(),
                debug = {} )
        }
    )

    fun s3Connector() : S3Connector {
        return s3ConnectorSingleton.get()
    }

    fun setS3Connector(s3Connector: S3Connector) {
        s3ConnectorSingleton.set(s3Connector)
    }

    fun setS3ConnectorFactory(factory: () -> S3Connector) {
        s3ConnectorSingleton.setFactory(factory)
    }

    fun reset() {
        synchronized(lock) {
            envConfigSingleton.reset()
            eventPublisherSingleton.reset()
            faultManagerSingleton.reset()
            dynamoDbConnectorSingleton.reset()
            s3ConnectorSingleton.reset()
        }
    }

}