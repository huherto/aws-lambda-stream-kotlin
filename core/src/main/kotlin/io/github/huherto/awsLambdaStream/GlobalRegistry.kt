package io.github.huherto.awsLambdaStream

import io.github.huherto.awsLambdaStream.sinks.EventBridgePublisher
import io.github.huherto.awsLambdaStream.sinks.EventPublisher

object GlobalRegistry {

    @Volatile
    private var _envConfig: EnvironmentConfig? = null

    @Volatile
    private var _eventPublisher: EventPublisher? = null

    @Volatile
    private var _faultManager: FaultManager? = null

    fun envConfig(): EnvironmentConfig {
        return _envConfig ?: synchronized(this) {
            _envConfig ?: EnvironmentConfig().also { _envConfig = it }
        }
    }

    fun setEnvConfig(config: EnvironmentConfig) {
        _envConfig = config
    }

    fun eventPublisher(): EventPublisher {
        return _eventPublisher ?: synchronized(this) {
            _eventPublisher ?: EventBridgePublisher(envConfig()).also { _eventPublisher = it }
        }
    }

    fun setEventPublisher(publisher: EventPublisher) {
        _eventPublisher = publisher
    }

    fun faultManager(): FaultManager {
        return _faultManager ?: synchronized(this) {
            _faultManager ?: FaultManager(envConfig(), eventPublisher()).also { _faultManager = it }
        }
    }

    fun setFaultManager(manager: FaultManager) {
        _faultManager = manager
    }

    fun reset() {
        _envConfig = null
        _eventPublisher = null
        _faultManager = null
    }

}