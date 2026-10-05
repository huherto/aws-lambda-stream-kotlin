package io.kopipes.core

import io.kopipes.core.faults.FaultManager
import io.kopipes.core.serialization.snapshots.DefaultUnitOfWorkSnapshotter
import io.kopipes.core.serialization.snapshots.RecordSnapshotter
import io.kopipes.core.serialization.snapshots.UnitOfWorkSnapshotter
import io.kopipes.core.sinks.EventPublisher
import io.kopipes.core.sinks.EventPublisherInMemory
import java.util.concurrent.CopyOnWriteArrayList

/** Internal registry for singleton instances. */
class RegistrySingleton<T>(
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

fun envConfig(): EnvironmentConfig {
    return GlobalRegistry.envConfig()
}

/** Singleton registry for global components. */
object GlobalRegistry {

    private val lock = Any()

    private val recordSnapshottersList = CopyOnWriteArrayList<RecordSnapshotter>()

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
        defaultFactory = { EventPublisherInMemory() as EventPublisher },
        onChange = {
            faultManagerSingleton.clear()
        },
    )

    private val faultManagerSingleton = RegistrySingleton(
        lock = lock,
        defaultFactory = { FaultManager(eventPublisher()) },
    )

    private val unitOfWorkSnapshotterSingleton = RegistrySingleton(
        lock = lock,
        defaultFactory = { DefaultUnitOfWorkSnapshotter(recordSnapshotters = recordSnapshottersList.toList()) as UnitOfWorkSnapshotter },
    )

    @JvmStatic
    fun envConfig(): EnvironmentConfig {
        return envConfigSingleton.get()
    }

    @JvmStatic
    fun setEnvConfig(config: EnvironmentConfig) {
        envConfigSingleton.set(config)
    }

    @JvmStatic
    fun setEnvConfigFactory(factory: () -> EnvironmentConfig) {
        envConfigSingleton.setFactory(factory)
    }

    @JvmStatic
    fun eventPublisher(): EventPublisher {
        return eventPublisherSingleton.get()
    }

    @JvmStatic
    fun setEventPublisher(publisher: EventPublisher) {
        eventPublisherSingleton.set(publisher)
    }

    @JvmStatic
    fun setEventPublisherFactory(factory: () -> EventPublisher) {
        eventPublisherSingleton.setFactory(factory)
    }

    @JvmStatic
    fun faultManager(): FaultManager {
        return faultManagerSingleton.get()
    }

    @JvmStatic
    fun setFaultManager(manager: FaultManager) {
        faultManagerSingleton.set(manager)
    }

    @JvmStatic
    fun setFaultManagerFactory(factory: () -> FaultManager) {
        faultManagerSingleton.setFactory(factory)
    }

    @JvmStatic
    fun unitOfWorkSnapshotter(): UnitOfWorkSnapshotter {
        return unitOfWorkSnapshotterSingleton.get()
    }

    @JvmStatic
    fun setUnitOfWorkSnapshotter(snapshotter: UnitOfWorkSnapshotter) {
        unitOfWorkSnapshotterSingleton.set(snapshotter)
    }

    @JvmStatic
    fun setUnitOfWorkSnapshotterFactory(factory: () -> UnitOfWorkSnapshotter) {
        unitOfWorkSnapshotterSingleton.setFactory(factory)
    }

    @JvmStatic
    fun registerRecordSnapshotter(snapshotter: RecordSnapshotter) {
        recordSnapshottersList.addIfAbsent(snapshotter)
        unitOfWorkSnapshotterSingleton.clear()
    }

    @JvmStatic
    fun registeredRecordSnapshotters(): List<RecordSnapshotter> {
        return recordSnapshottersList.toList()
    }

    @JvmStatic
    fun reset() {
        synchronized(lock) {
            envConfigSingleton.reset()
            eventPublisherSingleton.reset()
            faultManagerSingleton.reset()
            unitOfWorkSnapshotterSingleton.reset()
            recordSnapshottersList.clear()
        }
    }
}
