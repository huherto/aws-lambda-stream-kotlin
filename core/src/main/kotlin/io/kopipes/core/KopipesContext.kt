package io.kopipes.core

import io.kopipes.core.faults.FaultManager
import io.kopipes.core.serialization.snapshots.DefaultUnitOfWorkSnapshotter
import io.kopipes.core.serialization.snapshots.UnitOfWorkSnapshotter
import io.kopipes.core.sinks.EventPublisher
import io.kopipes.core.sinks.EventPublisherInMemory

/**
 * The collaborators a kopipes application shares across its pipelines.
 *
 * There is no global registry: an application builds one context at start-up (by hand, or as a Spring bean) and passes
 * it, or the pieces it needs, to the components it creates. Platform modules extend it, e.g. `AwsKopipesContext`.
 */
interface KopipesContext {
    val envConfig: EnvironmentConfig
    val eventPublisher: EventPublisher
    val unitOfWorkSnapshotter: UnitOfWorkSnapshotter
    val faultManager: FaultManager
}

/** A [KopipesContext] with no platform dependencies. Its default publisher keeps events in memory, which suits tests. */
class BasicKopipesContext @JvmOverloads constructor(
    override val envConfig: EnvironmentConfig = EnvironmentConfig(),
    override val eventPublisher: EventPublisher = EventPublisherInMemory(),
    override val unitOfWorkSnapshotter: UnitOfWorkSnapshotter = DefaultUnitOfWorkSnapshotter(),
    isRetryable: (Throwable) -> Boolean = { false },
    override val faultManager: FaultManager = FaultManager.fromConfig(
        envConfig = envConfig,
        eventPublisher = eventPublisher,
        unitOfWorkSnapshotter = unitOfWorkSnapshotter,
        isRetryable = isRetryable,
    ),
) : KopipesContext
