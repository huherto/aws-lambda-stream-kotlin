package io.kopipes.aws.java

import io.kopipes.core.UnitOfWork
import io.kopipes.core.metrics.PipelineMetrics
import io.kopipes.core.metrics.collectMetrics
import io.kopipes.core.metrics.updateMetrics
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.map
import java.util.function.Function

/** Utility class providing Java-friendly methods for common pipeline creation. */
object Handlers {

    @JvmStatic
    fun collectMetrics(flow: Flow<UnitOfWork>): Flow<UnitOfWork> =
        flow.collectMetrics()

    @JvmStatic
    fun collectMetrics(flow: Flow<UnitOfWork>, functionMetrics: Map<String, Any>): Flow<UnitOfWork> =
        flow.collectMetrics(functionMetrics)

    @JvmStatic
    fun updateMetrics(flow: Flow<UnitOfWork>, transform: Function<PipelineMetrics, PipelineMetrics>): Flow<UnitOfWork> =
        flow.map { uow -> uow.updateMetrics { pm -> transform.apply(pm) } }
}
