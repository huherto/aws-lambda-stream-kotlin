package io.kopipes.core.metrics

import io.kopipes.core.GlobalRegistry.envConfig
import io.kopipes.core.UnitOfWork
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.onCompletion
import kotlinx.coroutines.flow.onEach

fun interface MetricsReporter {
    fun report(metrics: Map<String, Any>)
}

fun UnitOfWork.withMetrics(metrics: PipelineMetrics): UnitOfWork =
    this.withExtension(metrics)

val UnitOfWork.metrics: PipelineMetrics?
    get() = this.getExtension<PipelineMetrics>()

fun UnitOfWork.updateMetrics(transform: (PipelineMetrics) -> PipelineMetrics): UnitOfWork {
    val currentMetrics = this.metrics ?: PipelineMetrics()
    return this.withMetrics(transform(currentMetrics))
}

suspend fun UnitOfWork.withStepMetrics(
    step: String,
    block: suspend (UnitOfWork) -> UnitOfWork
): UnitOfWork {
    val uowWithStart = if (envConfig().isMetricEnabled("step")) {
        updateMetrics { it.startStep(step) }
    } else {
        this
    }

    val result = block(uowWithStart)

    return if (envConfig().isMetricEnabled("step")) {
        result.updateMetrics { it.endStep(step) }
    } else {
        result
    }
}

fun Flow<UnitOfWork>.collectMetrics(
    functionMetrics: Map<String, Any> = emptyMap(),
    reporter: MetricsReporter? = null
): Flow<UnitOfWork> {
    val collected = mutableListOf<UnitOfWork>()
    return this
        .onEach { collected.add(it) }
        .onCompletion {
            if (collected.isNotEmpty()) {
                val aggregated = CalculateMetrics.calculateMetrics(collected, functionMetrics)
                reporter?.report(aggregated)
            }
        }
}
