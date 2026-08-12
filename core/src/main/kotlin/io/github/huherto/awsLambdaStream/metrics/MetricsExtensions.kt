package io.github.huherto.awsLambdaStream.metrics

import io.github.huherto.awsLambdaStream.EnvironmentConfig
import io.github.huherto.awsLambdaStream.UnitOfWork
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.onCompletion
import kotlinx.coroutines.flow.onEach

fun UnitOfWork.withMetrics(metrics: PipelineMetrics): UnitOfWork =
    this.withExtension(metrics)

val UnitOfWork.metrics: PipelineMetrics?
    get() = this.getExtension<PipelineMetrics>()

fun UnitOfWork.updateMetrics(transform: (PipelineMetrics) -> PipelineMetrics): UnitOfWork {
    val currentMetrics = this.metrics ?: PipelineMetrics()
    return this.withMetrics(transform(currentMetrics))
}

fun Flow<UnitOfWork>.collectMetrics(
    envConfig: EnvironmentConfig,
    functionMetrics: Map<String, Any> = emptyMap()
): Flow<UnitOfWork> {
    val collected = mutableListOf<UnitOfWork>()
    return this
        .onEach { collected.add(it) }
        .onCompletion {
            if (collected.isNotEmpty()) {
                val aggregated = CalculateMetrics.calculateMetrics(collected, functionMetrics)
                EmfReporter.logMetrics(aggregated, envConfig)
            }
        }
}
