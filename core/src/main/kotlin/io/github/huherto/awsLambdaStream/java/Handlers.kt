package io.github.huherto.awsLambdaStream.java

import io.github.huherto.awsLambdaStream.UnitOfWork
import io.github.huherto.awsLambdaStream.metrics.PipelineMetrics
import io.github.huherto.awsLambdaStream.metrics.collectMetrics
import io.github.huherto.awsLambdaStream.metrics.updateMetrics
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.map
import java.util.function.Function

/** Utility class providing Java-friendly methods for common pipeline creation. */
object Handlers {

//    @JvmStatic
//    fun assemblerBuilder(): PipelineAssembler.Builder =
//        PipelineAssembler.builder()


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
