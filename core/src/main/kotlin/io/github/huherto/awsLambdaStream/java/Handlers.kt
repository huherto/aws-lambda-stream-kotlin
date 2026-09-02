package io.github.huherto.awsLambdaStream.java

import io.github.huherto.awsLambdaStream.Event
import io.github.huherto.awsLambdaStream.EventCodec
import io.github.huherto.awsLambdaStream.PipelineAssembler
import io.github.huherto.awsLambdaStream.UnitOfWork
import io.github.huherto.awsLambdaStream.filters.EventFilter
import io.github.huherto.awsLambdaStream.flavors.CdcPipeline
import io.github.huherto.awsLambdaStream.flavors.CollectPipeline
import io.github.huherto.awsLambdaStream.flavors.CorrelatePipeline
import io.github.huherto.awsLambdaStream.flavors.EvaluatePipeline
import io.github.huherto.awsLambdaStream.metrics.PipelineMetrics
import io.github.huherto.awsLambdaStream.metrics.collectMetrics
import io.github.huherto.awsLambdaStream.metrics.updateMetrics
import io.github.huherto.awsLambdaStream.sinks.EventPublisher
import io.github.huherto.awsLambdaStream.sinks.EventsMicrostore
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.map
import java.util.function.Function

/** Utility class providing Java-friendly methods for common pipeline creation. */
object Handlers {

    @JvmStatic
    fun assemblerBuilder(): PipelineAssembler.Builder =
        PipelineAssembler.builder()

    @JvmStatic
    @JvmOverloads
    fun cdcPipeline(
        id: String,
        eventPublisher: EventPublisher,
        toEvent: (UnitOfWork) -> Event?,
        eventFilter: EventFilter = EventFilter.Any
    ): CdcPipeline = CdcPipeline(
        id = id,
        eventPublisher = eventPublisher,
        toEvent = { uow -> toEvent(uow) },
        eventFilter = eventFilter
    )

    @JvmStatic
    @JvmOverloads
    fun collectPipeline(
        id: String,
        eventsMicrostore: EventsMicrostore,
        eventFilter: EventFilter = EventFilter.Any,
        correlationKey: (UnitOfWork) -> String? = { uow -> uow.event?.partitionKey }
    ): CollectPipeline = CollectPipeline(
        pipelineId = id,
        eventsMicrostore = eventsMicrostore,
        eventFilter = eventFilter,
        correlationKey = correlationKey
    )

    @JvmStatic
    @JvmOverloads
    fun correlatePipeline(
        id: String,
        eventsMicrostore: EventsMicrostore,
        eventCodec: EventCodec,
        correlationKeySupplier: (UnitOfWork) -> String,
        eventFilter: EventFilter = EventFilter.Any
    ): CorrelatePipeline = CorrelatePipeline(
        id = id,
        eventsMicrostore = eventsMicrostore,
        eventCodec = eventCodec,
        correlationKeySupplier = correlationKeySupplier,
        eventFilter = eventFilter
    )

    @JvmStatic
    @JvmOverloads
    fun evaluatePipeline(
        id: String,
        eventPublisher: EventPublisher,
        eventsMicrostore: EventsMicrostore,
        eventCodec: EventCodec,
        eventFilter: EventFilter = EventFilter.Any,
        expression: ((UnitOfWork) -> Boolean)? = null,
        emit: ((UnitOfWork) -> List<Event>)? = null
    ): EvaluatePipeline = EvaluatePipeline(
        id = id,
        eventPublisher = eventPublisher,
        eventsMicrostore = eventsMicrostore,
        eventCodec = eventCodec,
        eventFilter = eventFilter,
        expression = expression,
        emit = emit
    )

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
