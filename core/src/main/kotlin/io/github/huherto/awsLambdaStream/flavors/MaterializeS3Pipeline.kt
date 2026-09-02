package io.github.huherto.awsLambdaStream.flavors

import aws.sdk.kotlin.services.s3.model.DeleteObjectRequest
import aws.sdk.kotlin.services.s3.model.GetObjectRequest
import aws.sdk.kotlin.services.s3.model.PutObjectRequest
import io.github.huherto.awsLambdaStream.PipelineBuilder
import io.github.huherto.awsLambdaStream.UnitOfWork
import io.github.huherto.awsLambdaStream.connectors.S3Connector
import io.github.huherto.awsLambdaStream.extensions.copyS3
import io.github.huherto.awsLambdaStream.faults.FaultManager
import io.github.huherto.awsLambdaStream.filters.EventFilter
import io.github.huherto.awsLambdaStream.filters.filterEvents
import io.github.huherto.awsLambdaStream.queries.S3Query
import io.github.huherto.awsLambdaStream.sinks.S3Sink
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.onEach

/** A pipeline that materializes events to S3. */
class MaterializeS3Pipeline(
    pipelineId: String,
    private val eventFilter: EventFilter,
    private val onContentType: (UnitOfWork) -> Boolean,
    private val splitObject: (Flow<UnitOfWork>) -> Flow<UnitOfWork>,
    private val s3ConnectorOptions: S3Connector.Options,
    private val toGetRequest: ((UnitOfWork) -> GetObjectRequest?)?,
    private val toPutRequest: ((UnitOfWork) -> PutObjectRequest?)?,
    private val toDeleteRequest: ((UnitOfWork) -> DeleteObjectRequest?)?,
) : Pipeline(pipelineId) {

    private val s3Query: S3Query = S3Query(s3ConnectorOptions)
    private val s3Sink: S3Sink = S3Sink(s3ConnectorOptions)

    override fun connect(
        fm: FaultManager,
        fromFlow: Flow<UnitOfWork>,
    ): Flow<UnitOfWork> {
        logger.info { "MaterializeS3Pipeline.connect: id=$id" }

        with(fm) {
            return fromFlow
                .filterEvents(fm, eventFilter)
                .onEach { uow -> printStartPipeline(uow) }
                .filterNotFaulty { uow -> onContentType(uow) }
                .let { flow -> splitObject(flow) }
                .mapNotFaulty { uow ->
                    if (toGetRequest != null) {
                        uow.copyS3 {
                            copy(
                                getRequest = toGetRequest(uow),
                            )
                        }
                    }
                    else
                        uow
                }
                .let { flow -> s3Query.getObject(fm, flow) }
                .mapNotFaulty { uow ->
                    if (toPutRequest != null) {
                        uow.copyS3 {
                            copy(
                                putRequest = toPutRequest(uow),
                            )
                        }
                    }
                    else
                        uow
                }
                .let { flow -> s3Sink.putObject(flow) }
                .mapNotFaulty { uow ->
                    if (toDeleteRequest != null) {
                        uow.copyS3 {
                            copy(
                                deleteRequest = toDeleteRequest(uow),
                            )
                        }
                    }
                    else
                        uow
                }
                .let { flow -> s3Sink.deleteObject(flow) }
                .onEach { uow -> printEndPipeline(uow) }
        }
    }

    companion object {
        @JvmStatic
        fun builder() = Builder()
    }

    class Builder : PipelineBuilder<MaterializeS3Pipeline, Builder>() {
        private var eventFilter: EventFilter = EventFilter.Any
        private var onContentType: (UnitOfWork) -> Boolean = { true }
        private var splitObject: (Flow<UnitOfWork>) -> Flow<UnitOfWork> = { it }
        private var s3ConnectorOptions: S3Connector.Options = S3Connector.Options()
        private var toGetRequest: ((UnitOfWork) -> GetObjectRequest?)? = null
        private var toPutRequest: ((UnitOfWork) -> PutObjectRequest?)? = null
        private var toDeleteRequest: ((UnitOfWork) -> DeleteObjectRequest?)? = null

        fun eventFilter(eventFilter: EventFilter) = apply { this.eventFilter = eventFilter }
        fun onContentType(onContentType: (UnitOfWork) -> Boolean) = apply { this.onContentType = onContentType }
        fun onContentTypeJava(onContentType: java.util.function.Predicate<UnitOfWork>) = apply { this.onContentType = { uow -> onContentType.test(uow) } }
        fun splitObject(splitObject: (Flow<UnitOfWork>) -> Flow<UnitOfWork>) = apply { this.splitObject = splitObject }
        fun splitObjectJava(splitObject: java.util.function.Function<Flow<UnitOfWork>, Flow<UnitOfWork>>) = apply { this.splitObject = { flow -> splitObject.apply(flow) } }
        fun s3ConnectorOptions(options: S3Connector.Options) = apply { this.s3ConnectorOptions = options }
        fun toGetRequest(toGetRequest: (UnitOfWork) -> GetObjectRequest?) = apply { this.toGetRequest = toGetRequest }
        fun toGetRequestJava(toGetRequest: java.util.function.Function<UnitOfWork, GetObjectRequest?>) = apply { this.toGetRequest = { uow -> toGetRequest.apply(uow) } }
        fun toPutRequest(toPutRequest: (UnitOfWork) -> PutObjectRequest?) = apply { this.toPutRequest = toPutRequest }
        fun toPutRequestJava(toPutRequest: java.util.function.Function<UnitOfWork, PutObjectRequest?>) = apply { this.toPutRequest = { uow -> toPutRequest.apply(uow) } }
        fun toDeleteRequest(toDeleteRequest: (UnitOfWork) -> DeleteObjectRequest?) = apply { this.toDeleteRequest = toDeleteRequest }
        fun toDeleteRequestJava(toDeleteRequest: java.util.function.Function<UnitOfWork, DeleteObjectRequest?>) = apply { this.toDeleteRequest = { uow -> toDeleteRequest.apply(uow) } }

        override fun build(): MaterializeS3Pipeline {
            return MaterializeS3Pipeline(
                pipelineId = id ?: throw IllegalArgumentException("id is required"),
                eventFilter = eventFilter,
                onContentType = onContentType,
                splitObject = splitObject,
                s3ConnectorOptions = s3ConnectorOptions,
                toGetRequest = toGetRequest,
                toPutRequest = toPutRequest,
                toDeleteRequest = toDeleteRequest
            )
        }
    }
}