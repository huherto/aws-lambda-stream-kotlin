package io.github.huherto.awsLambdaStream.flavors

import aws.sdk.kotlin.services.s3.model.DeleteObjectRequest
import aws.sdk.kotlin.services.s3.model.GetObjectRequest
import aws.sdk.kotlin.services.s3.model.PutObjectRequest
import io.github.huherto.awsLambdaStream.FaultManager
import io.github.huherto.awsLambdaStream.UnitOfWork
import io.github.huherto.awsLambdaStream.connectors.S3Connector
import io.github.huherto.awsLambdaStream.copyS3
import io.github.huherto.awsLambdaStream.filters.EventFilter
import io.github.huherto.awsLambdaStream.filters.filterEvents
import io.github.huherto.awsLambdaStream.queries.S3Query
import io.github.huherto.awsLambdaStream.sinks.S3Sink
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.onEach

class MaterializeS3Pipeline(
    pipelineId: String,
    private val eventFilter: EventFilter = EventFilter.Any,
    private val onContentType: (UnitOfWork) -> Boolean = { true },
    private val splitObject: (Flow<UnitOfWork>) -> Flow<UnitOfWork> = { it },
    private val s3Connector: S3Connector,
    private val toGetRequest: (UnitOfWork, MaterializeS3Pipeline) -> GetObjectRequest? = { _, _ -> null },
    private val toPutRequest: (UnitOfWork, MaterializeS3Pipeline) -> PutObjectRequest? = { _, _ -> null },
    private val toDeleteRequest: (UnitOfWork, MaterializeS3Pipeline) -> DeleteObjectRequest? = { _, _ -> null },
) : Pipeline(pipelineId) {

    private val s3Query: S3Query = S3Query(s3Connector)
    private val s3Sink: S3Sink = S3Sink(s3Connector)

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
                    uow.copyS3 {
                        copy(
                            getRequest = toGetRequest(uow, this@MaterializeS3Pipeline),
                        )
                    }
                }
                .let { flow -> s3Query.getObject(flow) }
                .mapNotFaulty { uow ->
                    uow.copyS3 {
                        copy(
                            putRequest = toPutRequest(uow, this@MaterializeS3Pipeline),
                        )
                    }
                }
                .let { flow -> s3Sink.putObject(flow) }
                .mapNotFaulty { uow ->
                    uow.copyS3 {
                        copy(
                            deleteRequest = toDeleteRequest(uow, this@MaterializeS3Pipeline),
                        )
                    }
                }
                .let { flow -> s3Sink.deleteObject(flow) }
                .onEach { uow -> printEndPipeline(uow) }
        }
    }
}