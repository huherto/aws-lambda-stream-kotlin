package io.github.huherto.awsLambdaStream.faults

import io.github.huherto.awsLambdaStream.UnitOfWork
import io.github.huherto.awsLambdaStream.extensions.*
import kotlinx.serialization.json.JsonObject

class DefaultUnitOfWorkSnapshotter(
    private val recordSnapshotters: List<RecordSnapshotter> = listOf(
        KinesisRecordSnapshotter(),
        DynamoDbRecordSnapshotter()
    )
) : UnitOfWorkSnapshotter {
    override fun snapshot(uow: UnitOfWork): UnitOfWorkSnapshot {
        return UnitOfWorkSnapshot(
            pipeline = uow.pipeline?.let { PipelineSnapshot(id = it.id) },
            record = uow.record?.let { record ->
                recordSnapshotters.find { it.supports(record) }?.snapshot(record)
                    ?: ReplayRecordSnapshot(kind = "unknown", payload = JsonObject(emptyMap()))
            },
            event = uow.event?.let { event ->
                EventSummarySnapshot(
                    id = event.id,
                    type = event.eventType(),
                    timestamp = event.timestamp,
                    partitionKey = event.partitionKey,
                    tags = event.tags
                )
            },
            key = uow.key,
            sequenceNumber = uow.sequenceNumber,
            shardId = uow.shardId,
            timestamp = uow.timestamp,
            meta = uow.meta,
            triggers = uow.triggers?.map { event ->
                EventSummarySnapshot(
                    id = event.id,
                    type = event.eventType(),
                    timestamp = event.timestamp,
                    partitionKey = event.partitionKey,
                    tags = event.tags
                )
            },
            correlated = uow.correlated?.map { event ->
                EventSummarySnapshot(
                    id = event.id,
                    type = event.eventType(),
                    timestamp = event.timestamp,
                    partitionKey = event.partitionKey,
                    tags = event.tags
                )
            },
            batch = uow.batch?.map { snapshot(it) },
            aws = captureAwsOperations(uow),
            s3 = uow.s3?.let { s3 ->
                S3Snapshot(
                    bucket = s3.getRequest?.bucket ?: s3.putRequest?.bucket ?: s3.listRequest?.bucket ?: s3.headRequest?.bucket,
                    key = s3.getRequest?.key ?: s3.putRequest?.key ?: s3.headRequest?.key
                )
            }
        )
    }

    private fun captureAwsOperations(uow: UnitOfWork): List<AwsOperationSnapshot>? {
        val ops = mutableListOf<AwsOperationSnapshot>()

        uow.publishResponse?.let {
            ops.add(AwsOperationSnapshot(service = "EventBridge", operation = "PutEvents"))
        }
        uow.putResponse?.let {
            ops.add(AwsOperationSnapshot(service = "DynamoDB", operation = "PutItem"))
        }
        uow.updateResponse?.let {
            ops.add(AwsOperationSnapshot(service = "DynamoDB", operation = "UpdateItem"))
        }
        uow.queryResponse?.let {
            ops.add(AwsOperationSnapshot(service = "DynamoDB", operation = "Query"))
        }
        uow.scanRequest?.let {
            ops.add(AwsOperationSnapshot(service = "DynamoDB", operation = "Scan"))
        }
        uow.batchGetResponse?.let {
            ops.add(AwsOperationSnapshot(service = "DynamoDB", operation = "BatchGetItem"))
        }

        return if (ops.isEmpty()) null else ops
    }
}
