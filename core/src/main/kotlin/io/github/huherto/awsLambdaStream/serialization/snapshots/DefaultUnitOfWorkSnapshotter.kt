package io.github.huherto.awsLambdaStream.serialization.snapshots

import io.github.huherto.awsLambdaStream.RawRecord
import io.github.huherto.awsLambdaStream.UnitOfWork
import io.github.huherto.awsLambdaStream.extensions.*
import io.github.huherto.awsLambdaStream.serialization.Snapshottable
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonObject

class DefaultUnitOfWorkSnapshotter(
    private val recordSnapshotters: List<RecordSnapshotter> = listOf(
        KinesisRecordSnapshotter(),
        DynamoDbRecordSnapshotter(),
        SqsRecordSnapshotter()
    )
) : UnitOfWorkSnapshotter {
    override fun snapshot(uow: UnitOfWork): UnitOfWorkSnapshot {
        return UnitOfWorkSnapshot(
            pipeline = uow.pipeline?.let { PipelineSnapshot(id = it.id) },
            record = uow.record?.let { record ->
                recordSnapshotters.find { it.supports(record) }?.snapshot(record)
                    ?: RecordSnapshot(kind = "unknown", payload = JsonObject(emptyMap()))
            },
            event = uow.event?.let { event ->
                EventSnapshot(
                    id = event.id,
                    type = event.eventType(),
                    timestamp = event.timestamp,
                    partitionKey = event.partitionKey,
                    tags = event.tags,
                    raw = event.raw?.let { Json.encodeToString(RawRecord.serializer(), it) },
                    eem = event.eem?.toString(),
                    triggers = event.triggers,
                    encoded = event.toString()
                )
            },
            key = uow.key,
            sequenceNumber = uow.sequenceNumber,
            shardId = uow.shardId,
            timestamp = uow.timestamp,
            meta = uow.meta,
            triggers = uow.triggers?.map { event ->
                EventSnapshot(
                    id = event.id,
                    type = event.eventType(),
                    timestamp = event.timestamp,
                    partitionKey = event.partitionKey,
                    tags = event.tags
                )
            },
            correlated = uow.correlated?.map { event ->
                EventSnapshot(
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
                    key = s3.getRequest?.key ?: s3.putRequest?.key ?: s3.headRequest?.key,
                    getRequest = s3.getRequest?.toString(),
                    getResponse = s3.getResponse?.toString(),
                    deleteRequest = s3.deleteRequest?.toString(),
                    deleteResponse = s3.deleteResponse?.toString(),
                    copyRequest = s3.copyRequest?.toString(),
                    copyResponse = s3.copyResponse?.toString(),
                    getResponseText = s3.getResponseText,
                    getResponseBytes = s3.getResponseBytes,
                    putRequest = s3.putRequest?.toString(),
                    putResponse = s3.putResponse?.toString(),
                    listRequest = s3.listRequest?.toString(),
                    listResponse = s3.listResponse?.toString(),
                    listResponseObject = s3.listResponseObject?.toString(),
                    headRequest = s3.headRequest?.toString(),
                    headResponse = s3.headResponse?.toString()
                )
            },
            extensions = uow.extensions?.entries?.associate { (k, v) ->
                val name = k.simpleName ?: "unknown"
                val snapshot = if (v is Snapshottable) v.toSnapshot() else v
                name to snapshot?.toString()
            }?.takeUnless { it.isEmpty() },
            batchGetRequest = uow.batchGetRequest?.toString(),
            batchGetResponse = uow.batchGetResponse?.toString(),
            publishRequest = uow.publishRequest?.toString(),
            publishRequestEntry = uow.publishRequestEntry?.toString(),
            publishResponse = uow.publishResponse?.toString(),
            putRequest = uow.putRequest?.toString(),
            putResponse = uow.putResponse?.toString(),
            queryParams = uow.queryParams?.toString(),
            queryRequest = uow.queryRequest?.toString(),
            queryResponse = uow.queryResponse?.toString(),
            saveOptions = uow.saveOptions?.toString(),
            scanRequest = uow.scanRequest?.toString(),
            updateRequest = uow.updateRequest?.toString(),
            updateResponse = uow.updateResponse?.toString()
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
