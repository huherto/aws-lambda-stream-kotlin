package io.github.huherto.awsLambdaStream.from

import com.amazonaws.services.lambda.runtime.events.DynamodbEvent
import io.github.huherto.awsLambdaStream.DynamodbRaw
import io.github.huherto.awsLambdaStream.FaultManager
import io.github.huherto.awsLambdaStream.GlobalRegistry
import io.github.huherto.awsLambdaStream.GlobalRegistry.envConfig
import io.github.huherto.awsLambdaStream.UnitOfWork
import io.github.huherto.awsLambdaStream.metrics.PipelineMetrics
import io.github.huherto.awsLambdaStream.metrics.Timer
import io.github.huherto.awsLambdaStream.metrics.withMetrics
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.asFlow
import kotlinx.coroutines.flow.mapNotNull
import com.amazonaws.services.lambda.runtime.events.models.dynamodb.AttributeValue as EventAV

class DynamodbAdapter (private val faultManager: FaultManager = GlobalRegistry.faultManager()) {

    private val pkFn = "pk"

    private val preferApproximateTimestamp = true

    private val discriminatorFn = "discriminator"

    private val skFn = "sk"

    fun fromDynamoDB(dynamodbEvent: DynamodbEvent) : Flow<UnitOfWork> {
        with(faultManager) {
            val batchUtilization = dynamodbEvent.records.size.toDouble() / (envConfig().batchSize() ?: 100)
            return dynamodbEvent.records.asFlow()
                .mapNotNull { dynamodbRecord -> UnitOfWork().copy(record = dynamodbRecord) }
                .mapNotFaulty { uow ->
                    val dynamodbRecord = uow.record as DynamodbEvent.DynamodbStreamRecord
                    val event = buildEvent(dynamodbRecord)
                    val timestamp = event.timestamp ?: System.currentTimeMillis()
                    UnitOfWork().copy(
                        record = dynamodbRecord,
                        event = event,
                        sequenceNumber = dynamodbRecord.dynamodb?.sequenceNumber,
                    ).withMetrics(
                        PipelineMetrics(
                            timer = Timer(
                                start = timestamp,
                                last = timestamp
                            )
                        ).gauge("stream.batch.utilization", batchUtilization)
                    )
                }
        }
    }

    internal fun buildEvent(dynamodbRecord: DynamodbEvent.DynamodbStreamRecord): TableChangeEvent {

        val event = TableChangeEvent(
            id = dynamodbRecord.eventID,
            timestamp = deriveTimestamp(dynamodbRecord),
            partitionKey = dynamodbRecord.dynamodb?.keys?.get(pkFn)?.s,
            type = calculateEventType(dynamodbRecord),
            tags = mapOf(
                "region" to dynamodbRecord.awsRegion
            ),
            raw = DynamodbRaw(dynamodbRecord)
        )
        return event
    }

    private fun deriveTimestamp(dynamodbRecord: DynamodbEvent.DynamodbStreamRecord) : Long? {
        val timestamp =  dynamodbRecord.dynamodb?.newImage?.get("timestamp")?.n?.toLong()
        if (preferApproximateTimestamp || timestamp == null) {
            return dynamodbRecord.dynamodb?.approximateCreationDateTime?.time
        }
        else {
            return timestamp * 1000
        }
    }

    private fun calculateEventType(dynamodbRecord: DynamodbEvent.DynamodbStreamRecord): String {
        val eventTypePrefix = calculateEventTypePrefix(dynamodbRecord)
        val eventTypeSuffix = calculateEventTypeSuffix(dynamodbRecord)
        return "$eventTypePrefix-$eventTypeSuffix"
    }

    private fun calculateEventTypePrefix(dynamodbRecord: DynamodbEvent.DynamodbStreamRecord): String {
        val image = dynamodbRecord.dynamodb?.newImage ?: dynamodbRecord.dynamodb?.oldImage
        val discriminator : EventAV? = image?.get(discriminatorFn) ?: image?.get(skFn)
        return discriminator?.s ?: ""
    }

    internal fun calculateEventTypeSuffix(dynamodbRecord: DynamodbEvent.DynamodbStreamRecord): String {
        val eventNameMap = mapOf(
            "INSERT" to "created",
            "MODIFY" to "updated",
            "REMOVE" to "deleted"
        )

        val suffix = eventNameMap[dynamodbRecord.eventName] ?: ""

        if (suffix != "deleted") {
            val newImage = dynamodbRecord.dynamodb?.newImage
            val oldImage = dynamodbRecord.dynamodb?.oldImage

            if ((newImage?.containsKey("deleted") == true) || (oldImage?.containsKey("deleted") == true)) {
                if (newImage?.get("deleted")?.bool == true) {
                    return "deleted"
                } else if (oldImage?.get("deleted")?.bool == true) {
                    return "undeleted"
                }
            }
        }

        return suffix
    }


}