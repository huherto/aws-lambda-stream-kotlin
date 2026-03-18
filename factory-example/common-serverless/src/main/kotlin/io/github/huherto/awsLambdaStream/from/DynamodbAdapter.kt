package io.github.huherto.awsLambdaStream.from

import com.amazonaws.services.lambda.runtime.events.DynamodbEvent
import io.github.huherto.awsLambdaStream.FaultManager
import io.github.huherto.awsLambdaStream.UnitOfWork
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.asFlow
import kotlinx.coroutines.flow.mapNotNull
import com.amazonaws.services.lambda.runtime.events.models.dynamodb.AttributeValue as EventAV

class DynamodbAdapter {

    private val logger = mu.KotlinLogging.logger {  }

    private val pkFn = "pk"

    private val preferApproximateTimestamp = true

    private val discriminatorFn = "discriminator"

    private val skFn = "sk"

    fun fromDynamoDB(faultManager: FaultManager, dynamodbEvent: DynamodbEvent) : Flow<UnitOfWork> {
        with(faultManager) {
            return dynamodbEvent.records.asFlow()
                .mapNotNull { dynamodbRecord -> UnitOfWork().copy(record = dynamodbRecord) }
                .mapNotFaulty { uow ->
                    val dynamodbRecord = uow.record as DynamodbEvent.DynamodbStreamRecord
                    val event = buildEvent(dynamodbRecord)
                    UnitOfWork().copy(
                        record = dynamodbRecord,
                        event = event
                    )
                }
        }
    }

    internal fun buildEvent(dynamodbRecord: DynamodbEvent.DynamodbStreamRecord): TableEvent {
        val event = TableEvent().apply {
            id = dynamodbRecord.eventID
            timestamp = deriveTimestamp(dynamodbRecord)
            partitionKey = dynamodbRecord.dynamodb.keys[pkFn]?.s
            type = calculateEventType(dynamodbRecord)
            tags = mapOf(
                "region" to dynamodbRecord.awsRegion
            )
            raw = RecordPair(
                new = dynamodbRecord.dynamodb.newImage?.let { RecordImage(it) },
                old = dynamodbRecord.dynamodb.oldImage?.let { RecordImage(it) },
            )
        }
        return event
    }

    private fun deriveTimestamp(dynamodbRecord: DynamodbEvent.DynamodbStreamRecord) : Long? {
        val timestamp =  dynamodbRecord.dynamodb.newImage?.get("timestamp")?.n?.toLong()
        if (preferApproximateTimestamp || timestamp == null) {
            return dynamodbRecord.dynamodb.approximateCreationDateTime?.time?.let {
                it * 1000
            }
        }
        else {
            return timestamp * 1000
        }
    }

    private fun calculateEventType(dynamodbRecord: DynamodbEvent.DynamodbStreamRecord): String? {
        val eventTypePrefix = calculateEventTypePrefix(dynamodbRecord)
        val eventTypeSuffix = calculateEventTypeSuffix(dynamodbRecord)
        return "$eventTypePrefix-$eventTypeSuffix"
    }

    private fun calculateEventTypePrefix(dynamodbRecord: DynamodbEvent.DynamodbStreamRecord): String {
        val image = dynamodbRecord.dynamodb.newImage ?: dynamodbRecord.dynamodb.oldImage
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
            val newImage = dynamodbRecord.dynamodb.newImage
            val oldImage = dynamodbRecord.dynamodb.oldImage

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

data class RecordPair(val new: RecordImage?, val old: RecordImage?)

class RecordImage(val map: Map<String, EventAV?>) : Map<String, EventAV?> by map  {

    fun getTtl(): String? = map["ttl"]?.n

    fun getData(): String? = map["data"]?.s

    fun getEvent(): String? = map["event"]?.s

    fun isDeleted(): Boolean = map.containsKey("deleted") && map["deleted"]?.isBOOL == true

}