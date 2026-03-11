package io.github.huherto.awsLambdaStream.from

import com.amazonaws.services.lambda.runtime.events.DynamodbEvent
import com.amazonaws.services.lambda.runtime.events.models.dynamodb.AttributeValue
import io.github.huherto.awsLambdaStream.UnitOfWork
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.asFlow
import kotlinx.coroutines.flow.map

class DynamodbAdapter {

    private val logger = mu.KotlinLogging.logger {  }

    private val pkFn = "pk"

    private val preferApproximateTimestamp = true

    private val discriminatorFn = "discriminator"

    private val skFn = "sk"

    fun fromDynamoDB(dynamodbEvent: DynamodbEvent) : Flow<UnitOfWork> {
        return dynamodbEvent.records.asFlow()
            .map { dynamodbRecord ->
                val event = TableEvent().apply {
                    id = dynamodbRecord.eventID
                    timestamp = deriveTimestamp(dynamodbRecord)
                    partitionKey = dynamodbRecord.dynamodb.keys[pkFn]?.s
                    type = calculateEventType(dynamodbRecord)
                    tags = mapOf(
                        "region" to dynamodbRecord.awsRegion
                    )
                    raw = mapOf(
                        "new" to dynamodbRecord.dynamodb.newImage,
                        "old" to dynamodbRecord.dynamodb.oldImage
                    )
                }
                UnitOfWork().copy(
                    record = dynamodbRecord,
                    event = event
                )
            }
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

    private fun calculateEventTypePrefix(dynamodbRecord: DynamodbEvent.DynamodbStreamRecord): String? {
        val image = dynamodbRecord.dynamodb.newImage ?: dynamodbRecord.dynamodb.oldImage
        val discriminator : AttributeValue? = image?.get(discriminatorFn) ?: image?.get(skFn)
        return discriminator?.s

    }

    private fun calculateEventTypeSuffix(dynamodbRecord: DynamodbEvent.DynamodbStreamRecord): String? {
        return ""
    }

}