package io.kopipes.aws.from

import com.amazonaws.services.lambda.runtime.events.DynamodbEvent
import io.kopipes.aws.AwsGlobalRegistry
import io.kopipes.aws.DynamodbRaw
import io.kopipes.core.EventCodec
import io.kopipes.core.GlobalRegistry.envConfig
import io.kopipes.core.UnitOfWork
import io.kopipes.core.faults.FaultManager
import io.kopipes.core.metrics.PipelineMetrics
import io.kopipes.core.metrics.Timer
import io.kopipes.core.metrics.withMetrics
import io.kopipes.core.sinks.EventsMicrostore
import io.kopipes.core.sinks.withQueryParams
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.asFlow
import kotlinx.coroutines.flow.mapNotNull
import mu.KotlinLogging
import com.amazonaws.services.lambda.runtime.events.models.dynamodb.AttributeValue as EventAV

class DynamodbAdapter(private val faultManager: FaultManager = AwsGlobalRegistry.faultManager()) {

    private val pkFn = "pk"

    private val preferApproximateTimestamp = true

    private val discriminatorFn = "discriminator"

    private val skFn = "sk"

    fun fromDynamoDB(dynamodbEvent: DynamodbEvent): Flow<UnitOfWork> {
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

    private fun deriveTimestamp(dynamodbRecord: DynamodbEvent.DynamodbStreamRecord): Long? {
        val timestamp = dynamodbRecord.dynamodb?.newImage?.get("timestamp")?.n?.toLong()
        if (preferApproximateTimestamp || timestamp == null) {
            return dynamodbRecord.dynamodb?.approximateCreationDateTime?.time
        } else {
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
        val discriminator: EventAV? = image?.get(discriminatorFn) ?: image?.get(skFn)
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

    companion object {
        private val logger = KotlinLogging.logger { }

        @JvmStatic
        fun forCollectedEvents(uow: UnitOfWork): Boolean {
            val record = uow.record
            return when (record) {
                is DynamodbEvent.DynamodbStreamRecord -> {
                    record.eventName == "INSERT"
                            && record.dynamodb?.keys?.get("sk")?.s == "EVENT"
                            && uow.event?.raw is RecordPair
                }
                else -> false
            }
        }

        @JvmStatic
        fun normalize(eventCodec: EventCodec): (UnitOfWork) -> UnitOfWork {
            return { uow ->
                val raw = uow.event?.raw as? RecordPair
                val rawNew = raw?.new ?: RecordImage(mapOf())
                val eventAsString = rawNew.getEvent() ?: "{}"
                val eventAsObject = eventCodec.decode(eventAsString)
                val record = uow.record as? DynamodbEvent.DynamodbStreamRecord
                if (eventAsObject.id == null) {
                    logger.warn { "Event id is null: $eventAsString" }
                }
                uow.copy(
                    meta = mapOf(
                        "sequenceNumber" to record?.dynamodb?.sequenceNumber,
                        "ttl" to "" + rawNew.getTtl().toString(),
                        "data" to "" + rawNew.getData(),
                    ),
                    event = eventAsObject
                )
            }
        }

        @JvmStatic
        fun forEvaluateEvents(uow: UnitOfWork): Boolean {
            val record = uow.record
            return when (record) {
                is DynamodbEvent.DynamodbStreamRecord -> {
                    (record.eventName == "INSERT"
                            && record.dynamodb?.keys?.get("sk")?.s == "EVENT")
                            || record.dynamodb?.newImage?.get("discriminator")?.s == "CORREL"
                }
                else -> false
            }
        }

        @JvmStatic
        @JvmOverloads
        fun normalizeEvaluate(pipelineId: String, eventCodec: EventCodec, index: String? = null): (UnitOfWork) -> UnitOfWork {
            return { uow ->
                val tableChangeEvent = uow.event as? TableChangeEvent
                val raw = (tableChangeEvent?.raw ?: uow.event?.raw) as? RecordPair
                if (tableChangeEvent == null || raw == null) {
                    uow
                } else {
                    val rawNew = raw.new ?: RecordImage(mapOf())
                    val eventAsString = rawNew.getEvent() ?: "{}"
                    val eventAsObject = eventCodec.decode(eventAsString)
                    val correlation = rawNew.getDiscriminator() == "CORREL"
                    val pk = rawNew.getPk()
                    val data = rawNew.getData()
                    val suffix = rawNew.getSuffix()
                    val queryParams = EventsMicrostore.QueryParams(
                        pk = pk,
                        correlation = correlation,
                        data = data,
                        index = index,
                    )

                    val correlationKey = if (correlation) pk else data
                    val partitionKey = correlationKey?.replace(".${suffix}", "")

                    uow.withQueryParams(queryParams).copy(
                        event = eventAsObject,
                        meta = mapOf(
                            "eventId" to "${tableChangeEvent.id}.${pipelineId}",
                            "partitionKey" to partitionKey,
                        )
                    )
                }
            }
        }
    }
}
