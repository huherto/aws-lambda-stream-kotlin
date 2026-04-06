package io.github.huherto.awsLambdaStream.flavors

import aws.sdk.kotlin.services.dynamodb.model.PutItemRequest
import com.amazonaws.services.lambda.runtime.events.DynamodbEvent
import io.github.huherto.awsLambdaStream.*
import io.github.huherto.awsLambdaStream.from.RecordImage
import io.github.huherto.awsLambdaStream.from.RecordPair
import io.github.huherto.awsLambdaStream.sinks.EventsMicrostore
import io.github.huherto.awsLambdaStream.utils.nullableBool
import io.github.huherto.awsLambdaStream.utils.nullableN
import io.github.huherto.awsLambdaStream.utils.nullableS
import kotlinx.coroutines.flow.*
import kotlin.reflect.KClass
import aws.sdk.kotlin.services.dynamodb.model.AttributeValue as SdkAV

const val CORREL = "CORREL"

class CorrelatePipeline constructor(
    id: String,
    val onContentType: (UnitOfWork) -> Boolean = { true },
    val onEventClass: List<KClass<out Event>> = listOf(Event::class),
    val correlationKey: ((UnitOfWork) -> String)? = null,
    val correlationKeySuffix: String = "",
    val envConfig: EnvironmentConfig,
    var eventsMicrostore: EventsMicrostore,
    val unmarshall: ((String) -> Event)? = null,
    val expire: Boolean = false,
) : Pipeline(id) {

    internal fun forCollectedEvents(uow: UnitOfWork) : Boolean {
        return when(uow.record) {
            is DynamodbEvent.DynamodbStreamRecord -> {
                uow.record.eventName == "INSERT"
                        && uow.record.dynamodb.keys["sk"]?.s == "EVENT"
                        && uow.event?.raw is RecordPair
            }
            else -> false
        }
    }
    internal fun defaultUnmarshall(eventAsString: String) : Event {
        if (unmarshall != null) return unmarshall(eventAsString)

        val jsonEvent: JsonEvent = try {
            JsonEvent(eventAsString)
        } catch (e: Exception) {
            logger.error {"Failed to parse event: $eventAsString, $e" }
            throw e
        }
        return jsonEvent
    }

    internal fun normalize(uow: UnitOfWork): UnitOfWork {
        val raw = uow.event?.raw as? RecordPair
        val rawNew = raw?.new ?: RecordImage(mapOf())
        val eventAsString = rawNew.getEvent()?: "{}"
        val eventAsObject = defaultUnmarshall(eventAsString)
        val record = uow.record as? DynamodbEvent.DynamodbStreamRecord

        return uow.copy(
            meta = mapOf(
                "sequenceNumber" to record?.dynamodb?.sequenceNumber,
                "ttl" to "" + rawNew.getTtl().toString(),
                "data" to "" + rawNew.getData(),
            ),
            event = eventAsObject
        )
    }

    private fun addCorrelationKey(uow: UnitOfWork) : UnitOfWork {
        require(correlationKey != null) { "correlationKey must be set" }

        // use a suffix when you need the same key for different sets of rules
        val key = correlationKey(uow) + correlationKeySuffix
        return uow.copy(key = key)
    }

    internal fun defaultPutRequest(uow: UnitOfWork) : UnitOfWork {

        val event: Event? = uow.event
        val timeStamp = event?.timestamp
        val awsRegion = envConfig.awsRegion()

        val itemValues = mapOf(
            "pk" to nullableS(uow.key),
            "sk" to nullableS(event?.id),
            "discriminator" to SdkAV.S(CORREL), // ATION
            "timestamp" to SdkAV.N(timeStamp.toString()),
            "awsregion" to SdkAV.S(awsRegion),
            "sequenceNumber" to nullableS(uow.meta?.get("sequenceNumber")),
            "ttl" to nullableN(uow.meta?.get("ttl")),
            "expire" to nullableBool(expire),
            "suffix" to SdkAV.S(correlationKeySuffix),
            "pipelineId" to nullableS(id),
            "event" to nullableS(event?.encoded()),
        )

        val putRequest = PutItemRequest.Companion {
            tableName = envConfig.tableName() ?: "events"
            item = itemValues
        }
        return uow.copy(putRequest = putRequest)
    }

    internal fun Flow<UnitOfWork>.save(): Flow<UnitOfWork> {
        val flow = this.map { uow ->
            val saveOptions = EventsMicrostore.SaveOptions(
                pk = uow.key,
                sk = uow.event?.id,
                discriminator = CORREL,
                timeStamp = uow.event?.timestamp.toString(),
                awsRegion = envConfig.awsRegion(),
                sequenceNumber = uow.meta?.get("sequenceNumber"),
                ttl = uow.meta?.get("ttl")?.toLongOrNull(),
                expire = expire,
                suffix = correlationKeySuffix,
                pipelineId = id,
            )
            uow.copy(saveOptions = saveOptions)
        }
        return eventsMicrostore.save(flow)
    }

    override fun connect(fm: FaultManager, fromFlow: Flow<UnitOfWork>) : Flow<UnitOfWork> {
        logger.info { "CorrelatePipeline.connect: id=$id" }
        with(fm) {
            val flow = fromFlow
                .filterNotNull()
                .filter{ uow -> faulty(uow){ forCollectedEvents(uow) } == true }
                .mapNotFaulty{  uow -> normalize(uow) }
                .filterEventTypes(this, *onEventClass.toTypedArray())
                .onEach { uow -> printStartPipeline(uow) }
                .filter { uow -> faulty(uow) { onContentType(uow) } == true }
                .mapNotFaulty { uow -> addCorrelationKey(uow)}
                .save()
                .onEach { uow -> printEndPipeline(uow) }
            return flow
        }
    }
}