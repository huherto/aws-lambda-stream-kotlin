package io.github.huherto.awsLambdaStream.flavors

import com.amazonaws.services.lambda.runtime.events.DynamodbEvent
import io.github.huherto.awsLambdaStream.*
import io.github.huherto.awsLambdaStream.extensions.withSaveOptions
import io.github.huherto.awsLambdaStream.faults.FaultManager
import io.github.huherto.awsLambdaStream.filters.EventFilter
import io.github.huherto.awsLambdaStream.filters.filterEvents
import io.github.huherto.awsLambdaStream.from.RecordImage
import io.github.huherto.awsLambdaStream.from.RecordPair
import io.github.huherto.awsLambdaStream.sinks.EventsMicrostore
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.flow.onEach

const val CORREL = "CORREL"

/** Pipeline flavor that correlates previously collected events and persists correlation records. */
class CorrelatePipeline(
    id: String,
    val onContentType: (UnitOfWork) -> Boolean,
    val eventFilter: EventFilter,
    val correlationKeySupplier: ((UnitOfWork) -> String),
    val correlationKeySuffix: String,
    var eventsMicrostore: EventsMicrostore,
    val eventCodec: EventCodec,
    val expire: Boolean,
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

    internal fun decodeEvent(eventAsString: String) : Event {
        return eventCodec.decode(eventAsString)
    }

    internal fun normalize(uow: UnitOfWork): UnitOfWork {
        val raw = uow.event?.raw as? RecordPair
        val rawNew = raw?.new ?: RecordImage(mapOf())
        val eventAsString = rawNew.getEvent()?: "{}"
        val eventAsObject = decodeEvent(eventAsString)
        val record = uow.record as? DynamodbEvent.DynamodbStreamRecord
        if (eventAsObject.id == null) {
            logger.warn { "Event id is null: $eventAsString" }
        }
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
        val correlationKey = correlationKeySupplier(uow)
        // use a suffix when you need the same key for different sets of rules
        val key = correlationKeySupplier(uow) + correlationKeySuffix
        return uow.copy(key = key)
    }

    internal fun Flow<UnitOfWork>.save(): Flow<UnitOfWork> {
        val flow = this.map { uow ->
            val key = uow.key ?: return@map uow
            val event: Event = uow.event ?: return@map uow
            val eventId = event.id ?: return@map uow
            val saveOptions = EventsMicrostore.SaveOptions(
                pk = key,
                sk = eventId,
                discriminator = CORREL,
                timeStamp = uow.event.timestamp,
                awsRegion = envConfig().awsRegion(),
                sequenceNumber = uow.meta?.get("sequenceNumber"),
                ttl = uow.meta?.get("ttl")?.toLongOrNull(),
                expire = expire,
                suffix = correlationKeySuffix,
                pipelineId = id,
            )
            uow.withSaveOptions(saveOptions)
        }
        // Save already has a fault manager.
        return eventsMicrostore.save(flow)
    }

    override fun connect(fm: FaultManager, fromFlow: Flow<UnitOfWork>) : Flow<UnitOfWork> {
        logger.info { "CorrelatePipeline.connect: id=$id" }
        with(fm) {
            val flow = fromFlow
                .filterNotFaulty{ uow ->  forCollectedEvents(uow) }
                .mapNotFaulty{  uow -> normalize(uow) }
                .filterEvents(this, eventFilter)
                .onEach { uow -> printStartPipeline(uow) }
                .filterNotFaulty { uow -> onContentType(uow) }
                .mapNotFaulty { uow -> addCorrelationKey(uow)}
                .save()
                .onEach { uow -> printEndPipeline(uow) }
            return flow
        }
    }

    companion object {
        @JvmStatic
        fun builder() = Builder()
    }

    class Builder : PipelineBuilder<CorrelatePipeline, Builder>() {
        private var onContentType: (UnitOfWork) -> Boolean = { true }
        private var eventFilter: EventFilter = EventFilter.Any
        private var correlationKeySupplier: ((UnitOfWork) -> String)? = null
        private var correlationKeySuffix: String = ""
        private var eventsMicrostore: EventsMicrostore? = null
        private var eventCodec: EventCodec? = null
        private var expire: Boolean = false

        fun onContentType(onContentType: (UnitOfWork) -> Boolean) = apply { this.onContentType = onContentType }
        fun onContentTypeJava(onContentType: java.util.function.Predicate<UnitOfWork>) = apply { this.onContentType = { uow -> onContentType.test(uow) } }
        fun eventFilter(eventFilter: EventFilter) = apply { this.eventFilter = eventFilter }
        fun correlationKeySupplier(correlationKeySupplier: (UnitOfWork) -> String) = apply { this.correlationKeySupplier = correlationKeySupplier }
        fun correlationKeySupplierJava(correlationKeySupplier: java.util.function.Function<UnitOfWork, String>) = apply { this.correlationKeySupplier = { uow -> correlationKeySupplier.apply(uow) } }
        fun correlationKeySuffix(correlationKeySuffix: String) = apply { this.correlationKeySuffix = correlationKeySuffix }
        fun eventsMicrostore(eventsMicrostore: EventsMicrostore) = apply { this.eventsMicrostore = eventsMicrostore }
        fun eventCodec(eventCodec: EventCodec) = apply { this.eventCodec = eventCodec }
        fun expire(expire: Boolean) = apply { this.expire = expire }

        override fun build(): CorrelatePipeline {
            return CorrelatePipeline(
                id = id ?: throw IllegalArgumentException("id is required"),
                onContentType = onContentType,
                eventFilter = eventFilter,
                correlationKeySupplier = correlationKeySupplier ?: throw IllegalArgumentException("correlationKeySupplier is required"),
                correlationKeySuffix = correlationKeySuffix,
                eventsMicrostore = eventsMicrostore ?: throw IllegalArgumentException("eventsMicrostore is required"),
                eventCodec = eventCodec ?: throw IllegalArgumentException("eventCodec is required"),
                expire = expire
            )
        }
    }
}