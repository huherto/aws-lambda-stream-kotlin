package org.myorg.payments.adapters.inbound

import com.amazonaws.services.lambda.runtime.events.DynamodbEvent
import io.kopipes.aws.from.RecordImage
import io.kopipes.aws.from.RecordPair
import io.kopipes.core.Event
import io.kopipes.core.EventCodec
import io.kopipes.core.UnitOfWork
import io.kopipes.core.sinks.EventsMicrostore
import io.kopipes.core.sinks.withQueryParams
import org.myorg.payments.PaymentsEvent
import org.myorg.payments.PaymentsEventCodec

/**
 * How the control service's events table looks from its stream: what triggers each pipeline, and how a correlation
 * item becomes the triggering event. Plumbing between DynamoDB, kopipes and the domain; no business rules here.
 */

/** The pipelines that run the payment process's rules. Each id is part of the events it emits: `<payment>.<rule>`. */
object Rules {
    const val START = "start"
    const val NEXT_STEP = "next-step"
    const val DONE = "done"
    const val NOTHING_TO_UNDO = "nothing-to-undo"
    const val COMPENSATE = "compensate"
    const val UNDONE = "undone"
    const val TIMEOUT = "timeout"
}

/** Correlation items of the deadline live under `<payment>.deadline`, apart from the payment's own events. */
const val DEADLINE_SUFFIX = ".deadline"

private const val CORREL = "CORREL"

private fun UnitOfWork.streamRecord(): DynamodbEvent.DynamodbStreamRecord? =
    record as? DynamodbEvent.DynamodbStreamRecord

private fun UnitOfWork.images(): RecordPair? = event?.raw as? RecordPair

/**
 * Rules evaluate only when an event is correlated to its payment, never on the raw collected event.
 * So each fact triggers each rule once, and a rule sees the payment's events through one consistent query.
 */
fun isCorrelationInsert(uow: UnitOfWork): Boolean {
    val record = uow.streamRecord() ?: return false
    val image = uow.images()?.new ?: return false
    return record.eventName == "INSERT" && image.getDiscriminator() == CORREL && image.getSuffix().isNullOrEmpty()
}

/** The deadline item of a payment was deleted by the table's TTL: the deadline has passed. */
fun isDeadlineExpiry(uow: UnitOfWork): Boolean {
    val record = uow.streamRecord() ?: return false
    val image = uow.images()?.old ?: return false
    return record.eventName == "REMOVE" &&
        image.getDiscriminator() == CORREL &&
        image.getSuffix() == DEADLINE_SUFFIX &&
        image.getBool("expire") == true
}

/**
 * Like the evaluate pipeline's default normalizer, but the emitted event's id is `<payment>.<pipeline>`
 * instead of the stream record's id, so replays and redeliveries produce the same ids.
 */
fun correlationNormalizer(
    pipelineId: String,
    codec: EventCodec,
    image: (RecordPair) -> RecordImage?,
    adjust: (event: Event, image: RecordImage) -> Event = { event, _ -> event },
): (UnitOfWork) -> UnitOfWork = { uow ->
    val recordImage = uow.images()?.let(image) ?: error("No record image to normalize")
    val pk = recordImage.getPk() ?: error("Correlation item without pk")
    val suffix = recordImage.getSuffix().orEmpty()
    val paymentId = pk.removeSuffix(suffix)
    val event = adjust(codec.decode(recordImage.getEvent() ?: error("Correlation item without event")), recordImage)

    uow.withQueryParams(
        EventsMicrostore.QueryParams(pk = paymentId, correlation = true, consistentRead = true)
    ).copy(
        event = event,
        meta = (uow.meta ?: emptyMap()) + mapOf(
            "eventId" to "$paymentId.$pipelineId",
            "partitionKey" to paymentId,
            "suffix" to suffix,
        ),
    )
}

/** The deadline is the fact's time: `PAYMENT_EXPIRED` carries the deadline, not the time the TTL sweep ran. */
fun atDeadline(event: Event, image: RecordImage): Event {
    val deadlineMillis = image.getTtl()?.toLongOrNull()?.times(1000) ?: return event
    return event.copyEvent(timestamp = deadlineMillis)
}

/** The payment's history as typed events. The DynamoDB microstore returns them as generic JSON events. */
fun correlatedEvents(uow: UnitOfWork): List<PaymentsEvent> =
    uow.correlated.orEmpty().map { it as? PaymentsEvent ?: PaymentsEventCodec.decode(it.toString()) as PaymentsEvent }
