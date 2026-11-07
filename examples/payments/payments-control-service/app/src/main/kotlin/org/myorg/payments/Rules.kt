package org.myorg.payments

import com.amazonaws.services.lambda.runtime.events.DynamodbEvent
import io.kopipes.aws.from.RecordImage
import io.kopipes.aws.from.RecordPair
import io.kopipes.core.Event
import io.kopipes.core.EventCodec
import io.kopipes.core.UnitOfWork
import io.kopipes.core.sinks.EventsMicrostore
import io.kopipes.core.sinks.withQueryParams

/**
 * The control service's rules, one pipeline each. The pipeline id is part of every emitted event id
 * (`<payment>.<pipeline>`), so a rule that fires again emits the same event, with the same id.
 */
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

// ---------------------------------------------------------------------------------------------------------------
// Which table changes each pipeline reacts to.
// ---------------------------------------------------------------------------------------------------------------

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

// ---------------------------------------------------------------------------------------------------------------
// Normalizers: turn a correlation item into the triggering event, with a deterministic id.
// ---------------------------------------------------------------------------------------------------------------

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

// ---------------------------------------------------------------------------------------------------------------
// The rules. Each takes the unit of work after normalization (and, for the complex ones, the correlated events).
// ---------------------------------------------------------------------------------------------------------------

private fun UnitOfWork.paymentEvent(): PaymentEvent =
    event as? PaymentEvent ?: error("Expected a payment event, got ${event?.eventType()}")

/** The payment's events as typed events. The DynamoDB microstore returns them as generic JSON events. */
fun correlatedEvents(uow: UnitOfWork): List<PaymentsEvent> =
    uow.correlated.orEmpty().map { it as? PaymentsEvent ?: PaymentsEventCodec.decode(it.toString()) as PaymentsEvent }

/** Start: `PAYMENT_SUBMITTED` → `WITHDRAWAL_REQUESTED`. */
fun start(uow: UnitOfWork): List<Event> = listOf(WithdrawalRequested(payment = uow.paymentEvent().payment))

/** Next step: `FUNDS_WITHDRAWN` → `DEPOSIT_REQUESTED`. */
fun nextStep(uow: UnitOfWork): List<Event> = listOf(DepositRequested(payment = uow.paymentEvent().payment))

/** Done: `FUNDS_DEPOSITED` → `PAYMENT_COMPLETED`, also after a timeout (late deposit). */
fun done(uow: UnitOfWork): List<Event> = listOf(PaymentCompleted(payment = uow.paymentEvent().payment))

/** Nothing to undo: `WITHDRAWAL_FAILED` → `PAYMENT_FAILED`, with the reason copied. */
fun nothingToUndo(uow: UnitOfWork): List<Event> {
    val failed = uow.event as? WithdrawalFailed ?: error("Expected WITHDRAWAL_FAILED, got ${uow.event?.eventType()}")
    return listOf(PaymentFailed(payment = failed.payment, reason = failed.reason))
}

/** Compensate: `DEPOSIT_FAILED` → `REFUND_REQUESTED`, also after a timeout (late failure). */
fun compensate(uow: UnitOfWork): List<Event> = listOf(RefundRequested(payment = uow.paymentEvent().payment))

/**
 * Undone: `FUNDS_REFUNDED` → `PAYMENT_FAILED`, with the reason of the deposit failure that caused the refund.
 * Without that failure the rule can't decide, so it throws: the payment's unit of work becomes a fault.
 */
fun undone(uow: UnitOfWork): List<Event> {
    val payment = uow.paymentEvent().payment
    val depositFailed = correlatedEvents(uow).filterIsInstance<DepositFailed>().firstOrNull()
        ?: error("FUNDS_REFUNDED for ${payment.id} without a DEPOSIT_FAILED: the refund has no cause")
    return listOf(PaymentFailed(payment = payment, reason = depositFailed.reason))
}

/** Timeout: by the deadline neither `FUNDS_DEPOSITED` nor `DEPOSIT_FAILED` was heard. */
fun depositIsLate(uow: UnitOfWork): Boolean {
    val types = correlatedEvents(uow).map { it.eventType() }.toSet()
    return PaymentsEvent.FUNDS_DEPOSITED !in types && PaymentsEvent.DEPOSIT_FAILED !in types
}

/** Timeout: `DEPOSIT_REQUESTED` plus the deadline → `PAYMENT_EXPIRED`. Not an outcome: the other rules keep running. */
fun timeout(uow: UnitOfWork): List<Event> = listOf(PaymentExpired(payment = uow.paymentEvent().payment))
