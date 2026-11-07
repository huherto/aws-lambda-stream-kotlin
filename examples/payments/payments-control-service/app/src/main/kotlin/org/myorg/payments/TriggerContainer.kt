package org.myorg.payments

import io.kopipes.aws.AwsGlobalRegistry
import io.kopipes.aws.from.DynamodbAdapter
import io.kopipes.aws.sinks.DynamoDbEventsMicrostore
import io.kopipes.core.Event
import io.kopipes.core.EventCodec
import io.kopipes.core.PipelineAssembler
import io.kopipes.core.UnitOfWork
import io.kopipes.core.filters.EventFilters
import io.kopipes.core.flavors.CorrelatePipeline
import io.kopipes.core.flavors.EvaluatePipeline
import io.kopipes.core.sinks.EventPublisher
import io.kopipes.core.sinks.EventsMicrostore

/**
 * Reacts to the control service's events table: correlates each collected event to its payment,
 * then evaluates the payment's rules and publishes what they emit.
 *
 * | Rule            | When it has heard…                                  | It emits               |
 * |-----------------|-----------------------------------------------------|------------------------|
 * | start           | PAYMENT_SUBMITTED                                   | WITHDRAWAL_REQUESTED   |
 * | next-step       | FUNDS_WITHDRAWN                                     | DEPOSIT_REQUESTED      |
 * | done            | FUNDS_DEPOSITED                                     | PAYMENT_COMPLETED      |
 * | nothing-to-undo | WITHDRAWAL_FAILED                                   | PAYMENT_FAILED         |
 * | compensate      | DEPOSIT_FAILED                                      | REFUND_REQUESTED       |
 * | undone          | FUNDS_REFUNDED (+ the DEPOSIT_FAILED behind it)     | PAYMENT_FAILED         |
 * | timeout         | DEPOSIT_REQUESTED, and by the deadline no outcome   | PAYMENT_EXPIRED        |
 */
class TriggerContainer(
    val eventsMicrostore: EventsMicrostore,
    val eventPublisher: EventPublisher,
    /** How long a deposit may take before PAYMENT_EXPIRED. 2 days in production, minutes in demos. */
    val deadlineSeconds: Long = DEFAULT_DEADLINE_SECONDS,
    val codec: EventCodec = PaymentsEventCodec,
) {
    companion object {
        const val CORRELATE = "correlate"
        const val DEADLINE = "deadline"
        const val DEFAULT_DEADLINE_SECONDS = 2 * 24 * 60 * 60L

        fun build(): TriggerContainer = TriggerContainer(
            eventsMicrostore = DynamoDbEventsMicrostore(),
            eventPublisher = AwsGlobalRegistry.eventPublisher(),
            deadlineSeconds = System.getenv("DEADLINE_SECONDS")?.toLongOrNull() ?: DEFAULT_DEADLINE_SECONDS,
        )
    }

    private fun paymentId(uow: UnitOfWork): String =
        (uow.event as? PaymentEvent)?.payment?.id ?: error("Not a payment event: ${uow.event?.eventType()}")

    /** Every collected payment event is correlated to its payment: `pk = <payment>`, `sk = <event id>`. */
    val correlatePipeline: CorrelatePipeline by lazy {
        CorrelatePipeline.builder()
            .id(CORRELATE)
            .eventsMicrostore(eventsMicrostore)
            .eventCodec(codec)
            .isCollectedEvent(DynamodbAdapter::forCollectedEvents)
            .normalizer(DynamodbAdapter.normalize(codec))
            .eventFilter(EventFilters.classes(PaymentEvent::class))
            .correlationKey(::paymentId)
            .build()
    }

    /**
     * A `DEPOSIT_REQUESTED` starts the clock: a correlation item under `<payment>.deadline` whose TTL is the
     * deadline. When the table's TTL deletes it, the [timeout] rule runs.
     */
    val deadlinePipeline: CorrelatePipeline by lazy {
        val normalize = DynamodbAdapter.normalize(codec)
        CorrelatePipeline.builder()
            .id(DEADLINE)
            .eventsMicrostore(eventsMicrostore)
            .eventCodec(codec)
            .isCollectedEvent(DynamodbAdapter::forCollectedEvents)
            .normalizer { uow -> withDeadline(normalize(uow)) }
            .eventFilter(EventFilters.name(PaymentsEvent.DEPOSIT_REQUESTED))
            .correlationKey(::paymentId)
            .correlationKeySuffix(DEADLINE_SUFFIX)
            .expire(true)
            .build()
    }

    internal fun withDeadline(uow: UnitOfWork): UnitOfWork {
        val requestedAt = uow.event?.timestamp ?: System.currentTimeMillis()
        val deadline = requestedAt / 1000 + deadlineSeconds
        return uow.copy(meta = (uow.meta ?: emptyMap()) + ("ttl" to deadline.toString()))
    }

    private fun rule(
        id: String,
        trigger: String,
        emit: (UnitOfWork) -> List<Event>,
        expression: ((UnitOfWork) -> Boolean)? = null,
    ): EvaluatePipeline =
        EvaluatePipeline.builder()
            .id(id)
            .eventPublisher(eventPublisher)
            .eventsMicrostore(eventsMicrostore)
            .eventCodec(codec)
            .isEvaluateEvent(::isCorrelationInsert)
            .normalizer(correlationNormalizer(id, codec, { it.new }))
            .eventFilter(EventFilters.name(trigger))
            .emit(emit)
            .apply { if (expression != null) expression(expression) }
            .build()

    val startPipeline by lazy { rule(Rules.START, PaymentsEvent.PAYMENT_SUBMITTED, ::start) }
    val nextStepPipeline by lazy { rule(Rules.NEXT_STEP, PaymentsEvent.FUNDS_WITHDRAWN, ::nextStep) }
    val donePipeline by lazy { rule(Rules.DONE, PaymentsEvent.FUNDS_DEPOSITED, ::done) }
    val nothingToUndoPipeline by lazy { rule(Rules.NOTHING_TO_UNDO, PaymentsEvent.WITHDRAWAL_FAILED, ::nothingToUndo) }
    val compensatePipeline by lazy { rule(Rules.COMPENSATE, PaymentsEvent.DEPOSIT_FAILED, ::compensate) }

    // Needs the payment's other events (the DEPOSIT_FAILED behind the refund), so it queries the correlation.
    val undonePipeline by lazy { rule(Rules.UNDONE, PaymentsEvent.FUNDS_REFUNDED, ::undone, expression = { true }) }

    val timeoutPipeline: EvaluatePipeline by lazy {
        EvaluatePipeline.builder()
            .id(Rules.TIMEOUT)
            .eventPublisher(eventPublisher)
            .eventsMicrostore(eventsMicrostore)
            .eventCodec(codec)
            .isEvaluateEvent(::isDeadlineExpiry)
            .normalizer(correlationNormalizer(Rules.TIMEOUT, codec, { it.old }, ::atDeadline))
            .correlationKeySuffix(DEADLINE_SUFFIX)
            .eventFilter(EventFilters.name(PaymentsEvent.DEPOSIT_REQUESTED))
            .expression(::depositIsLate)
            .emit(::timeout)
            .build()
    }

    val assembler: PipelineAssembler by lazy {
        PipelineAssembler.builder()
            .addPipeline(correlatePipeline)
            .addPipeline(deadlinePipeline)
            .addPipeline(startPipeline)
            .addPipeline(nextStepPipeline)
            .addPipeline(donePipeline)
            .addPipeline(nothingToUndoPipeline)
            .addPipeline(compensatePipeline)
            .addPipeline(undonePipeline)
            .addPipeline(timeoutPipeline)
            .build()
    }

    val dynamoDbAdapter: DynamodbAdapter by lazy { DynamodbAdapter() }
}
