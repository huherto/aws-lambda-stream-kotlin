package org.myorg.payments

import com.amazonaws.services.lambda.runtime.Context
import com.amazonaws.services.lambda.runtime.RequestHandler
import com.amazonaws.services.lambda.runtime.events.KinesisEvent
import io.kopipes.aws.AwsKopipesContext
import io.kopipes.aws.flavors.CdcPipeline
import io.kopipes.aws.from.KinesisAdapter
import io.kopipes.aws.sinks.EventBridgePublisher
import io.kopipes.core.EnvelopeEncryptionMetadata
import io.kopipes.core.Event
import io.kopipes.core.EventReference
import io.kopipes.core.PipelineAssembler
import io.kopipes.core.RawRecord
import io.kopipes.core.UnitOfWork
import io.kopipes.core.filters.EventFilters
import io.kopipes.core.sinks.EventPublisher
import io.kopipes.core.utils.loggedLazy
import kotlinx.serialization.Serializable
import kotlinx.serialization.Transient
import kotlinx.serialization.json.Json
import mu.KotlinLogging

/** What the payments subsystem shares about a payment. No account numbers, no steps, no `raw`. */
@Serializable
data class ExternalPayment(
    val id: String,
    /** Personal data crossing a boundary: shared so a downstream subsystem can contact the customer. */
    val customerId: String,
    val amount: String,
    val currency: String,
    /** `card` or `bank`: the destination kind, never the account. */
    val destination: String,
    val submittedAt: Long,
)

/**
 * The payments subsystem's **external** domain events, a stable contract published as JSON Schema
 * (`schema/payments-external-events.schema.json`). Change it only by adding fields.
 */
@Serializable
data class PaymentsExternalEvent(
    val type: String,
    val payment: ExternalPayment,
    /** On `PAYMENTS.PAYMENT_FAILED` only, from the public list. */
    val reason: String? = null,
    override val id: String? = null,
    override val timestamp: Long? = null,
    override val partitionKey: String? = null,
    override val tags: Map<String, String>? = null,
) : Event {

    // Never shared: internal raw data, encryption metadata of internal events, references to internal events.
    @Transient override val raw: RawRecord? = null
    @Transient override val eem: EnvelopeEncryptionMetadata? = null
    @Transient override val triggers: List<EventReference>? = null

    companion object {
        const val PAYMENT_COMPLETED = "PAYMENTS.PAYMENT_COMPLETED"
        const val PAYMENT_FAILED = "PAYMENTS.PAYMENT_FAILED"
    }

    override fun eventType(): String = type

    override fun toString(): String = externalJson.encodeToString(serializer(), this)

    /** Only the envelope fields that may cross the boundary are copied; `raw`, `eem` and `triggers` never are. */
    override fun copyEvent(
        id: String?, timestamp: Long?, partitionKey: String?, tags: Map<String, String>?,
        raw: RawRecord?, eem: EnvelopeEncryptionMetadata?, triggers: List<EventReference>?,
    ): Event = copy(id = id, timestamp = timestamp, partitionKey = partitionKey, tags = tags)
}

private val externalJson = Json { explicitNulls = false; encodeDefaults = false }

/** A new internal reason maps to `rejected` until we decide to publish it. */
fun publicReason(reason: String): String = if (reason in Reasons.ALL) reason else Reasons.REJECTED

/**
 * | Consumes (internal) | Produces (external)          |
 * |---------------------|------------------------------|
 * | PAYMENT_COMPLETED   | PAYMENTS.PAYMENT_COMPLETED   |
 * | PAYMENT_FAILED      | PAYMENTS.PAYMENT_FAILED      |
 *
 * The id derives from the internal event's id, so a redelivered internal event produces the same external event.
 */
fun toExternal(uow: UnitOfWork): Event? {
    val internal = uow.event as? PaymentEvent ?: return null
    val type = when (internal) {
        is PaymentCompleted -> PaymentsExternalEvent.PAYMENT_COMPLETED
        is PaymentFailed -> PaymentsExternalEvent.PAYMENT_FAILED
        else -> return null
    }
    val payment = internal.payment
    return PaymentsExternalEvent(
        type = type,
        payment = ExternalPayment(
            id = payment.id,
            customerId = payment.customerId,
            amount = payment.amount,
            currency = payment.currency,
            destination = payment.to.kind,
            submittedAt = payment.submittedAt,
        ),
        reason = (internal as? PaymentFailed)?.reason?.let(::publicReason),
        id = "${internal.id}.external",
        timestamp = internal.timestamp,
        partitionKey = payment.id,
        tags = mapOf("subsystem" to "payments"),
    )
}

class EgressContainer(
    val context: AwsKopipesContext,
    val eventPublisher: EventPublisher = context.eventPublisher,
) {
    companion object {
        /** `source = external` on the bus: the egress rule forwards these, and the default channel skips them. */
        fun build(): EgressContainer {
            val context = AwsKopipesContext()
            return EgressContainer(
                context,
                EventBridgePublisher(context.envConfig, context.eventBridgeClientFactory, source = "external"),
            )
        }
    }

    val assembler: PipelineAssembler = PipelineAssembler.builder()
        .context(context)
        .addPipeline(
            CdcPipeline.builder()
                .id("outcomes")
                .eventFilter(
                    EventFilters.anyOf(
                        EventFilters.name(PaymentsEvent.PAYMENT_COMPLETED),
                        EventFilters.name(PaymentsEvent.PAYMENT_FAILED),
                    )
                )
                .toEvent { uow -> toExternal(uow) }
                .eventPublisher(eventPublisher)
                .build()
        )
        .build()

    val kinesisAdapter = KinesisAdapter(context, PaymentsEventCodec)
}

class Listener(
    containerFactory: () -> EgressContainer = { EgressContainer.build() },
) : RequestHandler<KinesisEvent, Void?> {
    private val container by loggedLazy("EgressContainer", KotlinLogging.logger { }, containerFactory)

    override fun handleRequest(event: KinesisEvent, context: Context): Void? {
        runKinesis(container.assembler, container.kinesisAdapter, event)
        return null
    }
}
