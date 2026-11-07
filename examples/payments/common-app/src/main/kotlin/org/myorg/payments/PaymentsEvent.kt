package org.myorg.payments

import io.kopipes.core.EnvelopeEncryptionMetadata
import io.kopipes.core.Event
import io.kopipes.core.EventReference
import io.kopipes.core.RawRecord
import kotlinx.serialization.InternalSerializationApi
import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable
import kotlinx.serialization.serializerOrNull

/**
 * The payments subsystem's **internal** domain events: they flow only between its own services.
 * The envelope is the framework's [Event]; `type` is the polymorphic discriminator.
 */
@Serializable
sealed class PaymentsEvent : Event {

    companion object {
        const val PAYMENT_SUBMITTED = "PAYMENT_SUBMITTED"
        const val WITHDRAWAL_REQUESTED = "WITHDRAWAL_REQUESTED"
        const val FUNDS_WITHDRAWN = "FUNDS_WITHDRAWN"
        const val WITHDRAWAL_FAILED = "WITHDRAWAL_FAILED"
        const val DEPOSIT_REQUESTED = "DEPOSIT_REQUESTED"
        const val FUNDS_DEPOSITED = "FUNDS_DEPOSITED"
        const val DEPOSIT_FAILED = "DEPOSIT_FAILED"
        const val REFUND_REQUESTED = "REFUND_REQUESTED"
        const val FUNDS_REFUNDED = "FUNDS_REFUNDED"
        const val PAYMENT_COMPLETED = "PAYMENT_COMPLETED"
        const val PAYMENT_FAILED = "PAYMENT_FAILED"
        const val PAYMENT_EXPIRED = "PAYMENT_EXPIRED"
        const val ACCOUNT_CHANGED = "ACCOUNT_CHANGED"
    }

    abstract override val id: String?
    abstract override val timestamp: Long?
    abstract override val partitionKey: String?
    abstract override val tags: Map<String, String>?
    abstract override val raw: RawRecord?
    abstract override val eem: EnvelopeEncryptionMetadata?
    abstract override val triggers: List<EventReference>?

    @OptIn(InternalSerializationApi::class)
    override fun eventType(): String =
        this::class.serializerOrNull()?.descriptor?.serialName ?: "unknown"

    override fun toString(): String = PaymentsEventCodec.encode(this)
}

/** An event about one payment. It carries the payment's state at that moment. */
@Serializable
sealed class PaymentEvent : PaymentsEvent() {
    abstract val payment: Payment
}

/** A failure. `reason` is one of [Reasons]. */
interface Failure {
    val reason: String
}

@Serializable
@SerialName(PaymentsEvent.PAYMENT_SUBMITTED)
data class PaymentSubmitted(
    override val payment: Payment,
    override val id: String? = null,
    override val timestamp: Long? = null,
    override val partitionKey: String? = null,
    override val tags: Map<String, String>? = null,
    override val raw: RawRecord? = null,
    override val eem: EnvelopeEncryptionMetadata? = null,
    override val triggers: List<EventReference>? = null,
) : PaymentEvent() {
    override fun toString() = super.toString()
    override fun copyEvent(
        id: String?, timestamp: Long?, partitionKey: String?, tags: Map<String, String>?,
        raw: RawRecord?, eem: EnvelopeEncryptionMetadata?, triggers: List<EventReference>?,
    ): Event = copy(id = id, timestamp = timestamp, partitionKey = partitionKey, tags = tags, raw = raw, eem = eem, triggers = triggers)
}

@Serializable
@SerialName(PaymentsEvent.WITHDRAWAL_REQUESTED)
data class WithdrawalRequested(
    override val payment: Payment,
    override val id: String? = null,
    override val timestamp: Long? = null,
    override val partitionKey: String? = null,
    override val tags: Map<String, String>? = null,
    override val raw: RawRecord? = null,
    override val eem: EnvelopeEncryptionMetadata? = null,
    override val triggers: List<EventReference>? = null,
) : PaymentEvent() {
    override fun toString() = super.toString()
    override fun copyEvent(
        id: String?, timestamp: Long?, partitionKey: String?, tags: Map<String, String>?,
        raw: RawRecord?, eem: EnvelopeEncryptionMetadata?, triggers: List<EventReference>?,
    ): Event = copy(id = id, timestamp = timestamp, partitionKey = partitionKey, tags = tags, raw = raw, eem = eem, triggers = triggers)
}

@Serializable
@SerialName(PaymentsEvent.FUNDS_WITHDRAWN)
data class FundsWithdrawn(
    override val payment: Payment,
    override val id: String? = null,
    override val timestamp: Long? = null,
    override val partitionKey: String? = null,
    override val tags: Map<String, String>? = null,
    override val raw: RawRecord? = null,
    override val eem: EnvelopeEncryptionMetadata? = null,
    override val triggers: List<EventReference>? = null,
) : PaymentEvent() {
    override fun toString() = super.toString()
    override fun copyEvent(
        id: String?, timestamp: Long?, partitionKey: String?, tags: Map<String, String>?,
        raw: RawRecord?, eem: EnvelopeEncryptionMetadata?, triggers: List<EventReference>?,
    ): Event = copy(id = id, timestamp = timestamp, partitionKey = partitionKey, tags = tags, raw = raw, eem = eem, triggers = triggers)
}

@Serializable
@SerialName(PaymentsEvent.WITHDRAWAL_FAILED)
data class WithdrawalFailed(
    override val payment: Payment,
    override val reason: String,
    override val id: String? = null,
    override val timestamp: Long? = null,
    override val partitionKey: String? = null,
    override val tags: Map<String, String>? = null,
    override val raw: RawRecord? = null,
    override val eem: EnvelopeEncryptionMetadata? = null,
    override val triggers: List<EventReference>? = null,
) : PaymentEvent(), Failure {
    override fun toString() = super.toString()
    override fun copyEvent(
        id: String?, timestamp: Long?, partitionKey: String?, tags: Map<String, String>?,
        raw: RawRecord?, eem: EnvelopeEncryptionMetadata?, triggers: List<EventReference>?,
    ): Event = copy(id = id, timestamp = timestamp, partitionKey = partitionKey, tags = tags, raw = raw, eem = eem, triggers = triggers)
}

@Serializable
@SerialName(PaymentsEvent.DEPOSIT_REQUESTED)
data class DepositRequested(
    override val payment: Payment,
    override val id: String? = null,
    override val timestamp: Long? = null,
    override val partitionKey: String? = null,
    override val tags: Map<String, String>? = null,
    override val raw: RawRecord? = null,
    override val eem: EnvelopeEncryptionMetadata? = null,
    override val triggers: List<EventReference>? = null,
) : PaymentEvent() {
    override fun toString() = super.toString()
    override fun copyEvent(
        id: String?, timestamp: Long?, partitionKey: String?, tags: Map<String, String>?,
        raw: RawRecord?, eem: EnvelopeEncryptionMetadata?, triggers: List<EventReference>?,
    ): Event = copy(id = id, timestamp = timestamp, partitionKey = partitionKey, tags = tags, raw = raw, eem = eem, triggers = triggers)
}

@Serializable
@SerialName(PaymentsEvent.FUNDS_DEPOSITED)
data class FundsDeposited(
    override val payment: Payment,
    override val id: String? = null,
    override val timestamp: Long? = null,
    override val partitionKey: String? = null,
    override val tags: Map<String, String>? = null,
    override val raw: RawRecord? = null,
    override val eem: EnvelopeEncryptionMetadata? = null,
    override val triggers: List<EventReference>? = null,
) : PaymentEvent() {
    override fun toString() = super.toString()
    override fun copyEvent(
        id: String?, timestamp: Long?, partitionKey: String?, tags: Map<String, String>?,
        raw: RawRecord?, eem: EnvelopeEncryptionMetadata?, triggers: List<EventReference>?,
    ): Event = copy(id = id, timestamp = timestamp, partitionKey = partitionKey, tags = tags, raw = raw, eem = eem, triggers = triggers)
}

@Serializable
@SerialName(PaymentsEvent.DEPOSIT_FAILED)
data class DepositFailed(
    override val payment: Payment,
    override val reason: String,
    override val id: String? = null,
    override val timestamp: Long? = null,
    override val partitionKey: String? = null,
    override val tags: Map<String, String>? = null,
    override val raw: RawRecord? = null,
    override val eem: EnvelopeEncryptionMetadata? = null,
    override val triggers: List<EventReference>? = null,
) : PaymentEvent(), Failure {
    override fun toString() = super.toString()
    override fun copyEvent(
        id: String?, timestamp: Long?, partitionKey: String?, tags: Map<String, String>?,
        raw: RawRecord?, eem: EnvelopeEncryptionMetadata?, triggers: List<EventReference>?,
    ): Event = copy(id = id, timestamp = timestamp, partitionKey = partitionKey, tags = tags, raw = raw, eem = eem, triggers = triggers)
}

@Serializable
@SerialName(PaymentsEvent.REFUND_REQUESTED)
data class RefundRequested(
    override val payment: Payment,
    override val id: String? = null,
    override val timestamp: Long? = null,
    override val partitionKey: String? = null,
    override val tags: Map<String, String>? = null,
    override val raw: RawRecord? = null,
    override val eem: EnvelopeEncryptionMetadata? = null,
    override val triggers: List<EventReference>? = null,
) : PaymentEvent() {
    override fun toString() = super.toString()
    override fun copyEvent(
        id: String?, timestamp: Long?, partitionKey: String?, tags: Map<String, String>?,
        raw: RawRecord?, eem: EnvelopeEncryptionMetadata?, triggers: List<EventReference>?,
    ): Event = copy(id = id, timestamp = timestamp, partitionKey = partitionKey, tags = tags, raw = raw, eem = eem, triggers = triggers)
}

@Serializable
@SerialName(PaymentsEvent.FUNDS_REFUNDED)
data class FundsRefunded(
    override val payment: Payment,
    override val id: String? = null,
    override val timestamp: Long? = null,
    override val partitionKey: String? = null,
    override val tags: Map<String, String>? = null,
    override val raw: RawRecord? = null,
    override val eem: EnvelopeEncryptionMetadata? = null,
    override val triggers: List<EventReference>? = null,
) : PaymentEvent() {
    override fun toString() = super.toString()
    override fun copyEvent(
        id: String?, timestamp: Long?, partitionKey: String?, tags: Map<String, String>?,
        raw: RawRecord?, eem: EnvelopeEncryptionMetadata?, triggers: List<EventReference>?,
    ): Event = copy(id = id, timestamp = timestamp, partitionKey = partitionKey, tags = tags, raw = raw, eem = eem, triggers = triggers)
}

@Serializable
@SerialName(PaymentsEvent.PAYMENT_COMPLETED)
data class PaymentCompleted(
    override val payment: Payment,
    override val id: String? = null,
    override val timestamp: Long? = null,
    override val partitionKey: String? = null,
    override val tags: Map<String, String>? = null,
    override val raw: RawRecord? = null,
    override val eem: EnvelopeEncryptionMetadata? = null,
    override val triggers: List<EventReference>? = null,
) : PaymentEvent() {
    override fun toString() = super.toString()
    override fun copyEvent(
        id: String?, timestamp: Long?, partitionKey: String?, tags: Map<String, String>?,
        raw: RawRecord?, eem: EnvelopeEncryptionMetadata?, triggers: List<EventReference>?,
    ): Event = copy(id = id, timestamp = timestamp, partitionKey = partitionKey, tags = tags, raw = raw, eem = eem, triggers = triggers)
}

@Serializable
@SerialName(PaymentsEvent.PAYMENT_FAILED)
data class PaymentFailed(
    override val payment: Payment,
    override val reason: String,
    override val id: String? = null,
    override val timestamp: Long? = null,
    override val partitionKey: String? = null,
    override val tags: Map<String, String>? = null,
    override val raw: RawRecord? = null,
    override val eem: EnvelopeEncryptionMetadata? = null,
    override val triggers: List<EventReference>? = null,
) : PaymentEvent(), Failure {
    override fun toString() = super.toString()
    override fun copyEvent(
        id: String?, timestamp: Long?, partitionKey: String?, tags: Map<String, String>?,
        raw: RawRecord?, eem: EnvelopeEncryptionMetadata?, triggers: List<EventReference>?,
    ): Event = copy(id = id, timestamp = timestamp, partitionKey = partitionKey, tags = tags, raw = raw, eem = eem, triggers = triggers)
}

/** The deposit is late. A timeout, **not an outcome**: later events still move the payment on. */
@Serializable
@SerialName(PaymentsEvent.PAYMENT_EXPIRED)
data class PaymentExpired(
    override val payment: Payment,
    override val id: String? = null,
    override val timestamp: Long? = null,
    override val partitionKey: String? = null,
    override val tags: Map<String, String>? = null,
    override val raw: RawRecord? = null,
    override val eem: EnvelopeEncryptionMetadata? = null,
    override val triggers: List<EventReference>? = null,
) : PaymentEvent() {
    override fun toString() = super.toString()
    override fun copyEvent(
        id: String?, timestamp: Long?, partitionKey: String?, tags: Map<String, String>?,
        raw: RawRecord?, eem: EnvelopeEncryptionMetadata?, triggers: List<EventReference>?,
    ): Event = copy(id = id, timestamp = timestamp, partitionKey = partitionKey, tags = tags, raw = raw, eem = eem, triggers = triggers)
}

/** Reference data, translated by the accounts ingress gateway from the accounts subsystem's ACCOUNTS.* events. */
@Serializable
@SerialName(PaymentsEvent.ACCOUNT_CHANGED)
data class AccountChanged(
    val account: Account,
    override val id: String? = null,
    override val timestamp: Long? = null,
    override val partitionKey: String? = null,
    override val tags: Map<String, String>? = null,
    override val raw: RawRecord? = null,
    override val eem: EnvelopeEncryptionMetadata? = null,
    override val triggers: List<EventReference>? = null,
) : PaymentsEvent() {
    override fun toString() = super.toString()
    override fun copyEvent(
        id: String?, timestamp: Long?, partitionKey: String?, tags: Map<String, String>?,
        raw: RawRecord?, eem: EnvelopeEncryptionMetadata?, triggers: List<EventReference>?,
    ): Event = copy(id = id, timestamp = timestamp, partitionKey = partitionKey, tags = tags, raw = raw, eem = eem, triggers = triggers)
}
