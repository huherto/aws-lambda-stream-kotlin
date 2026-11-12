package org.myorg.payments.adapters.inbound

import com.amazonaws.services.lambda.runtime.Context
import com.amazonaws.services.lambda.runtime.RequestHandler
import com.amazonaws.services.lambda.runtime.events.DynamodbEvent
import com.amazonaws.services.lambda.runtime.events.KinesisEvent
import io.kopipes.aws.AwsKopipesContext
import io.kopipes.aws.from.DynamodbAdapter
import io.kopipes.aws.from.KinesisAdapter
import io.kopipes.core.Event
import io.kopipes.core.JsonRaw
import io.kopipes.core.PipelineAssembler
import io.kopipes.core.UnitOfWork
import io.kopipes.core.filters.EventFilters
import io.kopipes.core.sinks.EventPublisher
import io.kopipes.core.utils.loggedLazy
import mu.KotlinLogging
import org.myorg.payments.*

/**
 * An anti-corruption layer, so all of it is adapter: no policy of its own, only translation.
 *
 * Hears core checking through change data capture on its ledger and produces our events.
 *
 * | Hears (a ledger row)                       | Produces                       |
 * |--------------------------------------------|--------------------------------|
 * | `PMT`, `POSTED`, reference `p-7`           | FUNDS_WITHDRAWN                |
 * | `PMT`, `REJECTED` (`NSF`, `ACCT_CLSD`, …)  | WITHDRAWAL_FAILED (`reason`)   |
 * | `RFND`, `POSTED`, reference `p-7`          | FUNDS_REFUNDED                 |
 *
 * The same ledger row always yields the same event id. `raw` is the row.
 */
fun ledgerEvent(entry: LedgerEntry, payment: Payment): PaymentEvent {
    val raw = JsonRaw(entry.toJson())
    val timestamp = entry.POST_TS.toLongOrNull() ?: error("ledger row ${entry.ENTRY_ID} has no valid POST_TS")
    fun id(suffix: String) = "${entry.ENTRY_ID}.$suffix"
    return when (entry.TXN_TYP to entry.STAT) {
        LedgerEntry.PAYMENT to LedgerEntry.POSTED ->
            FundsWithdrawn(payment, id("funds-withdrawn"), timestamp, payment.id, raw = raw)
        LedgerEntry.PAYMENT to LedgerEntry.REJECTED ->
            WithdrawalFailed(payment, coreCheckingReason(entry.REJ_CD), id("withdrawal-failed"), timestamp, payment.id, raw = raw)
        LedgerEntry.REFUND to LedgerEntry.POSTED ->
            FundsRefunded(payment, id("funds-refunded"), timestamp, payment.id, raw = raw)
        // A rejected refund has no event in our catalog: someone has to look at it. It becomes a fault.
        else -> error("ledger row ${entry.ENTRY_ID}: no event for ${entry.TXN_TYP} ${entry.STAT}")
    }
}

/** Only new rows: the legacy ledger never rewrites one. */
fun newLedgerEntry(uow: UnitOfWork): LedgerEntry? {
    val record = uow.record as? DynamodbEvent.DynamodbStreamRecord ?: return null
    if (record.eventName != "INSERT") return null
    val image = record.dynamodb?.newImage ?: return null
    return LedgerEntry.fromAttributes(image.mapValues { it.value?.s })
}

class LedgerContainer(
    val context: AwsKopipesContext,
    val lookup: PaymentLookup = DynamoDbPaymentLookup.fromContext(context),
    val eventPublisher: EventPublisher = context.eventPublisher,
) {
    companion object {
        fun build(): LedgerContainer = LedgerContainer(gatewayContext())
    }

    val assembler: PipelineAssembler by lazy {
        PipelineAssembler.builder()
            .context(context)
            .addPipeline(
                TranslatePipeline.builder()
                    .id("ledger")
                    .toEvents { uow ->
                        val entry = newLedgerEntry(uow) ?: return@toEvents emptyList<Event>()
                        listOf(ledgerEvent(entry, lookup.payment(entry.REF_NO)))
                    }
                    .eventPublisher(eventPublisher)
                    .build()
            )
            .build()
    }

    val dynamoDbAdapter: DynamodbAdapter by lazy { DynamodbAdapter(context) }
}

/** The ledger's change feed → our events. */
class LedgerTrigger(
    containerFactory: () -> LedgerContainer = { LedgerContainer.build() },
) : RequestHandler<DynamodbEvent, Void?> {
    private val container by loggedLazy("LedgerContainer", KotlinLogging.logger { }, containerFactory)

    override fun handleRequest(event: DynamodbEvent, context: Context): Void? {
        runAssembler(container.assembler, container.dynamoDbAdapter.fromDynamoDB(event))
        return null
    }
}

/** Default channel → the payments we asked core checking about. */
class RequestsContainer(val context: AwsKopipesContext, table: String) {
    companion object {
        fun build() = RequestsContainer(
            AwsKopipesContext(),
            System.getenv("REQUESTS_TABLE_NAME") ?: error("REQUESTS_TABLE_NAME is not configured"),
        )
    }

    val assembler: PipelineAssembler = PipelineAssembler.builder()
        .context(context)
        .addPipeline(
            requestsPipeline(
                context, "requests", table,
                EventFilters.anyOf(
                    EventFilters.name(PaymentsEvent.WITHDRAWAL_REQUESTED),
                    EventFilters.name(PaymentsEvent.REFUND_REQUESTED),
                ),
            )
        )
        .build()

    val kinesisAdapter = KinesisAdapter(context, PaymentsEventCodec)
}

class Listener(
    containerFactory: () -> RequestsContainer = { RequestsContainer.build() },
) : RequestHandler<KinesisEvent, Void?> {
    private val container by loggedLazy("RequestsContainer", KotlinLogging.logger { }, containerFactory)

    override fun handleRequest(event: KinesisEvent, context: Context): Void? {
        runKinesis(container.assembler, container.kinesisAdapter, event)
        return null
    }
}
