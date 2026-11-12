package org.myorg.payments.adapters.inbound

import io.kopipes.aws.AwsKopipesContext
import io.kopipes.aws.flavors.CdcPipeline
import io.kopipes.aws.from.DynamodbAdapter
import io.kopipes.aws.from.RecordPair
import io.kopipes.core.Event
import io.kopipes.core.PipelineAssembler
import io.kopipes.core.UnitOfWork
import io.kopipes.core.flavors.Pipeline
import io.kopipes.core.sinks.EventPublisher
import org.myorg.payments.PaymentSubmitted
import org.myorg.payments.adapters.outbound.Items
import org.myorg.payments.adapters.outbound.paymentRecordOf

/**
 * Accepted means published: `PAYMENT_SUBMITTED` comes from the table's stream, never from the request itself.
 * No dual write: a payment is never accepted without its event, nor published without being accepted.
 * It fires when the item first gets its `requestHash`, which happens once per payment.
 */
fun toPaymentSubmitted(uow: UnitOfWork): Event? {
    val images = uow.event?.raw as? RecordPair ?: return null
    val new = images.new ?: return null
    if (new.getS("sk") != Items.PAYMENT) return null
    if (images.old?.getS(Items.REQUEST_HASH) != null) return null
    val record = paymentRecordOf(new.keys, new) ?: return null
    val payment = record.payment
    return PaymentSubmitted(
        payment = payment,
        // One payment, one PAYMENT_SUBMITTED id, however often the request is retried.
        id = "${payment.id}.submitted",
        timestamp = payment.submittedAt,
        partitionKey = payment.id,
    )
}

class TriggerContainer(
    val context: AwsKopipesContext,
    val eventPublisher: EventPublisher = context.eventPublisher,
) {
    companion object {
        const val SUBMITTED = "submitted"

        fun build(): TriggerContainer = TriggerContainer(AwsKopipesContext())
    }

    val cdcPipeline: Pipeline by lazy {
        CdcPipeline.builder()
            .id(SUBMITTED)
            .eventPublisher(eventPublisher)
            .toEvent { uow -> toPaymentSubmitted(uow) }
            .build()
    }

    val assembler: PipelineAssembler by lazy {
        PipelineAssembler.builder()
            .context(context)
            .addPipeline(cdcPipeline)
            .build()
    }

    val dynamoDbAdapter: DynamodbAdapter by lazy { DynamodbAdapter(context) }
}
