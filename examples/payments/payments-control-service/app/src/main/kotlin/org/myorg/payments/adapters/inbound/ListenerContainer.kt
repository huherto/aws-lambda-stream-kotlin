package org.myorg.payments.adapters.inbound

import io.kopipes.aws.AwsKopipesContext
import io.kopipes.aws.from.KinesisAdapter
import io.kopipes.aws.sinks.DynamoDbEventsMicrostore
import io.kopipes.core.PipelineAssembler
import io.kopipes.core.filters.EventFilters
import io.kopipes.core.flavors.CollectPipeline
import io.kopipes.core.sinks.EventsMicrostore
import org.myorg.payments.PaymentEvent
import org.myorg.payments.PaymentsEventCodec

/**
 * Collects every payment event from the default channel into the events table (`pk = <event id>`, `sk = EVENT`).
 * Writing first and acting from the table's stream means a fact is never acted on unless it is recorded.
 * A redelivered event writes the same item again, so it changes nothing.
 */
class ListenerContainer(
    val context: AwsKopipesContext,
    val eventsMicrostore: EventsMicrostore = DynamoDbEventsMicrostore(context),
) {
    companion object {
        const val COLLECT = "collect"

        fun build(): ListenerContainer = ListenerContainer(AwsKopipesContext())
    }

    val collectPipeline: CollectPipeline by lazy {
        CollectPipeline.builder()
            .id(COLLECT)
            // ACCOUNT_CHANGED and any other non-payment event are of no interest to the payment process.
            .eventFilter(EventFilters.classes(PaymentEvent::class))
            .correlationKey { uow -> (uow.event as? PaymentEvent)?.payment?.id }
            .eventsMicrostore(eventsMicrostore)
            .build()
    }

    val assembler: PipelineAssembler by lazy {
        PipelineAssembler.builder()
            .context(context)
            .addPipeline(collectPipeline)
            .build()
    }

    val kinesisAdapter: KinesisAdapter by lazy { KinesisAdapter(context, PaymentsEventCodec) }
}
