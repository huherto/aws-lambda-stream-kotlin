package org.myorg.payments

import io.kopipes.aws.from.KinesisAdapter
import io.kopipes.aws.sinks.DynamoDbEventsMicrostore
import io.kopipes.core.PipelineAssembler
import io.kopipes.core.filters.EventFilters
import io.kopipes.core.flavors.CollectPipeline
import io.kopipes.core.sinks.EventsMicrostore

/**
 * Collects every payment event from the default channel into the events table (`pk = <event id>`, `sk = EVENT`).
 * Writing first and acting from the table's stream means a fact is never acted on unless it is recorded.
 * A redelivered event writes the same item again, so it changes nothing.
 */
class ListenerContainer(
    val eventsMicrostore: EventsMicrostore,
) {
    companion object {
        const val COLLECT = "collect"

        fun build(): ListenerContainer = ListenerContainer(eventsMicrostore = DynamoDbEventsMicrostore())
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
            .addPipeline(collectPipeline)
            .build()
    }

    val kinesisAdapter: KinesisAdapter by lazy { KinesisAdapter(eventCodec = PaymentsEventCodec) }
}
