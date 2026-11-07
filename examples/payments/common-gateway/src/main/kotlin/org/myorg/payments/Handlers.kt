package org.myorg.payments

import com.amazonaws.services.lambda.runtime.events.KinesisEvent
import io.kopipes.aws.from.KinesisAdapter
import io.kopipes.core.PipelineAssembler
import io.kopipes.core.UnitOfWork
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.runBlocking
import mu.KotlinLogging

private val logger = KotlinLogging.logger { }

/** Runs an assembler over a head flow and logs what each pipeline did. */
fun runAssembler(assembler: PipelineAssembler, head: Flow<UnitOfWork>) = runBlocking {
    assembler.assemble(head).collect { uow ->
        logger.info { "${uow.pipeline?.id}: ${uow.event?.eventType()} ${uow.event?.id}" }
    }
}

fun runKinesis(assembler: PipelineAssembler, adapter: KinesisAdapter, event: KinesisEvent) {
    logger.info { "Invoked with ${event.records?.size ?: 0} Kinesis records" }
    runAssembler(assembler, adapter.fromKinesis(event))
}

/** `DEPOSIT_REQUESTED` for a destination of [kind]: the card and bank gateways each take their own. */
fun depositsTo(kind: String): io.kopipes.core.filters.EventFilter =
    io.kopipes.core.filters.EventFilters.allOf(
        io.kopipes.core.filters.EventFilters.name(PaymentsEvent.DEPOSIT_REQUESTED),
        io.kopipes.core.filters.EventFilters.onContent { (it as? PaymentEvent)?.payment?.to?.kind == kind },
    )
