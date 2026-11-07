package org.myorg.payments

import com.amazonaws.services.lambda.runtime.Context
import com.amazonaws.services.lambda.runtime.RequestHandler
import com.amazonaws.services.lambda.runtime.events.KinesisEvent
import io.kopipes.core.utils.loggedLazy
import kotlinx.coroutines.runBlocking
import mu.KotlinLogging

/** Ingress channel (ACCOUNTS.*) → ACCOUNT_CHANGED on our bus. */
class Listener(
    containerFactory: () -> ListenerContainer = { ListenerContainer.build() },
) : RequestHandler<KinesisEvent, Void?> {

    private val logger = KotlinLogging.logger { }

    private val container: ListenerContainer by loggedLazy(
        name = "ListenerContainer",
        logger = logger,
        initializer = containerFactory,
    )

    override fun handleRequest(kinesisEvent: KinesisEvent, context: Context): Void? = runBlocking {
        logger.info { "Listener invoked with ${kinesisEvent.records?.size ?: 0} Kinesis records" }
        container.assembler
            .assemble(container.kinesisAdapter.fromKinesis(kinesisEvent))
            .collect { logger.info { "translated ${it.event?.eventType()} ${it.event?.id}" } }
        null
    }
}
