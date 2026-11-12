package org.myorg.payments.adapters.inbound

import com.amazonaws.services.lambda.runtime.Context
import com.amazonaws.services.lambda.runtime.RequestHandler
import com.amazonaws.services.lambda.runtime.events.DynamodbEvent
import com.amazonaws.services.lambda.runtime.events.KinesisEvent
import io.kopipes.core.utils.loggedLazy
import kotlinx.coroutines.runBlocking
import mu.KotlinLogging

/** Default channel → the customer's view and the known accounts. */
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
            .collect { logger.info { "materialized ${it.pipeline?.id} ${it.event?.eventType()} ${it.event?.id}" } }
        null
    }
}

/** The BFF table's stream → `PAYMENT_SUBMITTED` on the bus. */
class Trigger(
    containerFactory: () -> TriggerContainer = { TriggerContainer.build() },
) : RequestHandler<DynamodbEvent, Void?> {

    private val logger = KotlinLogging.logger { }

    private val container: TriggerContainer by loggedLazy(
        name = "TriggerContainer",
        logger = logger,
        initializer = containerFactory,
    )

    override fun handleRequest(dynamodbEvent: DynamodbEvent, context: Context): Void? = runBlocking {
        logger.info { "Trigger invoked with ${dynamodbEvent.records?.size ?: 0} DynamoDB records" }
        container.assembler
            .assemble(container.dynamoDbAdapter.fromDynamoDB(dynamodbEvent))
            .collect { logger.info { "published ${it.event?.eventType()} ${it.event?.id}" } }
        null
    }
}
