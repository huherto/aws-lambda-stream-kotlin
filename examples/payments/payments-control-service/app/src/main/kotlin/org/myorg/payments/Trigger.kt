package org.myorg.payments

import com.amazonaws.services.lambda.runtime.Context
import com.amazonaws.services.lambda.runtime.RequestHandler
import com.amazonaws.services.lambda.runtime.events.DynamodbEvent
import io.kopipes.core.utils.loggedLazy
import kotlinx.coroutines.runBlocking
import mu.KotlinLogging

/** DynamoDB stream of the events table → correlate, evaluate the rules, publish. */
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
            .collect { logger.info { "processed ${it.pipeline?.id} ${it.event?.eventType()} ${it.event?.id}" } }
        null
    }
}
