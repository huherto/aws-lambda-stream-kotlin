package org.myorg.sut

import com.amazonaws.services.lambda.runtime.Context
import com.amazonaws.services.lambda.runtime.RequestHandler
import com.amazonaws.services.lambda.runtime.events.DynamodbEvent
import io.kopipes.core.utils.loggedLazy
import kotlinx.coroutines.runBlocking
import mu.KotlinLogging

class Trigger constructor(
    containerFactory: () -> TriggerContainer = { TriggerContainer.build() }
) : RequestHandler<DynamodbEvent, Void?> {

    private val logger = KotlinLogging.logger { }

    private val container: TriggerContainer by loggedLazy(
        name = "TriggerContainer",
        logger = logger,
        initializer = containerFactory,
    )

    override fun handleRequest(dynamodbEvent: DynamodbEvent, context: Context): Void? = runBlocking {
        logger.info { "Trigger invoked with ${dynamodbEvent.records?.size ?: 0} DynamoDB records" }

        val assembler = container.assembler
        val headFlow = container.dynamoDbAdapter
            .fromDynamoDB(dynamodbEvent)

        assembler
            .assemble(headFlow)
            .collect { logger.info { "collected " + it.event?.id } }
        null
    }
}
