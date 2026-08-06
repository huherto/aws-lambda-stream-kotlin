package org.myorg.sut

import com.amazonaws.services.lambda.runtime.Context
import com.amazonaws.services.lambda.runtime.RequestHandler
import com.amazonaws.services.lambda.runtime.events.DynamodbEvent
import io.github.huherto.awsLambdaStream.utils.loggedLazy
import kotlinx.coroutines.runBlocking
import mu.KotlinLogging

class Trigger (
    containerFactory: () -> TriggerContainer = { TriggerContainer.build() }
): RequestHandler<DynamodbEvent, String> {

    private val logger = KotlinLogging.logger {  }

    private val container: TriggerContainer by loggedLazy(
        name = "TriggerContainer",
        logger = logger,
        initializer = containerFactory,
    )

    override fun handleRequest(ddbEvent: DynamodbEvent, context: Context) : String = runBlocking{

        val headFlow = container.dynamoDbAdapter.fromDynamoDB(ddbEvent)
        logger.info { "Processing ${ddbEvent.records?.size} records" }
        container.assembler
            .assemble(headFlow, true)
            .collect {
                val eventClass = it.event?.javaClass?.simpleName ?: "unknown"
                val eventAsString = it.event?.encoded() ?: "no event"
                logger.info { "processed event ${it.event?.id}, $eventClass" }
            }

        "Done"
    }
}