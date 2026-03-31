package org.myorg.sut

import com.amazonaws.services.lambda.runtime.Context
import com.amazonaws.services.lambda.runtime.RequestHandler
import com.amazonaws.services.lambda.runtime.events.DynamodbEvent
import kotlinx.coroutines.runBlocking
import mu.KotlinLogging

class Trigger constructor(private val container: TriggerContainer): RequestHandler<DynamodbEvent, String> {

    private val logger = KotlinLogging.logger {  }

    override fun handleRequest(ddbEvent: DynamodbEvent, context: Context) : String = runBlocking{

        val headFlow = container.dynamoDbAdapter.fromDynamoDB(ddbEvent)
        logger.info { "Processing ${ddbEvent.records?.size} records" }
        container.assembler
            .assemble(headFlow, true)
            .collect {
                val eventClass = it.event?.javaClass?.simpleName ?: "null"
                logger.info { "collected event ${it.event?.id}, $eventClass" }
            }

        "Done"
    }
}