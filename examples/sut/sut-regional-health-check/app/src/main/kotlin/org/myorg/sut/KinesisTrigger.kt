package org.myorg.sut

import com.amazonaws.services.lambda.runtime.Context
import com.amazonaws.services.lambda.runtime.RequestHandler
import com.amazonaws.services.lambda.runtime.events.KinesisEvent
import kotlinx.coroutines.runBlocking
import mu.KotlinLogging

/**
 * Consume DynamoDB events and publish to an S3 bucket
 */
class KinesisTrigger (private val container: KinesisTriggerContainer = KinesisTriggerContainer.build()): RequestHandler<KinesisEvent, String> {

    private val logger = KotlinLogging.logger {  }

    override fun handleRequest(kinesisEvent: KinesisEvent, context: Context) : String = runBlocking{

        val headFlow = container.kinesisAdapter.fromKinesis(kinesisEvent)
        logger.info { "Processing ${kinesisEvent.records?.size} records" }
        container.assembler
            .assemble(headFlow, true)
            .collect {
                val eventClass = it.event?.javaClass?.simpleName ?: "unknown"
                //val eventAsString = it.event?.encoded() ?: "no event"
                logger.info { "processed event ${it.event?.id}, $eventClass" }
            }

        "Done"
    }
}