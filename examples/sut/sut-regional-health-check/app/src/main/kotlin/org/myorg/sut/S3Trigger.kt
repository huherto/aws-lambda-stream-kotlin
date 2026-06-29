package org.myorg.sut

import com.amazonaws.services.lambda.runtime.Context
import com.amazonaws.services.lambda.runtime.RequestHandler
import com.amazonaws.services.lambda.runtime.events.SQSEvent
import kotlinx.coroutines.runBlocking
import mu.KotlinLogging

class S3Trigger (private val container: S3TriggerContainer = S3TriggerContainer.build()): RequestHandler<SQSEvent, String> {

    private val logger = KotlinLogging.logger {  }

    override fun handleRequest(sqsEvent: SQSEvent, context: Context) : String = runBlocking{

        val headFlow = container.s3Adapter.fromS3Event(sqsEvent)
        logger.info { "Processing ${sqsEvent.records?.size} records" }
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