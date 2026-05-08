package org.myorg.sut

import com.amazonaws.services.lambda.runtime.Context
import com.amazonaws.services.lambda.runtime.RequestHandler
import com.amazonaws.services.lambda.runtime.events.KinesisEvent
import kotlinx.coroutines.runBlocking
import mu.KotlinLogging

class Listener(private var container : ListenerContainer = ListenerContainer.build()) : RequestHandler<KinesisEvent, Void?> {

    private val logger = KotlinLogging.logger {  }

    override fun handleRequest(kinesisEvent: KinesisEvent, context: Context): Void? = runBlocking{
        val assembler = container.assembler
        val faultManager = container.faultManager
        val headFlow = container.kinesisAdapter
            .fromKinesis(faultManager, kinesisEvent)

        assembler
            .assemble(headFlow, true)
            .collect { logger.info { "materialized " + it.event?.id} }
        null
    }
}