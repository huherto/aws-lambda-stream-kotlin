package org.myorg.sut

import com.amazonaws.services.lambda.runtime.Context
import com.amazonaws.services.lambda.runtime.RequestHandler
import com.amazonaws.services.lambda.runtime.events.KinesisEvent
import io.github.huherto.awsLambdaStream.utils.loggedLazy
import kotlinx.coroutines.runBlocking
import mu.KotlinLogging

class Listener(
    containerFactory: () -> ListenerContainer = { ListenerContainer.build() },
) : RequestHandler<KinesisEvent, Void?> {

    private val logger = KotlinLogging.logger {  }

    private val container: ListenerContainer by loggedLazy(
        name = "ListenerContainer",
        logger = logger,
        initializer = containerFactory,
    )
    override fun handleRequest(kinesisEvent: KinesisEvent, context: Context): Void? = runBlocking {
        logger.info { "Listener invoked with ${kinesisEvent.records?.size ?: 0} Kinesis records" }

        val assembler = container.assembler
        val headFlow = container.kinesisAdapter
            .fromKinesis(kinesisEvent)

        assembler
            .assemble(headFlow, true)
            .collect { logger.info { "collected " + it.event?.id } }
        null
    }
}