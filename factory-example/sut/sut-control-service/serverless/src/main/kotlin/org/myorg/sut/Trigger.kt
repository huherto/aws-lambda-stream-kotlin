package org.myorg.sut

import com.amazonaws.services.lambda.runtime.Context
import com.amazonaws.services.lambda.runtime.RequestHandler
import com.amazonaws.services.lambda.runtime.events.DynamodbEvent
import io.github.huherto.awsLambdaStream.asJson
import io.github.huherto.awsLambdaStream.from.DynamodbAdapter
import kotlinx.coroutines.runBlocking
import mu.KotlinLogging

class Trigger : RequestHandler<DynamodbEvent, String> {

    private val logger = KotlinLogging.logger {  }

    override fun handleRequest(ddbEvent: DynamodbEvent, context: Context) : String = runBlocking{

        logger.info{"Received DynamodbEvent $ddbEvent"}
        DynamodbAdapter().fromDynamoDB(ddbEvent).collect {
            logger.info{"Collected " + it.asJson()}
        }
        "Done"
    }
}