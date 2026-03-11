package org.myorg.sut

import com.amazonaws.services.lambda.runtime.Context
import com.amazonaws.services.lambda.runtime.RequestHandler
import com.amazonaws.services.lambda.runtime.events.DynamodbEvent
import mu.KotlinLogging

class Trigger : RequestHandler<DynamodbEvent, String> {

    private val logger = KotlinLogging.logger {  }

    override fun handleRequest(ddbEvent: DynamodbEvent, context: Context): String {
        if (ddbEvent.records != null) {
            logger.info{"Event received " + ddbEvent.records}
            for (record in ddbEvent.records) {
                logger.info{"Record $record"}
            }
        }
        if (ddbEvent.records != null) {
            return "Validated " + ddbEvent.records.size + " records."
        }
        return "No records found."
    }
}