package io.github.huherto.awsLambdaStream.sinks

import io.github.huherto.awsLambdaStream.UnitOfWork
import kotlinx.coroutines.flow.*

data class EventBridgePublishOptions(
    val busName: String = System.getenv("BUS_NAME") ?: "undefined",
    val source: String = System.getenv("BUS_SRC") ?: "custom",
    val maxPublishRequestSize: Int = System.getenv("PUBLISH_MAX_REQ_SIZE")?.toIntOrNull() 
        ?: System.getenv("MAX_REQ_SIZE")?.toIntOrNull() ?: (256 * 1024),
    val batchSize: Int = System.getenv("PUBLISH_BATCH_SIZE")?.toIntOrNull() 
        ?: System.getenv("BATCH_SIZE")?.toIntOrNull() ?: 10,
    val parallel: Int = System.getenv("PUBLISH_PARALLEL")?.toIntOrNull() 
        ?: System.getenv("PARALLEL")?.toIntOrNull() ?: 8,
    val endpointId: String? = System.getenv("BUS_ENDPOINT_ID"),
    val handleErrors: Boolean = true,
    val step: String = "publish"
    // Other retryConfig / metric dependencies can be added here
)

class EventBridgeSink {

    companion object {

        /**
         * Represents the publishToEventBridge pipeline step.
         * Modeled as an extension function on Flow<UnitOfWork>.
         */
        fun Flow<UnitOfWork>.publishToEventBridge(
            opt: EventBridgePublishOptions = EventBridgePublishOptions()
        ): Flow<UnitOfWork> {
            return this
                // 1. adornStandardTags (Assuming extension or map operation exists)
                // .map { adornStandardTags(it, opt) }

                // 2. ratelimit (Assuming a corresponding Flow operator/delay exists)
                // .ratelimit(opt)

                // 3. toPublishRequestEntry
                .map { toPublishRequestEntry(it, opt) }
                
                // 4. batchWithSize (from your batch.kt implementation)
                // Assuming you map EventBridgePublishOptions to BatchSizeOptions
                /*
                .batchWithSize(BatchSizeOptions(
                    batchSize = opt.batchSize,
                    maxRequestSize = opt.maxPublishRequestSize,
                    // map other relevant fields...
                ))
                */
                // For demonstration, let's assume `batchWithSize` yields a List<UnitOfWork>
                .chunked(opt.batchSize) // Replace this line with your actual .batchWithSize(opt)

                // 5. storeClaimcheck (if not implicitly handled within batchWithSize)
                // .map { storeClaimcheck(it, opt) }

                // 6. toBatchUow
                .map { batchedList -> UnitOfWork(batch = batchedList) }
                
                // 7. toPublishRequest
                .map { toPublishRequest(it, opt) }
                
                // 8. putEvents & parallel execution
                // flatMapMerge acts as the equivalent of .parallel() in Highland.js
                .flatMapMerge(opt.parallel) { batchUow ->
                    flow {
                        emit(putEvents(batchUow, opt))
                    }
                }
                
                // 9. unBatchUow - for cleaner logging and testing downstream
                .flatMapConcat { batchUow ->
                    batchUow.batch?.asFlow() ?: emptyFlow()
                }
        }

        private fun toPublishRequestEntry(uow: UnitOfWork, opt: EventBridgePublishOptions): UnitOfWork {
            // Translates: map(toPublishRequestEntry)
            // Here you would convert the inner Event to an AWS EventBridge Entry.
            // In Kotlin, you might store this constructed entry inside the `UnitOfWork` meta/attributes map
            // or in a dedicated field within the `UnitOfWork` class.
            
            val event = uow.event
            if (event != null) {
                // val entry = PutEventsRequestEntry.builder()
                //     .eventBusName(opt.busName)
                //     .source(opt.source)
                //     .detailType(event.type)
                //     .detail(serializeAndCompress(event))
                //     .build()
                // store in uow (e.g. uow.copy(meta = uow.meta + ("publishRequestEntry" to entry)))
            }
            return uow
        }

        private fun toPublishRequest(batchUow: UnitOfWork, opt: EventBridgePublishOptions): UnitOfWork {
            // Translates: map(toPublishRequest)
            // Extract the prepared entries from the batch list and formulate the PutEventsRequest.
            // Return updated batchUow.
            return batchUow
        }

        private suspend fun putEvents(batchUow: UnitOfWork, opt: EventBridgePublishOptions): UnitOfWork {
            // Translates: map(putEvents)
            // The batchUow contains the publishRequest. Use EventBridgeAsyncClient to send the request.
            // Catch faults/exceptions based on `opt.handleErrors`
            
            // Example skeleton:
            // try {
            //     val request = extractRequest(batchUow)
            //     val response = eventBridgeClient.putEvents(request).await()
            //     return batchUow.copy(putResponse = response) // assuming generic or specific response binding
            // } catch(e: Exception) {
            //     handleFault(batchUow, e, opt.handleErrors)
            // }
            
            return batchUow
        }
    }
}