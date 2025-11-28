package org.myorg.sut

import org.junit.jupiter.api.Test
import com.amazonaws.services.eventbridge.AmazonEventBridgeClient
import com.amazonaws.services.eventbridge.model.PutEventsRequest
import com.amazonaws.services.eventbridge.model.PutEventsRequestEntry
import org.junit.jupiter.api.Assertions.assertNotNull
//import software.amazon.awssdk.core.SdkBytes;
//import software.amazon.awssdk.regions.Region;
//import software.amazon.awssdk.services.kinesis.KinesisClient;
//import software.amazon.awssdk.services.kinesis.model.PutRecordRequest;
//import software.amazon.awssdk.services.kinesis.model.KinesisException;

// Components tested.
//   - Connection from Kinesis Stream to Lambda Listener.
//   - Listener being able to write to Dynamodb.
//  Possibly also but not sure
//    - Trigger lambda consuming from Dynamodb stream.
//    - Trigger sending messages to EventBridge.

class ListenerTest {


    @Test
    fun sendEvents() {

        // send event to event bridge running in local stack.
        println("Running integration test")



/*        val eventBridgeClient = AmazonEventBridgeClient.builder()
            .build()

        val event = PutEventsRequestEntry()
            .withDetail("")
            .withSource("listener-test")
            .withDetailType("event-submit")

        val request = PutEventsRequest()
            .withEntries(event)
            .withEndpointId("")

        val putEventsResult = eventBridgeClient.putEvents(request)

        assertNotNull(putEventsResult)
        */


    }

}