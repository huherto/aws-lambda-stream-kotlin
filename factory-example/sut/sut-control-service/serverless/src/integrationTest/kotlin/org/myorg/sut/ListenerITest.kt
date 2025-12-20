package org.myorg.sut

import org.junit.jupiter.api.Test

// Components tested.
//   - Connection from Kinesis Stream to Lambda Listener.
//   - Listener being able to write to Dynamodb.
//  Possibly also but not sure
//    - Trigger lambda consuming from Dynamodb stream.
//    - Trigger sending messages to EventBridge.

class ListenerITest {

    @Test
    fun sendEvents() {

        // send event to kinesis stream running in localstack.
        println("Running integration test")



    }

}