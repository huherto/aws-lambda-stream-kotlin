package org.myorg.sut

import aws.sdk.kotlin.runtime.auth.credentials.StaticCredentialsProvider
import aws.sdk.kotlin.services.eventbridge.EventBridgeClient
import aws.sdk.kotlin.services.eventbridge.model.PutEventsRequest
import aws.sdk.kotlin.services.eventbridge.model.PutEventsRequestEntry
import aws.smithy.kotlin.runtime.net.url.Url
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Test



// Components tested.
//   - Send event to event bridge. sut-event-hub-local-bus.
//   - Event bridge will send event to kinesis stream. sut-event-hub-local-s1
//   - sut-control-service-local-listener will read event from kinesis stream.
//   - sut-control-service-local-listener will insert event in DynamoDB. sut-control-service-local-events
//@Testcontainers
class ListenerITest {

    @Test
    fun sendEvents() {
//        val localstack: LocalStackContainer = LocalStackContainer(DockerImageName.parse("localstack/localstack:stable"))
//            .withServices(LocalStackContainer.Service.KINESIS, LocalStackContainer.EnabledService.named("eventbridge"))
//            .withExposedPorts(4566)

        runBlocking<Unit> {
            val eventBridgeClient = createEventBridgeClient(Url.parse("http://localhost:4566"))
            eventBridgeClient.use { eventBridgeClient ->
                val res = eventBridgeClient.putEvents(PutEventsRequest{
                    entries = listOf(
                        PutEventsRequestEntry {
                            eventBusName = "sut-event-hub-local-bus"
                            detail = """{"event":"create","id":"abc123"}"""
                            detailType = "my-event"
                            source = "integration-test"
                        }
                    )
                })
                println("failedEntryCount=${res.failedEntryCount}")
                for (entry in res.entries!!) {
                    println("eventID=${entry.eventId}")
                }
            }
        }
    }

    fun createEventBridgeClient(endpointUrl : Url): EventBridgeClient {

        return EventBridgeClient {
            this.region = "us-east-1"
            this.endpointUrl = endpointUrl
            credentialsProvider =
                StaticCredentialsProvider {
                    this.accessKeyId = "test"
                    this.secretAccessKey = "test"
                }
        }
    }
}