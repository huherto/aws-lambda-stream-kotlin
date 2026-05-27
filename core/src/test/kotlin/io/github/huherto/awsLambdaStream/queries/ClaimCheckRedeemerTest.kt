package io.github.huherto.awsLambdaStream.queries

import aws.sdk.kotlin.services.s3.S3Client
import aws.sdk.kotlin.services.s3.model.GetObjectRequest
import aws.sdk.kotlin.services.s3.model.GetObjectResponse
import aws.smithy.kotlin.runtime.content.ByteStream
import io.github.huherto.awsLambdaStream.*
import io.github.huherto.awsLambdaStream.connectors.S3ClientFactory
import io.github.huherto.awsLambdaStream.connectors.S3Connector
import io.github.huherto.awsLambdaStream.sinks.EventPublisherInMemory
import io.kotest.matchers.collections.shouldHaveSize
import io.kotest.matchers.shouldBe
import io.mockk.coEvery
import io.mockk.coVerify
import io.mockk.mockk
import io.mockk.spyk
import kotlinx.coroutines.flow.asFlow
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Test

class ClaimCheckRedeemerTest {

    private val envConfig = spyk(EnvironmentConfig())

    private fun faultManager(): FaultManager {
        return FaultManager(
            envConfig = envConfig,
            eventPublisher = EventPublisherInMemory(),
            skipErrorLogging = true,
        )
    }

    @Test
    fun `redeemClaimCheck should fetch claimed event from s3 and replace event`() = runBlocking {
        // Arrange
        val s3Client = mockk<S3Client>()
        val s3ClientFactory = object : S3ClientFactory {
            override fun getClient(pipelineId: String): S3Client = s3Client
        }
        val s3Connector = S3Connector(clientFactory = s3ClientFactory)
        val eventCodec = MyEventCodec()
        val claimedEvent = MyEventA(foo = "claimed-foo", bar = "claimed-bar").apply {
            id = "claimed-event-id"
        }
        val claimCheckEvent = MyEventA().apply {
            raw = ClaimCheck(bucket = "claim-bucket", key = "events/claimed-event.json")
        }
        val originalUow = UnitOfWork(event = claimCheckEvent)
        val redeemer = ClaimCheckRedeemer(
            s3Connector = s3Connector,
            faultManager = faultManager(),
            eventCodec = eventCodec,
        )

        coEvery {
            s3Client.getObject(
                match<GetObjectRequest> {
                    it.bucket == "claim-bucket" && it.key == "events/claimed-event.json"
                },
                any<suspend (GetObjectResponse) -> ByteArray>(),
            )
        } coAnswers {
            secondArg<suspend (GetObjectResponse) -> ByteArray>().invoke(
                GetObjectResponse {
                    body = ByteStream.fromBytes(claimedEvent.encoded().encodeToByteArray())
                }
            )
        }

        // Act
        val results = with(redeemer) {
            listOf(originalUow).asFlow().redeemClaimCheck().toList()
        }

        // Assert
        results.shouldHaveSize(1)
        results[0].event shouldBe claimedEvent
        results[0].s3.getRequest?.bucket shouldBe "claim-bucket"
        results[0].s3.getRequest?.key shouldBe "events/claimed-event.json"
        results[0].s3.getResponseBytes?.decodeToString() shouldBe claimedEvent.encoded()

        coVerify(exactly = 1) {
            s3Client.getObject(
                match<GetObjectRequest> {
                    it.bucket == "claim-bucket" && it.key == "events/claimed-event.json"
                },
                any<suspend (GetObjectResponse) -> ByteArray>(),
            )
        }
    }

    @Test
    fun `redeemClaimCheck should clear transient s3 state when event has no claim check`() = runBlocking {
        // Arrange
        val s3Client = mockk<S3Client>(relaxed = true)
        val s3ClientFactory = object : S3ClientFactory {
            override fun getClient(pipelineId: String): S3Client = s3Client
        }
        val s3Connector = S3Connector(clientFactory = s3ClientFactory)
        val eventCodec = MyEventCodec()
        val originalEvent = MyEventA(foo = "original-foo", bar = "original-bar")
        val originalUow = UnitOfWork(event = originalEvent)
        val redeemer = ClaimCheckRedeemer(
            s3Connector = s3Connector,
            faultManager = faultManager(),
            eventCodec = eventCodec,
        )

        // Act
        val results = with(redeemer) {
            listOf(originalUow).asFlow().redeemClaimCheck().toList()
        }

        // Assert
        results.shouldHaveSize(1)
        results[0].event shouldBe originalEvent
        results[0].s3.getRequest shouldBe null
        results[0].s3.getResponse shouldBe null
        results[0].s3.getResponseText shouldBe null
        results[0].s3.getResponseBytes shouldBe null

        coVerify(exactly = 0) {
            s3Client.getObject(any<GetObjectRequest>(), any<suspend (GetObjectResponse) -> ByteArray>())
        }
    }
}
