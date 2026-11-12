package io.kopipes.aws

import io.kopipes.aws.connectors.DynamoDbClientFactory
import io.kopipes.aws.sinks.DynamoDbEventsMicrostore
import io.kopipes.aws.sinks.EventBridgePublisher
import io.kopipes.core.RAW_CLAIM_CHECK
import io.kopipes.core.RawRecord
import io.kopipes.core.sinks.EventPublisherInMemory
import io.kotest.matchers.shouldBe
import io.kotest.matchers.types.shouldBeInstanceOf
import io.kotest.matchers.types.shouldBeSameInstanceAs
import io.mockk.every
import io.mockk.mockk
import io.mockk.spyk
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonPrimitive
import kotlinx.serialization.json.jsonObject
import org.junit.jupiter.api.Test

class AwsKopipesContextTest {

    private fun envConfig(): AwsEnvironmentConfig {
        val config = spyk(AwsEnvironmentConfig())
        every { config.busName() } returns "the-bus"
        every { config.busSource() } returns "the-source"
        every { config.awsRegion() } returns "eu-west-1"
        every { config.region() } returns "eu-west-1"
        every { config.stage() } returns "dev"
        return config
    }

    @Test
    fun `default publisher is EventBridge configured from the env config`() {
        // arrange
        val context = AwsKopipesContext(envConfig = envConfig())

        // act
        val publisher = context.eventPublisher

        // assert
        val eventBridge = publisher.shouldBeInstanceOf<EventBridgePublisher>()
        eventBridge.busName shouldBe "the-bus"
        eventBridge.source shouldBe "the-source"
        eventBridge.clientFactory shouldBeSameInstanceAs context.eventBridgeClientFactory
        eventBridge.tags.region shouldBe "eu-west-1"
        eventBridge.tags.stage shouldBe "dev"
        context.faultManager.publisher() shouldBeSameInstanceAs publisher
    }

    @Test
    fun `replacing the publisher rewires the default fault manager`() {
        // arrange
        val publisher = EventPublisherInMemory()

        // act
        val context = AwsKopipesContext(envConfig = envConfig(), eventPublisher = publisher)

        // assert
        context.faultManager.publisher() shouldBeSameInstanceAs publisher
    }

    @Test
    fun `components built from the context use its collaborators`() {
        // arrange
        val clientFactory = mockk<DynamoDbClientFactory>()
        val context = AwsKopipesContext(envConfig = envConfig(), dynamoDbClientFactory = clientFactory)

        // act
        val microstore = DynamoDbEventsMicrostore(context)

        // assert
        microstore.shouldBeInstanceOf<DynamoDbEventsMicrostore>()
        context.kmsConnectors.region shouldBe "eu-west-1"
    }

    @Test
    fun `non-sdk errors are not aws-retryable`() {
        // arrange
        val bug = IllegalStateException("bug")

        // act & assert
        isAwsRetryable(bug) shouldBe false
    }

    @Test
    fun `aws raw record types are registered without any set-up`() {
        // arrange
        val raw: RawRecord = ClaimCheck(bucket = "b", key = "k")

        // act
        val json = Json.encodeToString(RawRecord.serializer(), raw)

        // assert
        Json.parseToJsonElement(json).jsonObject["type"] shouldBe JsonPrimitive(RAW_CLAIM_CHECK)
        Json.decodeFromString(RawRecord.serializer(), json) shouldBe raw
    }
}
