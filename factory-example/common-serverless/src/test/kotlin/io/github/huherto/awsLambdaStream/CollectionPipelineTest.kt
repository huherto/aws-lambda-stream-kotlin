package io.github.huherto.awsLambdaStream

import aws.sdk.kotlin.services.dynamodb.DynamoDbClient
import aws.sdk.kotlin.services.dynamodb.model.AttributeValue
import aws.sdk.kotlin.services.dynamodb.model.PutItemResponse
import io.mockk.coEvery
import io.mockk.every
import io.mockk.mockk
import io.mockk.spyk
import kotlinx.coroutines.flow.flowOf
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test

class CollectionPipelineTest {

    fun myEventA() = MyEventA(foo = "foo-value", bar = "bar-value").apply {
        id = "test-event-id"
        timestamp = 1672531200000 // 2023-01-01T00:00:00.000Z
        partitionKey = "pk-test"
    }

    fun mockEnvConfig()  : EnvironmentConfig {
        val envConfig = mockk<EnvironmentConfig>()
        every { envConfig.awsRegion() } returns "us-east-1"
        every { envConfig.tableName() } returns "events"
        return envConfig
    }

    fun mockDynamoDbClient() : DynamoDbClient {
        val dynamoDbClient = mockk<DynamoDbClient>()
        coEvery { dynamoDbClient.putItem(any()) } coAnswers { mockk<PutItemResponse>() }
        return dynamoDbClient
    }


    @Test
    fun `test defaultPutRequest generates correct put request with raw event`() {
        
        // Given
        val envConfig = spyk<EnvironmentConfig>()
        every { envConfig.awsRegion() } returns "us-east-1"
        every { envConfig.tableName() } returns "events"
        val ttlDaysTest = 5
        val pipeline = CollectPipeline.Builder("collection-pipeline-test")
            .includeRaw(true)
            .expire("test-expire")
            .ttlDays(ttlDaysTest)
            .envConfig(envConfig)
            .build()

        val event = myEventA()
        val uow = UnitOfWork(event = event, key = "test-correlation-key")

        // When
        val resultUow = pipeline.defaultPutRequest(uow)

        // Then
        assertNotNull(resultUow.putRequest)
        val itemValues = resultUow.putRequest!!.item

        assertNotNull(itemValues)
        assertEquals(AttributeValue.S("test-event-id"), itemValues?.get("pk"))
        assertEquals(AttributeValue.S("EVENT"), itemValues?.get("sk"))
        assertEquals(AttributeValue.S("EVENT"), itemValues?.get("discriminator"))
        assertEquals(AttributeValue.N("1672531200000"), itemValues?.get("timestamp"))

        // TTL logic check: timestamp(1672531200000) / 1000 + 5 days(432000 secs) = 1672963200
        val expectedTtl = (1672531200000 / 1000) + (ttlDaysTest * 24 * 60 * 60L)
        assertEquals(AttributeValue.N(expectedTtl.toString()), itemValues?.get("ttl"))

        assertEquals(AttributeValue.S("test-expire"), itemValues?.get("expire"))
        assertEquals(AttributeValue.S("test-correlation-key"), itemValues?.get("data"))
    }

    @Test
    fun `test defaultPutRequest without raw event throws exception`() {
        // Given
        val pipeline = CollectPipeline.Builder("collection-pipeline-test")
            .includeRaw(false) // This will trigger omitRaw()
            .build()

        val event = myEventA()
        val uow = UnitOfWork(event = event)

        // When and Then
        val exception = assertThrows(RuntimeException::class.java) {
            pipeline.defaultPutRequest(uow)
        }
        
        assertEquals("Not implemented yet", exception.message)
    }

    @Test
    fun `test builder configures pipeline with custom values correctly`() = runBlocking {
        // Arrange & Act
        val pipeline = CollectPipeline.Builder("collection-pipeline-test")
            .bufferCapacity(42)
            .ttlDays(10)
            .includeRaw(true)
            .correlationKey { "custom-key" }
            .envConfig(mockEnvConfig())
            .dynamoDbClient(mockDynamoDbClient())
            .build()

        // Assert
        assertNotNull(pipeline)

        val faultManager = FaultManager.instance
        System.out.println(pipeline)
        val uowFlow = flowOf(UnitOfWork(event = myEventA()))
        pipeline.connect(faultManager, uowFlow).collect { uow ->
            System.out.println(uow)
        }
        System.out.println("Done")
    }

    @Test
    fun `test collect executes without crashing`() = runBlocking {
        // Arrange
        val pipeline = CollectPipeline.Builder("collection-pipeline-test").build()
        val uowFlow = flowOf(UnitOfWork(event = myEventA()))
        val faultManager = FaultManager.instance

        // Act & Assert
        assertDoesNotThrow {
            val flow = pipeline.connect(faultManager, uowFlow)
            assertNotNull(flow)
        }
    }
}