package org.myorg.sut

import io.kotest.matchers.nulls.shouldNotBeNull
import io.kotest.matchers.shouldBe
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class RegionalHealthCheckITest {

    private val awsRegion = "us-east-1"
    private val tracerTableName = "sut-regional-health-check-local-tracer"
    private val checkHealthApiFacade = CheckHealthApiFacade()
    private val awsFacade = AwsFacade(entityTable = tracerTableName)

    @Test
    fun `check endpoint saves a trace record in dynamodb tracer entity table`(): Unit = runBlocking {
        // Arrange

        // Act
        val response = checkHealthApiFacade.check()

        // Assert
        val savedRecord = awsFacade.findEntityByPK(awsRegion) { items ->
            items?.firstOrNull { item ->
                item["sk"]?.asS() == response.timestamp.toString()
            }
        }

        savedRecord.shouldNotBeNull()
        savedRecord["pk"]?.asS() shouldBe awsRegion
        savedRecord["sk"]?.asS() shouldBe response.timestamp.toString()
        savedRecord["status"]?.asS() shouldBe "STARTED"
        savedRecord["discriminator"]?.asS() shouldBe "trace"
        savedRecord["awsregion"]?.asS() shouldBe awsRegion
        savedRecord["timestamp"]?.asN().shouldNotBeNull()
        savedRecord["ttl"]?.asN().shouldNotBeNull()
    }

    @AfterAll
    fun tearDownAll() {
        awsFacade.closeAll()
    }
}