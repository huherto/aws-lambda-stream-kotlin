package io.kopipes.spring

import io.kotest.matchers.shouldBe
import org.junit.jupiter.api.Test
import org.springframework.mock.env.MockEnvironment

class SpringEnvironmentConfigTest {

    @Test
    fun `reads the legacy env var name`() {
        // arrange
        val env = MockEnvironment().withProperty("BUS_NAME", "legacy-bus").withProperty("PUBLISH_BATCH_SIZE", "5")
        val config = SpringEnvironmentConfig(env)

        // act
        val busName = config.busName()
        val batchSize = config.publishBatchSize()

        // assert
        busName shouldBe "legacy-bus"
        batchSize shouldBe 5
    }

    @Test
    fun `kopipes property takes precedence over the legacy env var name`() {
        // arrange
        val env = MockEnvironment()
            .withProperty("BUS_NAME", "legacy-bus")
            .withProperty("kopipes.bus-name", "spring-bus")
        val config = SpringEnvironmentConfig(env)

        // act
        val busName = config.busName()

        // assert
        busName shouldBe "spring-bus"
    }

    @Test
    fun `keeps the defaults of EnvironmentConfig when nothing is set`() {
        // arrange
        val config = SpringEnvironmentConfig(MockEnvironment())

        // act & assert
        config.awsRegion() shouldBe "us-east-1"
        config.streamRetryEnabled() shouldBe false
        config.skip() shouldBe false
        config.tableName() shouldBe null
    }

    @Test
    fun `region falls back to AWS_DEFAULT_REGION and table name to the entity table`() {
        // arrange
        val env = MockEnvironment()
            .withProperty("AWS_DEFAULT_REGION", "eu-west-1")
            .withProperty("kopipes.entity-table-name", "entities")
        val config = SpringEnvironmentConfig(env)

        // act & assert
        config.awsRegion() shouldBe "eu-west-1"
        config.tableName() shouldBe "entities"
    }

    @Test
    fun `parses booleans and numbers like the env var config`() {
        // arrange
        val env = MockEnvironment()
            .withProperty("kopipes.stream-retry-enabled", "true")
            .withProperty("ITEM_LEVEL_RETRY_ENABLED", "yes")
            .withProperty("kopipes.timeout", "1500")
            .withProperty("kopipes.parallel", "not-a-number")
            .withProperty("kopipes.metrics", "pipeline")
        val config = SpringEnvironmentConfig(env)

        // act & assert
        config.streamRetryEnabled() shouldBe true
        config.itemLevelRetryEnabled() shouldBe false
        config.timeout() shouldBe 1500L
        config.parallel() shouldBe null
        config.isMetricEnabled("pipeline") shouldBe true
    }
}
