package io.github.huherto.awsLambdaStream.sinks

import aws.sdk.kotlin.services.dynamodb.model.AttributeValue
import io.kotest.matchers.maps.shouldContainExactly
import io.kotest.matchers.shouldBe
import org.junit.jupiter.api.Test

class DynamoDbUpdateExpressionTest {

    @Test
    fun `updateExpression should create SET REMOVE ADD and DELETE clauses`() {
        // Arrange
        val nameValue = AttributeValue.S("John")
        val counterValue = AttributeValue.N("1")
        val tagValue = AttributeValue.Ss(listOf("archived"))
        val item = mapOf(
            "name" to DynamoDbUpdateValue.Set(nameValue),
            "obsolete" to DynamoDbUpdateValue.Remove,
            "counter" to DynamoDbUpdateValue.Add(counterValue),
            "tags" to DynamoDbUpdateValue.Delete(tagValue),
        )

        // Act
        val result = updateExpression(item)

        // Assert
        result.expressionAttributeNames.shouldContainExactly(
            mapOf(
                "#name" to "name",
                "#obsolete" to "obsolete",
                "#counter" to "counter",
                "#tags" to "tags",
            )
        )
        result.expressionAttributeValues.shouldContainExactly(
            mapOf(
                ":name" to nameValue,
                ":counter" to counterValue,
                ":tags" to tagValue,
            )
        )
        result.updateExpression shouldBe "SET #name = :name REMOVE #obsolete ADD #counter :counter DELETE #tags :tags"
        result.returnValues shouldBe "ALL_NEW"
    }

    @Test
    fun `updateExpression should group similar operations together in input order`() {
        // Arrange
        val firstSetValue = AttributeValue.S("first")
        val firstAddValue = AttributeValue.N("1")
        val secondSetValue = AttributeValue.S("second")
        val secondAddValue = AttributeValue.N("2")
        val item = mapOf(
            "firstSet" to DynamoDbUpdateValue.Set(firstSetValue),
            "firstAdd" to DynamoDbUpdateValue.Add(firstAddValue),
            "firstRemove" to DynamoDbUpdateValue.Remove,
            "secondSet" to DynamoDbUpdateValue.Set(secondSetValue),
            "firstDelete" to DynamoDbUpdateValue.Delete(AttributeValue.Ss(listOf("old"))),
            "secondAdd" to DynamoDbUpdateValue.Add(secondAddValue),
            "secondRemove" to DynamoDbUpdateValue.Remove,
        )

        // Act
        val result = updateExpression(item)

        // Assert
        result.updateExpression shouldBe
            "SET #firstSet = :firstSet, #secondSet = :secondSet " +
            "REMOVE #firstRemove, #secondRemove " +
            "ADD #firstAdd :firstAdd, #secondAdd :secondAdd " +
            "DELETE #firstDelete :firstDelete"
    }

    @Test
    fun `updateExpression should encode invalid alias characters`() {
        // Arrange
        val value = AttributeValue.S("value")
        val item = mapOf(
            "profile.name" to DynamoDbUpdateValue.Set(value),
            "status-code" to DynamoDbUpdateValue.Remove,
            "snake_case" to DynamoDbUpdateValue.Set(value),
            "space key" to DynamoDbUpdateValue.Add(AttributeValue.N("1")),
        )

        // Act
        val result = updateExpression(item)

        // Assert
        result.expressionAttributeNames.shouldContainExactly(
            mapOf(
                "#profile_x2e_name" to "profile.name",
                "#status_x2d_code" to "status-code",
                "#snake_case" to "snake_case",
                "#space_x20_key" to "space key",
            )
        )
        result.expressionAttributeValues.shouldContainExactly(
            mapOf(
                ":profile_x2e_name" to value,
                ":snake_case" to value,
                ":space_x20_key" to AttributeValue.N("1"),
            )
        )
        result.updateExpression shouldBe
            "SET #profile_x2e_name = :profile_x2e_name, #snake_case = :snake_case " +
            "REMOVE #status_x2d_code " +
            "ADD #space_x20_key :space_x20_key"
    }

    @Test
    fun `updateExpression should return empty expression for empty item`() {
        // Arrange
        val item = emptyMap<String, DynamoDbUpdateValue>()

        // Act
        val result = updateExpression(item)

        // Assert
        result.expressionAttributeNames shouldBe emptyMap()
        result.expressionAttributeValues shouldBe emptyMap()
        result.updateExpression shouldBe ""
        result.returnValues shouldBe "ALL_NEW"
    }

    @Test
    fun `timestampCondition should create timestamp condition using default and custom field names`() {
        // Arrange
        val customFieldName = "updatedAt"

        // Act
        val defaultResult = timestampCondition()
        val customResult = timestampCondition(customFieldName)

        // Assert
        defaultResult.shouldContainExactly(
            mapOf(
                "ConditionExpression" to "attribute_not_exists(#timestamp) OR #timestamp < :timestamp",
            )
        )
        customResult.shouldContainExactly(
            mapOf(
                "ConditionExpression" to "attribute_not_exists(#updatedAt) OR #updatedAt < :updatedAt",
            )
        )
    }

    @Test
    fun `pkCondition should create partition key condition using default and custom field names`() {
        // Arrange
        val customFieldName = "aggregateId"

        // Act
        val defaultResult = pkCondition()
        val customResult = pkCondition(customFieldName)

        // Assert
        defaultResult.shouldContainExactly(
            mapOf(
                "ConditionExpression" to "attribute_not_exists(pk)",
            )
        )
        customResult.shouldContainExactly(
            mapOf(
                "ConditionExpression" to "attribute_not_exists(aggregateId)",
            )
        )
    }
}