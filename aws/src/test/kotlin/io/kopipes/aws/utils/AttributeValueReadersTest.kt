package io.kopipes.aws.utils

import io.kopipes.core.utils.AttributeValueMapReader
import io.kotest.matchers.nulls.shouldBeNull
import io.kotest.matchers.shouldBe
import org.junit.jupiter.api.Test
import aws.sdk.kotlin.services.dynamodb.model.AttributeValue as SdkAV
import com.amazonaws.services.lambda.runtime.events.models.dynamodb.AttributeValue as StreamAV

class AttributeValueReadersTest {

    @Test
    fun `StreamAttributeValueMapReader should read string, number, bool and null fields`() {
        // Arrange
        val map = mapOf(
            "strField" to StreamAV("hello"),
            "doubleField" to StreamAV().withN("12.34"),
            "intField" to StreamAV().withN("100"),
            "longField" to StreamAV().withN("9876543210"),
            "boolFieldTrue" to StreamAV().withBOOL(true),
            "boolFieldFalse" to StreamAV().withBOOL(false),
            "nullField" to StreamAV().withNULL(true),
        )
        val reader: AttributeValueMapReader = StreamAttributeValueMapReader(map)

        // Act & Assert
        reader.getS("strField") shouldBe "hello"
        reader.getDouble("doubleField") shouldBe 12.34
        reader.getInt("intField") shouldBe 100
        reader.getLong("longField") shouldBe 9876543210L
        reader.getBool("boolFieldTrue") shouldBe true
        reader.getBool("boolFieldFalse") shouldBe false
        reader.isNull("nullField") shouldBe true

        reader.getS("missing").shouldBeNull()
        reader.getDouble("missing").shouldBeNull()
        reader.getInt("missing").shouldBeNull()
        reader.getLong("missing").shouldBeNull()
        reader.getBool("missing").shouldBeNull()
        reader.isNull("missing").shouldBeNull()
    }

    @Test
    fun `DynamoDbAttributeValueMapReader should read string, number, bool and null fields`() {
        // Arrange
        val map = mapOf(
            "strField" to SdkAV.S("hello"),
            "doubleField" to SdkAV.N("12.34"),
            "intField" to SdkAV.N("100"),
            "longField" to SdkAV.N("9876543210"),
            "boolFieldTrue" to SdkAV.Bool(true),
            "boolFieldFalse" to SdkAV.Bool(false),
            "nullField" to SdkAV.Null(true),
        )
        val reader: AttributeValueMapReader = DynamoDbAttributeValueMapReader(map)

        // Act & Assert
        reader.getS("strField") shouldBe "hello"
        reader.getDouble("doubleField") shouldBe 12.34
        reader.getInt("intField") shouldBe 100
        reader.getLong("longField") shouldBe 9876543210L
        reader.getBool("boolFieldTrue") shouldBe true
        reader.getBool("boolFieldFalse") shouldBe false
        reader.isNull("nullField") shouldBe true

        reader.getS("missing").shouldBeNull()
        reader.getDouble("missing").shouldBeNull()
        reader.getInt("missing").shouldBeNull()
        reader.getLong("missing").shouldBeNull()
        reader.getBool("missing").shouldBeNull()
        reader.isNull("missing").shouldBeNull()
    }
}
