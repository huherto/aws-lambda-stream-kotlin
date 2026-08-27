package io.github.huherto.awsLambdaStream

import io.kotest.matchers.shouldBe
import io.kotest.matchers.string.shouldContain
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.JsonPrimitive
import org.junit.jupiter.api.Test

class SafeLoggerTest {

    class SimplePojo(val name: String, val age: Int)

    class PojoWithGetters {
        fun getName() = "John"
        fun getAge() = 30
        fun isHappy() = true
    }

    @Test
    fun `should serialize simple pojo`() {
        val pojo = SimplePojo("Bob", 25)
        val json = SafeLogger.toJson(pojo)
        
        val element = Json.parseToJsonElement(json) as JsonObject
        element["name"] shouldBe JsonPrimitive("Bob")
        element["age"] shouldBe JsonPrimitive(25)
    }

    @Test
    fun `should serialize pojo with getters`() {
        val pojo = PojoWithGetters()
        val json = SafeLogger.toJson(pojo)
        
        val element = Json.parseToJsonElement(json) as JsonObject
        element["name"] shouldBe JsonPrimitive("John")
        element["age"] shouldBe JsonPrimitive(30)
        element["happy"] shouldBe JsonPrimitive(true)
    }

    @Test
    fun `should handle circular references`() {
        val a = mutableMapOf<String, Any>()
        a["self"] = a
        
        val json = SafeLogger.toJson(a)
        json shouldContain "[Circular Reference to LinkedHashMap]"
    }

    @Test
    fun `should never fail even for null`() {
        SafeLogger.toJson(null) shouldBe "null"
    }
}
