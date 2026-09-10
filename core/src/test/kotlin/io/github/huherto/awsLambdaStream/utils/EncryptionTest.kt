package io.github.huherto.awsLambdaStream.utils

import aws.sdk.kotlin.services.kms.KmsClient
import aws.sdk.kotlin.services.kms.model.DecryptResponse
import aws.sdk.kotlin.services.kms.model.EncryptResponse
import aws.sdk.kotlin.services.kms.model.GenerateDataKeyResponse
import io.github.huherto.awsLambdaStream.EnvelopeEncryptionMetadata
import io.github.huherto.awsLambdaStream.EnvironmentConfig
import io.github.huherto.awsLambdaStream.GlobalRegistry
import io.github.huherto.awsLambdaStream.connectors.KmsClientFactory
import io.github.huherto.awsLambdaStream.connectors.KmsConnector
import io.kotest.matchers.shouldBe
import io.kotest.matchers.shouldNotBe
import io.kotest.matchers.types.shouldBeInstanceOf
import io.mockk.coEvery
import io.mockk.every
import io.mockk.mockk
import kotlinx.coroutines.runBlocking
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.JsonPrimitive
import kotlinx.serialization.json.jsonPrimitive
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test

class EncryptionTest {

    private val kmsClient = mockk<KmsClient>()
    private val kmsClientFactory = mockk<KmsClientFactory>()
    private val plaintextKey = "01234567890123456789012345678901".toByteArray() // 32 bytes for AES-256
    private val ciphertextKey = "encrypted-key".toByteArray()

    @BeforeEach
    fun setup() {
        val config = mockk<EnvironmentConfig>()
        every { config.awsRegion() } returns "us-east-1"
        every { config.endPointUrl() } returns null
        GlobalRegistry.setEnvConfig(config)

        every { kmsClientFactory.getClient(any(), any()) } returns kmsClient
        coEvery { kmsClient.generateDataKey(any()) } returns GenerateDataKeyResponse {
            plaintext = plaintextKey
            ciphertextBlob = ciphertextKey
        }
        coEvery { kmsClient.decrypt(any()) } returns DecryptResponse {
            plaintext = plaintextKey
        }
        coEvery { kmsClient.encrypt(any()) } returns EncryptResponse {
            ciphertextBlob = "encrypted-key-other-region".toByteArray()
        }
        GlobalRegistry.setKmsClientFactory(kmsClientFactory)
    }

    @Test
    fun `should encrypt and decrypt JsonObject fields`() {
        runBlocking {
            val json = JsonObject(mapOf(
                "secret" to JsonPrimitive("hush"),
                "public" to JsonPrimitive("hello")
            ))
            val eem = EnvelopeEncryptionMetadata(
                masterKeyAlias = "alias/test",
                fields = listOf("secret")
            )
            val kms = KmsConnector("test", kmsClientFactory)

            // Encrypt
            val (encrypted, newEem) = EncryptionUtils.encryptJsonObject(json, eem, kms)

            encrypted["secret"]!!.jsonPrimitive.isString shouldBe true
            encrypted["secret"]!!.jsonPrimitive.content shouldNotBe "hush"
            encrypted["public"]!!.jsonPrimitive.content shouldBe "hello"
            newEem.dataKeys shouldNotBe null
            newEem.dataKeys!!.size shouldBe 1

            // Decrypt
            val decrypted = EncryptionUtils.decryptJsonObject(encrypted, newEem, kms)
            decrypted["secret"]!!.jsonPrimitive.content shouldBe "hush"
            decrypted["public"]!!.jsonPrimitive.content shouldBe "hello"
        }
    }

    @Test
    fun `should handle multi-region encryption`() {
        runBlocking {
            val json = JsonObject(mapOf("secret" to JsonPrimitive("hush")))
            val eem = EnvelopeEncryptionMetadata(masterKeyAlias = "alias/test", fields = listOf("secret"))
            val kms = KmsConnector("test", kmsClientFactory)

            val (encrypted, newEem) = EncryptionUtils.encryptJsonObject(json, eem, kms, regions = listOf("us-east-1", "us-west-2"))

            newEem.dataKeys!!.size shouldBe 2
            newEem.dataKeys!!.containsKey("us-east-1") shouldBe true
            newEem.dataKeys!!.containsKey("us-west-2") shouldBe true
        }
    }

    @Test
    fun `should encrypt and decrypt DynamoDB maps`() {
        runBlocking {
            val map = mapOf(
                "secret" to aws.sdk.kotlin.services.dynamodb.model.AttributeValue.S("hush"),
                "public" to aws.sdk.kotlin.services.dynamodb.model.AttributeValue.S("hello")
            )
            val eem = EnvelopeEncryptionMetadata(
                masterKeyAlias = "alias/test",
                fields = listOf("secret")
            )
            val kms = KmsConnector("test", kmsClientFactory)

            // Encrypt
            val (encrypted, newEem) = EncryptionUtils.encryptMap(map, eem, kms)

            encrypted["secret"]!!.shouldBeInstanceOf<aws.sdk.kotlin.services.dynamodb.model.AttributeValue.B>()
            encrypted["public"] shouldBe aws.sdk.kotlin.services.dynamodb.model.AttributeValue.S("hello")

            // Decrypt
            val decrypted = EncryptionUtils.decryptMap(encrypted, newEem, kms)
            decrypted["secret"] shouldBe aws.sdk.kotlin.services.dynamodb.model.AttributeValue.S("hush")
            decrypted["public"] shouldBe aws.sdk.kotlin.services.dynamodb.model.AttributeValue.S("hello")
        }
    }
}
