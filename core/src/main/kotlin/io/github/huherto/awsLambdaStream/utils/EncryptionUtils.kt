package io.github.huherto.awsLambdaStream.utils

import aws.sdk.kotlin.services.dynamodb.model.AttributeValue
import io.github.huherto.awsLambdaStream.EnvelopeEncryptionMetadata
import io.github.huherto.awsLambdaStream.GlobalRegistry.envConfig
import io.github.huherto.awsLambdaStream.connectors.KmsConnector
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.JsonPrimitive
import kotlinx.serialization.json.jsonPrimitive
import java.security.SecureRandom
import java.util.*
import javax.crypto.Cipher
import javax.crypto.spec.GCMParameterSpec
import javax.crypto.spec.SecretKeySpec
import com.amazonaws.services.lambda.runtime.events.models.dynamodb.AttributeValue as EventAV

/**
 * Utilities for envelope encryption of JSON objects and DynamoDB maps.
 */
object EncryptionUtils {
    private const val ALGORITHM = "AES/GCM/NoPadding"
    private const val TAG_LENGTH_BIT = 128
    private const val IV_LENGTH_BYTE = 12
    private val secureRandom = SecureRandom()

    /**
     * Encrypts specified fields in a [JsonObject].
     */
    suspend fun encryptJsonObject(
        json: JsonObject,
        eem: EnvelopeEncryptionMetadata,
        kms: KmsConnector,
        regions: List<String>? = null
    ): Pair<JsonObject, EnvelopeEncryptionMetadata> {
        val fieldsToEncrypt = eem.fields ?: return json to eem
        if (fieldsToEncrypt.isEmpty()) return json to eem

        val masterKeyAlias = eem.masterKeyAlias ?: throw IllegalArgumentException("masterKeyAlias is required")
        
        // 1. Generate data key in primary region
        val primaryRegion = envConfig().awsRegion()
        val (plaintextKey, primaryCiphertextKey) = kms.generateDataKey(masterKeyAlias, primaryRegion)

        // 2. Encrypt for other regions
        val dataKeys = mutableMapOf<String, String>()
        dataKeys[primaryRegion] = Base64.getEncoder().encodeToString(primaryCiphertextKey)
        
        regions?.filter { it != primaryRegion }?.forEach { region ->
            val ciphertext = kms.encrypt(plaintextKey, masterKeyAlias, region)
            dataKeys[region] = Base64.getEncoder().encodeToString(ciphertext)
        }

        // 3. Encrypt fields
        val secretKey = SecretKeySpec(plaintextKey, "AES")
        val encryptedFields = json.toMutableMap()
        
        fieldsToEncrypt.forEach { field ->
            json[field]?.let { value ->
                val plaintext = when (value) {
                    is JsonPrimitive -> if (value.isString) value.content.toByteArray() else value.toString().toByteArray()
                    else -> value.toString().toByteArray()
                }
                val encrypted = encrypt(plaintext, secretKey)
                encryptedFields[field] = JsonPrimitive(Base64.getEncoder().encodeToString(encrypted))
            }
        }

        val newEem = eem.copy(dataKeys = dataKeys)
        return JsonObject(encryptedFields) to newEem
    }

    /**
     * Decrypts specified fields in a [JsonObject].
     */
    suspend fun decryptJsonObject(
        json: JsonObject,
        eem: EnvelopeEncryptionMetadata,
        kms: KmsConnector
    ): JsonObject {
        val fieldsToDecrypt = eem.fields ?: return json
        if (fieldsToDecrypt.isEmpty()) return json

        val dataKeys = eem.dataKeys ?: throw IllegalArgumentException("dataKeys are missing in eem")
        
        // 1. Decrypt data key
        val currentRegion = envConfig().awsRegion()
        val ciphertextKeyBase64 = dataKeys[currentRegion] 
            ?: dataKeys.values.firstOrNull() 
            ?: throw IllegalArgumentException("No data key found for decryption")
        
        val region = dataKeys.filterValues { it == ciphertextKeyBase64 }.keys.firstOrNull() ?: currentRegion
        
        val ciphertextKey = Base64.getDecoder().decode(ciphertextKeyBase64)
        val plaintextKey = kms.decryptDataKey(ciphertextKey, region)
        val secretKey = SecretKeySpec(plaintextKey, "AES")

        // 2. Decrypt fields
        val decryptedFields = json.toMutableMap()
        fieldsToDecrypt.forEach { field ->
            json[field]?.jsonPrimitive?.content?.let { ciphertextBase64 ->
                val ciphertext = Base64.getDecoder().decode(ciphertextBase64)
                val plaintext = decrypt(ciphertext, secretKey)
                val content = String(plaintext, Charsets.UTF_8)
                
                decryptedFields[field] = try {
                    Json.parseToJsonElement(content)
                } catch (e: Exception) {
                    JsonPrimitive(content)
                }
            }
        }

        return JsonObject(decryptedFields)
    }

    /**
     * Encrypts specified fields in a DynamoDB map.
     */
    suspend fun encryptMap(
        map: Map<String, AttributeValue>,
        eem: EnvelopeEncryptionMetadata,
        kms: KmsConnector,
        regions: List<String>? = null
    ): Pair<Map<String, AttributeValue>, EnvelopeEncryptionMetadata> {
        val fieldsToEncrypt = eem.fields ?: return map to eem
        if (fieldsToEncrypt.isEmpty()) return map to eem

        val masterKeyAlias = eem.masterKeyAlias ?: throw IllegalArgumentException("masterKeyAlias is required")
        
        // 1. Generate data key
        val primaryRegion = envConfig().awsRegion()
        val (plaintextKey, primaryCiphertextKey) = kms.generateDataKey(masterKeyAlias, primaryRegion)

        // 2. Encrypt for other regions
        val dataKeys = mutableMapOf<String, String>()
        dataKeys[primaryRegion] = Base64.getEncoder().encodeToString(primaryCiphertextKey)
        regions?.filter { it != primaryRegion }?.forEach { region ->
            val ciphertext = kms.encrypt(plaintextKey, masterKeyAlias, region)
            dataKeys[region] = Base64.getEncoder().encodeToString(ciphertext)
        }

        // 3. Encrypt fields
        val secretKey = SecretKeySpec(plaintextKey, "AES")
        val encryptedMap = map.toMutableMap()
        
        fieldsToEncrypt.forEach { field ->
            map[field]?.let { value ->
                val plaintext = when (value) {
                    is AttributeValue.S -> value.value.toByteArray()
                    is AttributeValue.N -> value.value.toByteArray()
                    is AttributeValue.B -> value.value
                    else -> value.toString().toByteArray()
                }
                val encrypted = encrypt(plaintext, secretKey)
                encryptedMap[field] = AttributeValue.B(encrypted)
            }
        }

        val newEem = eem.copy(dataKeys = dataKeys)
        return encryptedMap to newEem
    }

    /**
     * Decrypts specified fields in a DynamoDB map.
     */
    suspend fun decryptMap(
        map: Map<String, AttributeValue>,
        eem: EnvelopeEncryptionMetadata,
        kms: KmsConnector
    ): Map<String, AttributeValue> {
        val fieldsToDecrypt = eem.fields ?: return map
        if (fieldsToDecrypt.isEmpty()) return map

        val dataKeys = eem.dataKeys ?: throw IllegalArgumentException("dataKeys are missing in eem")
        
        // 1. Decrypt data key
        val currentRegion = envConfig().awsRegion()
        val ciphertextKeyBase64 = dataKeys[currentRegion] 
            ?: dataKeys.values.firstOrNull() 
            ?: throw IllegalArgumentException("No data key found for decryption")
        
        val region = dataKeys.filterValues { it == ciphertextKeyBase64 }.keys.firstOrNull() ?: currentRegion
        
        val ciphertextKey = Base64.getDecoder().decode(ciphertextKeyBase64)
        val plaintextKey = kms.decryptDataKey(ciphertextKey, region)
        val secretKey = SecretKeySpec(plaintextKey, "AES")

        // 2. Decrypt fields
        val decryptedMap = map.toMutableMap()
        fieldsToDecrypt.forEach { field ->
            map[field]?.let { value ->
                val ciphertext = when (value) {
                    is AttributeValue.B -> value.value
                    is AttributeValue.S -> Base64.getDecoder().decode(value.value)
                    else -> null
                }
                
                ciphertext?.let {
                    val plaintext = decrypt(it, secretKey)
                    val content = String(plaintext, Charsets.UTF_8)
                    // We assume it was a string. If it was a number, AttributeValue.S still works for many purposes
                    // or we could try to detect if it's a number.
                    decryptedMap[field] = AttributeValue.S(content)
                }
            }
        }

        return decryptedMap
    }

    /**
     * Decrypts specified fields in a [RecordImage].
     */
    suspend fun decryptRecordImage(
        image: io.github.huherto.awsLambdaStream.from.RecordImage,
        eem: EnvelopeEncryptionMetadata,
        kms: KmsConnector
    ): io.github.huherto.awsLambdaStream.from.RecordImage {
        val fieldsToDecrypt = eem.fields ?: return image
        if (fieldsToDecrypt.isEmpty()) return image

        val dataKeys = eem.dataKeys ?: throw IllegalArgumentException("dataKeys are missing in eem")

        // 1. Decrypt data key
        val currentRegion = envConfig().awsRegion()
        val ciphertextKeyBase64 = dataKeys[currentRegion]
            ?: dataKeys.values.firstOrNull()
            ?: throw IllegalArgumentException("No data key found for decryption")

        val region = dataKeys.filterValues { it == ciphertextKeyBase64 }.keys.firstOrNull() ?: currentRegion

        val ciphertextKey = Base64.getDecoder().decode(ciphertextKeyBase64)
        val plaintextKey = kms.decryptDataKey(ciphertextKey, region)
        val secretKey = SecretKeySpec(plaintextKey, "AES")

        // 2. Decrypt fields
        val decryptedMap = image.map.toMutableMap()
        fieldsToDecrypt.forEach { field ->
            image.map[field]?.let { value ->
                val ciphertext = when {
                    value.b != null -> value.b.array()
                    value.s != null -> Base64.getDecoder().decode(value.s)
                    else -> null
                }

                ciphertext?.let {
                    val plaintext = decrypt(it, secretKey)
                    val content = String(plaintext, Charsets.UTF_8)
                    decryptedMap[field] = EventAV().apply { s = content }
                }
            }
        }

        return io.github.huherto.awsLambdaStream.from.RecordImage(decryptedMap)
    }

    private fun encrypt(plaintext: ByteArray, secretKey: SecretKeySpec): ByteArray {
        val iv = ByteArray(IV_LENGTH_BYTE)
        secureRandom.nextBytes(iv)
        val cipher = Cipher.getInstance(ALGORITHM)
        cipher.init(Cipher.ENCRYPT_MODE, secretKey, GCMParameterSpec(TAG_LENGTH_BIT, iv))
        val ciphertext = cipher.doFinal(plaintext)
        return iv + ciphertext
    }

    private fun decrypt(encrypted: ByteArray, secretKey: SecretKeySpec): ByteArray {
        if (encrypted.size < IV_LENGTH_BYTE) throw IllegalArgumentException("Invalid encrypted data")
        val iv = encrypted.sliceArray(0 until IV_LENGTH_BYTE)
        val ciphertext = encrypted.sliceArray(IV_LENGTH_BYTE until encrypted.size)
        val cipher = Cipher.getInstance(ALGORITHM)
        cipher.init(Cipher.DECRYPT_MODE, secretKey, GCMParameterSpec(TAG_LENGTH_BIT, iv))
        return cipher.doFinal(ciphertext)
    }
}
