package io.github.huherto.awsLambdaStream.connectors

import aws.sdk.kotlin.runtime.auth.credentials.EnvironmentCredentialsProvider
import aws.sdk.kotlin.services.kms.KmsClient
import aws.sdk.kotlin.services.kms.model.DataKeySpec
import aws.sdk.kotlin.services.kms.model.DecryptRequest
import aws.sdk.kotlin.services.kms.model.EncryptRequest
import aws.sdk.kotlin.services.kms.model.GenerateDataKeyRequest
import aws.smithy.kotlin.runtime.net.url.Url
import io.github.huherto.awsLambdaStream.GlobalRegistry.envConfig
import java.util.concurrent.ConcurrentHashMap

/** Options for [KmsConnector]. */
data class KmsConnectorOptions(
    val keyAlias: String? = null
)

/** Factory for [KmsClient]. */
interface KmsClientFactory {
    fun getClient(pipelineId: String, region: String? = null): KmsClient
}

/** Default implementation for [KmsClientFactory]. */
class DefaultKmsClientFactory : KmsClientFactory {
    private val clients = ConcurrentHashMap<String, KmsClient>()

    override fun getClient(pipelineId: String, region: String?): KmsClient {
        val effectiveRegion = region ?: envConfig().awsRegion()
        val key = "$pipelineId-$effectiveRegion"
        return clients.computeIfAbsent(key) {
            val endpointUrl = envConfig().endPointUrl()?.ifEmpty { null }
            KmsClient {
                this.region = effectiveRegion
                this.credentialsProvider = EnvironmentCredentialsProvider()
                endpointUrl?.let { this.endpointUrl = Url.parse(it) }
            }
        }
    }
}

/** Connector for AWS KMS. */
class KmsConnector(
    private val pipelineId: String,
    private val clientFactory: KmsClientFactory
) {
    suspend fun generateDataKey(keyAlias: String, region: String? = null): Pair<ByteArray, ByteArray> {
        val client = clientFactory.getClient(pipelineId, region)
        val request = GenerateDataKeyRequest {
            keyId = keyAlias
            keySpec = DataKeySpec.Aes256
        }
        val response = client.generateDataKey(request)
        val plaintext = response.plaintext ?: throw IllegalStateException("No plaintext data key returned")
        val ciphertext = response.ciphertextBlob ?: throw IllegalStateException("No ciphertext data key returned")
        return plaintext to ciphertext
    }

    suspend fun decryptDataKey(ciphertext: ByteArray, region: String? = null): ByteArray {
        val client = clientFactory.getClient(pipelineId, region)
        val request = DecryptRequest {
            ciphertextBlob = ciphertext
        }
        val response = client.decrypt(request)
        return response.plaintext ?: throw IllegalStateException("No plaintext data key returned from decryption")
    }

    suspend fun encrypt(plaintext: ByteArray, keyId: String, region: String? = null): ByteArray {
        val client = clientFactory.getClient(pipelineId, region)
        val request = EncryptRequest {
            this.keyId = keyId
            this.plaintext = plaintext
        }
        val response = client.encrypt(request)
        return response.ciphertextBlob ?: throw IllegalStateException("No ciphertext returned from encryption")
    }
}
