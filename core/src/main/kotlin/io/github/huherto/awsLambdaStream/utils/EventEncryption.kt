package io.github.huherto.awsLambdaStream.utils

import aws.sdk.kotlin.services.dynamodb.model.AttributeValue
import io.github.huherto.awsLambdaStream.*
import io.github.huherto.awsLambdaStream.connectors.KmsConnector
import io.github.huherto.awsLambdaStream.faults.FaultManager
import io.github.huherto.awsLambdaStream.from.RecordPair
import kotlinx.coroutines.flow.Flow
import kotlinx.serialization.InternalSerializationApi
import kotlinx.serialization.KSerializer
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.serializerOrNull

/**
 * Options for event encryption.
 */
data class EncryptionOptions(
    val eem: EnvelopeEncryptionMetadata? = null,
    val regions: List<String>? = null,
    val kmsConnector: KmsConnector? = null
)

/**
 * Pipeline steps for encryption and decryption.
 */
@OptIn(InternalSerializationApi::class)
object EventEncryption {

    /**
     * Flow operator that encrypts the event in each [UnitOfWork].
     */
    fun encryptEvent(options: EncryptionOptions, fm: FaultManager): (Flow<UnitOfWork>) -> Flow<UnitOfWork> = { flow ->
        with(fm) {
            flow.mapNotFaulty { uow ->
                val eem = options.eem ?: return@mapNotFaulty uow
                val kms = options.kmsConnector ?: KmsConnector(uow.pipeline?.id ?: "default", GlobalRegistry.kmsClientFactory())

                val event = uow.event!!
                val jsonEvent = when (event) {
                    is JsonEvent -> event
                    else -> {
                        val serializer = event::class.serializerOrNull() ?: throw IllegalStateException("Event of type ${event::class} is not serializable")
                        @Suppress("UNCHECKED_CAST")
                        JsonEvent(Json.encodeToJsonElement(serializer as KSerializer<Any>, event).jsonObject)
                    }
                }

                val (encryptedJson, newEem) = EncryptionUtils.encryptJsonObject(
                    jsonEvent.json,
                    eem,
                    kms,
                    options.regions
                )

                uow.copy(
                    event = JsonEvent(encryptedJson).copyEvent(eem = newEem)
                )
            }
        }
    }

    /**
     * Flow operator that decrypts the event in each [UnitOfWork].
     */
    fun decryptEvent(fm: FaultManager): (Flow<UnitOfWork>) -> Flow<UnitOfWork> = { flow ->
        with(fm) {
            flow.mapNotFaulty { uow ->
                val eem = uow.event?.eem ?: return@mapNotFaulty uow
                val kms = KmsConnector(uow.pipeline?.id ?: "default", GlobalRegistry.kmsClientFactory())

                val event = uow.event!!
                val jsonEvent = when (event) {
                    is JsonEvent -> event
                    else -> {
                        val serializer = event::class.serializerOrNull() ?: throw IllegalStateException("Event of type ${event::class} is not serializable")
                        @Suppress("UNCHECKED_CAST")
                        JsonEvent(Json.encodeToJsonElement(serializer as KSerializer<Any>, event).jsonObject)
                    }
                }

                val decryptedJson = EncryptionUtils.decryptJsonObject(
                    jsonEvent.json,
                    eem,
                    kms
                )

                uow.copy(
                    event = JsonEvent(decryptedJson).copyEvent(eem = eem),
                    undecryptedEvent = uow.event
                )
            }
        }
    }

    /**
     * Flow operator that decrypts change events (DynamoDB streams).
     */
    fun decryptChangeEvent(fm: FaultManager): (Flow<UnitOfWork>) -> Flow<UnitOfWork> = { flow ->
        with(fm) {
            flow.mapNotFaulty { uow ->
                val event = uow.event
                val eemMetadata = event?.eem ?: return@mapNotFaulty uow
                val recordPair = event.raw as? RecordPair ?: return@mapNotFaulty uow

                val kms = KmsConnector(uow.pipeline?.id ?: "default", GlobalRegistry.kmsClientFactory())

                val newImage = recordPair.new?.let {
                    EncryptionUtils.decryptRecordImage(it, eemMetadata, kms)
                }
                val oldImage = recordPair.old?.let {
                    EncryptionUtils.decryptRecordImage(it, eemMetadata, kms)
                }

                val newRaw = when (val raw = event.raw) {
                    is DynamodbRaw -> ImagesRaw(newImage, oldImage)
                    is ImagesRaw -> raw.copy(new = newImage, old = oldImage)
                    else -> raw
                }

                uow.copy(
                    event = event.copyEvent(raw = newRaw),
                    undecryptedEvent = uow.event
                )
            }
        }
    }

    /**
     * Manual encryption of data.
     */
    suspend fun encryptData(data: Map<String, AttributeValue>, options: EncryptionOptions): Map<String, AttributeValue> {
        val eem = options.eem ?: return data
        val kms = options.kmsConnector ?: KmsConnector("manual", GlobalRegistry.kmsClientFactory())
        val (encrypted, newEem) = EncryptionUtils.encryptMap(data, eem, kms, options.regions)
        
        val result = encrypted.toMutableMap()
        result["eem"] = AttributeValue.M(
             buildMap {
                 newEem.masterKeyAlias?.let { put("masterKeyAlias", AttributeValue.S(it)) }
                 newEem.dataKeys?.let { keys -> 
                     put("dataKeys", AttributeValue.M(keys.mapValues { AttributeValue.S(it.value) }))
                 }
                 newEem.fields?.let { fields -> 
                     put("fields", AttributeValue.L(fields.map { AttributeValue.S(it) }))
                 }
                 newEem.algorithm?.let { put("algorithm", AttributeValue.S(it)) }
             }
        )
        return result
    }

    /**
     * Manual decryption of data.
     */
    suspend fun decryptData(data: Map<String, AttributeValue>, kmsConnector: KmsConnector? = null): Map<String, AttributeValue> {
        val eemValue = data["eem"] ?: return data
        val eemMap = (eemValue as? AttributeValue.M)?.value ?: return data
        
        val eem = EnvelopeEncryptionMetadata(
            masterKeyAlias = (eemMap["masterKeyAlias"] as? AttributeValue.S)?.value,
            dataKeys = (eemMap["dataKeys"] as? AttributeValue.M)?.value?.mapValues { (it.value as? AttributeValue.S)?.value ?: "" },
            fields = (eemMap["fields"] as? AttributeValue.L)?.value?.map { (it as? AttributeValue.S)?.value ?: "" },
            algorithm = (eemMap["algorithm"] as? AttributeValue.S)?.value
        )
        
        val kms = kmsConnector ?: KmsConnector("manual", GlobalRegistry.kmsClientFactory())
        return EncryptionUtils.decryptMap(data.filterKeys { it != "eem" }, eem, kms)
    }
}
