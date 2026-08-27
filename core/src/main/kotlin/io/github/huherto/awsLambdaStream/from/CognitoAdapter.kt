package io.github.huherto.awsLambdaStream.from

import io.github.huherto.awsLambdaStream.*
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flowOf
import kotlinx.datetime.Clock
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.contentOrNull
import kotlinx.serialization.json.jsonPrimitive
import java.util.*

class CognitoAdapter(private val faultManager: FaultManager = GlobalRegistry.faultManager()) {

    fun fromCognito(event: Any, eventTypePrefix: String = "aws-cognito"): Flow<UnitOfWork> {
        with(faultManager) {
            return flowOf(UnitOfWork(record = event))
                .mapNotFaulty { uow ->
                    val recordElement = uow.record.toJsonElement()
                    val recordMap = recordElement as? JsonObject ?: JsonObject(emptyMap())

                    val triggerSource = recordMap["triggerSource"]?.jsonPrimitive?.contentOrNull ?: ""
                    val userName = recordMap["userName"]?.jsonPrimitive?.contentOrNull
                    val region = recordMap["region"]?.jsonPrimitive?.contentOrNull
                    val userPoolId = recordMap["userPoolId"]?.jsonPrimitive?.contentOrNull
                    val request = recordMap["request"] as? JsonObject

                    val type = "$eventTypePrefix-${toKebabCase(triggerSource)}"

                    val eventObj = CognitoEvent(
                        id = UUID.randomUUID().toString(),
                        type = type,
                        partitionKey = userName,
                        tags = mutableMapOf<String, String>().apply {
                            region?.let { put("region", it) }
                            userPoolId?.let { put("source", it) }
                        },
                        raw = request?.let {
                            JsonRaw(value = it)
                        }
                    )
                    uow.copy(event = eventObj)
                }
        }
    }

    private fun toKebabCase(str: String): String {
        return str.replace(Regex("([a-z])([A-Z])"), "$1-$2")
            .replace(Regex("[^a-zA-Z0-9]"), "-")
            .lowercase()
            .replace(Regex("-+"), "-")
            .trim('-')
    }
}

data class CognitoEvent(
    override val id: String?,
    private val type: String,
    override val partitionKey: String?,
    override val tags: Map<String, String>?,
    override val raw: RawRecord?,
    override val timestamp: Long? = Clock.System.now().toEpochMilliseconds(),
    override val eem: EnvelopeEncryptionMetadata? = null,
    override val triggers: List<EventReference>? = null
) : BaseEvent() {
    override fun eventType(): String = type
}
