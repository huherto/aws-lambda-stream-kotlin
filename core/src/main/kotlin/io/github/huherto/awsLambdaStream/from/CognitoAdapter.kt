package io.github.huherto.awsLambdaStream.from

import com.fasterxml.jackson.module.kotlin.convertValue
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import io.github.huherto.awsLambdaStream.*
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flowOf
import kotlinx.datetime.Clock
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonElement
import java.util.*

class CognitoAdapter(private val faultManager: FaultManager) {
    private val mapper = jacksonObjectMapper()

    fun fromCognito(event: Any, eventTypePrefix: String = "aws-cognito"): Flow<UnitOfWork> {
        with(faultManager) {
            return flowOf(UnitOfWork(record = event))
                .mapNotFaulty { uow ->
                    val recordMap: Map<String, Any?> = mapper.convertValue(uow.record!!)

                    val triggerSource = recordMap["triggerSource"] as? String ?: ""
                    val userName = recordMap["userName"] as? String
                    val region = recordMap["region"] as? String
                    val userPoolId = recordMap["userPoolId"] as? String
                    val request = recordMap["request"] as? Map<String, Any?>

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
                            val jsonString = mapper.writeValueAsString(it)
                            JsonRaw(value = Json.decodeFromString<JsonElement>(jsonString))
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
    override val eem: Any? = null,
    override val triggers: List<EventReference>? = null
) : BaseEvent() {
    override fun eventType(): String = type
}
