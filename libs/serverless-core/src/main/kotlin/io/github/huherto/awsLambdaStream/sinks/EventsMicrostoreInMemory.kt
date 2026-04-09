package io.github.huherto.awsLambdaStream.sinks

import aws.sdk.kotlin.services.dynamodb.model.AttributeValue
import aws.sdk.kotlin.services.dynamodb.model.PutItemResponse
import aws.sdk.kotlin.services.dynamodb.model.QueryResponse
import io.github.huherto.awsLambdaStream.FaultManager
import io.github.huherto.awsLambdaStream.UnitOfWork
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.buffer
import kotlinx.coroutines.flow.mapNotNull

class EventsMicrostoreInMemory(
    faultManager: FaultManager,
) : BaseEventsMicrostore(faultManager) {

    private val uowMap: MutableMap<String, UnitOfWork> = mutableMapOf()
    private val itemsMap: MutableMap<String, MutableList<Map<String, AttributeValue>>> = mutableMapOf()

    fun reset() {
        uowMap.clear()
        itemsMap.clear()
    }

    fun saveUowMap(): Map<String, UnitOfWork> = uowMap.toMap()

    fun getItemsMap(): Map<String, List<Map<String, AttributeValue>>> = itemsMap.toMap()

    override fun save(flow: Flow<UnitOfWork>): Flow<UnitOfWork> {
        with(faultManager) {
            // We reuse the `putRequest` step from the parent class, intercepting the request locally.
            return flow.mapNotFaulty { uow -> putRequest(uow) }
                .buffer()
                .mapNotNull { uow -> faulty(uow) { memoryPut(uow) } }
        }
    }

    private fun memoryPut(uow: UnitOfWork): UnitOfWork {
        val request = uow.putRequest
        if (request != null) {
            val item = request.item
            if (item != null) {
                val pk = (item["pk"] as? AttributeValue.S)?.value
                if (pk != null) {
                    val list = itemsMap.getOrPut(pk) { mutableListOf() }
                    list.add(item)
                }
            }

            // Replicating previously existing behavior.
            uowMap[uow.event?.id ?: "unknown id"] = uow
            return uow.copy(putResponse = PutItemResponse { })
        }
        return uow
    }

    override fun queryByPk(flow: Flow<UnitOfWork>): Flow<UnitOfWork> {
        with(faultManager) {
            // Reusing parent logic `toQueryRequest` & `toCorrelated`
            return flow.mapNotFaulty { uow -> toQueryRequest(uow) }
                .buffer()
                .mapNotNull { uow -> faulty(uow) { memoryQuery(uow) } }
                .mapNotNull { uow -> faulty(uow) { toCorrelated(uow) } }
        }
    }

    private fun memoryQuery(uow: UnitOfWork): UnitOfWork {
        val request = uow.queryRequest
        if (request != null) {
            val expressionAttributeValues = request.expressionAttributeValues
            val pk = (expressionAttributeValues?.get(":pk") as? AttributeValue.S)?.value

            val items = if (pk != null) itemsMap[pk] ?: emptyList() else emptyList()

            val response = QueryResponse {
                this.items = items
                this.count = items.size
            }
            return uow.copy(queryResponse = response)
        }
        return uow
    }
}