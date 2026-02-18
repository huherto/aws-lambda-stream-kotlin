package io.github.huherto.`aws-lambda-stream`

import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.filter
import kotlin.reflect.KClass

fun <E : Event> Flow<UnitOfWork<E>>.filterEventTypes(vararg klassList: KClass<*>): Flow<UnitOfWork<E>> = filter {
    it.event != null && it.event!!::class in klassList
}
