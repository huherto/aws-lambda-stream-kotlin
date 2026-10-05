package io.kopipes.core.filters

import io.kopipes.core.GlobalRegistry.envConfig
import io.kopipes.core.UnitOfWork

fun skipTag(): Map<String, Boolean?> {
    return mapOf(
        "skip" to envConfig().skip(),
    )
}

fun outSkip(uow: UnitOfWork): Boolean {
    return uow.event?.tags?.get("skip") != "true"
}