package io.kopipes.core.filters

import io.kopipes.core.UnitOfWork

fun outSkip(uow: UnitOfWork): Boolean {
    return uow.event?.tags?.get("skip") != "true"
}
