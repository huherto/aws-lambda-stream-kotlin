package org.myorg.payments

import io.kopipes.aws.AwsGlobalRegistry
import io.kopipes.aws.flavors.CdcPipeline
import io.kopipes.aws.from.KinesisAdapter
import io.kopipes.core.Event
import io.kopipes.core.JsonRaw
import io.kopipes.core.PipelineAssembler
import io.kopipes.core.UnitOfWork
import io.kopipes.core.filters.EventFilters
import io.kopipes.core.flavors.Pipeline
import io.kopipes.core.sinks.EventPublisher

/**
 * Translates the accounts subsystem's contract into ours, carrying only what the BFF needs.
 *
 * | Consumes (external)        | Produces (internal)                 |
 * |----------------------------|-------------------------------------|
 * | ACCOUNTS.ACCOUNT_OPENED    | ACCOUNT_CHANGED                     |
 * | ACCOUNTS.ACCOUNT_UPDATED   | ACCOUNT_CHANGED                     |
 * | ACCOUNTS.ACCOUNT_CLOSED    | ACCOUNT_CHANGED (`status: closed`)  |
 *
 * Returns `null` for an ACCOUNTS event type we don't know: liberal in what we accept.
 * Throws when a known event lacks what the translation needs: it becomes a fault, not a guess.
 */
fun toAccountChanged(uow: UnitOfWork): Event? {
    val external = uow.event as? AccountsEvent ?: return null
    val status = when (external.type) {
        AccountsEvent.ACCOUNT_OPENED, AccountsEvent.ACCOUNT_UPDATED -> external.account?.status ?: Account.OPEN
        AccountsEvent.ACCOUNT_CLOSED -> Account.CLOSED
        else -> return null
    }
    val account = external.account ?: error("${external.type} ${external.id} has no account")
    val number = account.number ?: error("${external.type} ${external.id} has no account number")
    val customerId = account.holderId ?: account.ownerId
        ?: error("${external.type} ${external.id} has neither holderId nor ownerId")
    val externalId = external.id ?: error("${external.type} for $number has no id")

    return AccountChanged(
        account = Account(
            number = number,
            kind = account.kind ?: Party.CHECKING,
            customerId = customerId,
            status = status,
        ),
        // Derived from the external event's id: a redelivered external event is the same internal event.
        id = "$externalId.account-changed",
        timestamp = external.timestamp ?: error("${external.type} $externalId has no timestamp"),
        partitionKey = number,
        raw = external.original?.let(::JsonRaw),
    )
}

class ListenerContainer(
    val eventPublisher: EventPublisher,
) {
    companion object {
        const val TRANSLATE = "translate"

        fun build(): ListenerContainer = ListenerContainer(eventPublisher = AwsGlobalRegistry.eventPublisher())
    }

    val translatePipeline: Pipeline by lazy {
        CdcPipeline.builder()
            .id(TRANSLATE)
            .eventFilter(EventFilters.regex("^ACCOUNTS\\..+"))
            .toEvent { uow -> toAccountChanged(uow) }
            .eventPublisher(eventPublisher)
            .build()
    }

    val assembler: PipelineAssembler by lazy {
        PipelineAssembler.builder()
            .addPipeline(translatePipeline)
            .build()
    }

    val kinesisAdapter: KinesisAdapter by lazy { KinesisAdapter(eventCodec = AccountsEventCodec) }
}
