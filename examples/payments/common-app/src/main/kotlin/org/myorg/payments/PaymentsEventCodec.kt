package org.myorg.payments

import io.kopipes.core.EventCodec
import io.kopipes.core.serialization.KotlinxEventCodec
import io.kopipes.core.serialization.KotlinxSerializationStrategy
import kotlinx.serialization.json.Json

/** Liberal in what it accepts: unknown fields are ignored (the Robustness principle). */
val paymentsJson: Json = KotlinxSerializationStrategy.defaultJson()

/** Codec for every internal event of the payments subsystem. */
object PaymentsEventCodec : EventCodec by KotlinxEventCodec(PaymentsEvent.serializer(), paymentsJson)
