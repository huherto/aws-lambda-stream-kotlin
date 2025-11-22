package org.myorg.sut

import com.amazonaws.services.lambda.runtime.events.KinesisEvent
import kotlinx.serialization.json.Json
import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets
import java.util.stream.Stream

open class KinesisAdapter<T : Thing> {

    inline fun <reified T : Thing> fromKinesis(kinesisEvent: KinesisEvent): Stream<UnitOfWork<T>> {
        return kinesisEvent.records.map { record ->
            UnitOfWork<T>().apply {
                this.record = record
            }
        }.stream().map { uow ->
            // val payload = StandardCharsets.UTF_8.decode(uow.record?.kinesis?.data).toString()
            val payload = uow.record?.kinesis?.data
            val event: T = decodePayload(payload)
            if (event.id().isNullOrEmpty()) {
                if (uow.record != null) {
                    event.setId(uow.record?.eventID)
                }
            }
            uow
        }.map { uow ->
            // TODO: call claim_check processing
            uow;
        }
    }

    private fun utf8Decode(bb : ByteBuffer?) : String {
        return StandardCharsets.UTF_8.decode(bb).toString()
    }

    private inline fun <reified T> parseJson(str : String) : T {
        return Json.decodeFromString<T>(str)
    }

    private inline fun <reified T> decodePayload(payload : ByteBuffer?) : T {
        return parseJson<T>(utf8Decode(payload))
    }
}