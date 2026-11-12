package io.kopipes.core.serialization

import io.kopipes.core.UnitOfWork
import io.kopipes.core.serialization.snapshots.UnitOfWorkSnapshot
import io.kopipes.core.serialization.snapshots.UnitOfWorkSnapshotter
import kotlinx.serialization.KSerializer
import kotlinx.serialization.SerializationException
import kotlinx.serialization.builtins.nullable
import kotlinx.serialization.descriptors.SerialDescriptor
import kotlinx.serialization.encoding.Decoder
import kotlinx.serialization.encoding.Encoder

/** One-way serializer writing a [UnitOfWork] as the snapshot [snapshotter] produces, for diagnostics. */
class UnitOfWorkSnapshotSerializer(private val snapshotter: UnitOfWorkSnapshotter) : KSerializer<UnitOfWork?> {
    private val surrogateSerializer = UnitOfWorkSnapshot.serializer().nullable

    override val descriptor: SerialDescriptor =
        surrogateSerializer.descriptor

    override fun serialize(encoder: Encoder, value: UnitOfWork?) {
        encoder.encodeSerializableValue(
            surrogateSerializer,
            value?.let { snapshotter.snapshot(it) },
        )
    }

    override fun deserialize(decoder: Decoder): UnitOfWork? {
        throw SerializationException(
            "UnitOfWorkSnapshot deserialization is not supported. UnitOfWorkSnapshot is a one-way serialization surrogate.",
        )
    }
}
