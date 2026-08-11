package io.github.huherto.awsLambdaStream.serialization

import io.github.huherto.awsLambdaStream.UnitOfWork
import io.github.huherto.awsLambdaStream.serialization.snapshots.DefaultUnitOfWorkSnapshotter
import io.github.huherto.awsLambdaStream.serialization.snapshots.UnitOfWorkSnapshot
import kotlinx.serialization.KSerializer
import kotlinx.serialization.SerializationException
import kotlinx.serialization.builtins.nullable
import kotlinx.serialization.descriptors.SerialDescriptor
import kotlinx.serialization.encoding.Decoder
import kotlinx.serialization.encoding.Encoder

fun UnitOfWork.toSnapshot(): UnitOfWorkSnapshot =
    DefaultUnitOfWorkSnapshotter().snapshot(this)

object UnitOfWorkSnapshotSerializer : KSerializer<UnitOfWork?> {
    private val surrogateSerializer = UnitOfWorkSnapshot.serializer().nullable
    private val snapshotter = DefaultUnitOfWorkSnapshotter()

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
