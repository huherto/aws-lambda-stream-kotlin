package org.myorg.sut

import aws.sdk.kotlin.services.dynamodb.model.AttributeValue
import io.github.huherto.awsLambdaStream.from.RecordImage
import kotlinx.serialization.json.Json.Default.decodeFromString

typealias Shipment = TrackedUnit
typealias ItemMap = Map<String, AttributeValue>

fun itemMapToShipment(item: ItemMap): Shipment {
    val shipment = Shipment().apply {
        id = item.getS("pk")
        senderFullName = item.getS("senderFullName")
        returnAddress = item.getAddress("returnAddress")
        destinationAddress = item.getAddress("destinationAddress")
        trackingNumber = item.getS("trackingNumber")
        weight = item.getDouble("weight")
        val height = item.getDouble("dimensions.height") ?: 0.0
        val width = item.getDouble("dimensions.width") ?: 0.0
        val length = item.getDouble("dimensions.length") ?: 0.0
        dimensions = TrackedUnit.PackageDimensions(height = height, width = width, length = length)
    }
    return shipment
}

fun recordImageToShipment(item: RecordImage): Shipment {
    val shipment = Shipment().apply {
        id = item.getS("pk")
        senderFullName = item.getS("senderFullName")
        returnAddress = item.getDecodedObject<TrackedUnit.Address>("returnAddress")
        destinationAddress = item.getDecodedObject<TrackedUnit.Address>("destinationAddress")
        trackingNumber = item.getS("trackingNumber")
        weight = item.getDouble("weight")
        val height = item.getDouble("dimensions.height") ?: 0.0
        val width = item.getDouble("dimensions.width") ?: 0.0
        val length = item.getDouble("dimensions.length") ?: 0.0
        dimensions = TrackedUnit.PackageDimensions(height = height, width = width, length = length)
    }
    return shipment
}

fun ItemMap.getS(fieldName: String) : String? {
    return this.get(fieldName)?.asS()
}

fun ItemMap.getAddress(fieldName: String) : TrackedUnit.Address? {
    return this.get(fieldName)?.asS()?.let {
        decodeFromString<TrackedUnit.Address>(it)
    }
}

fun ItemMap.getDouble(fieldName: String) : Double? {
    return this.get(fieldName)?.asN()?.toDouble()
}
