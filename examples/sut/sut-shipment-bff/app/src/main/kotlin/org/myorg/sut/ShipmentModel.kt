package org.myorg.sut

import aws.sdk.kotlin.services.dynamodb.model.AttributeValue
import io.github.huherto.awsLambdaStream.from.RecordImage
import io.github.huherto.awsLambdaStream.utils.AttributeValueMapReader
import io.github.huherto.awsLambdaStream.utils.DynamoDbAttributeValueMapReader
import io.github.huherto.awsLambdaStream.utils.StreamAttributeValueMapReader
import kotlinx.serialization.json.Json.Default.decodeFromString

typealias Shipment = TrackedUnit
typealias ItemMap = Map<String, AttributeValue?>

const val SHIPMENT = "SHIPMENT"

fun itemMapToShipment(item: ItemMap): Shipment {
    val reader = DynamoDbAttributeValueMapReader(item)
    return convertToShipment(reader)
}

fun recordImageToShipment(item: RecordImage): Shipment {
    val reader = StreamAttributeValueMapReader(item)
    return convertToShipment(reader)
}

fun convertToShipment(item: AttributeValueMapReader): Shipment {

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

fun AttributeValueMapReader.getAddress(fieldName: String) : TrackedUnit.Address? {
    return this.getS(fieldName)?.let {
        decodeFromString<TrackedUnit.Address>(it)
    }
}
