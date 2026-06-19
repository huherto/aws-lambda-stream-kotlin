package org.myorg.sut

import io.github.huherto.awsLambdaStream.FaultEvent
import io.github.huherto.awsLambdaStream.FaultException
import io.github.huherto.awsLambdaStream.UnitOfWork
import kotlin.math.pow
import kotlin.random.Random

object ShipmentTrackingDomain {

    fun createTrackedUnit() = TrackedUnit().apply {
        id = "unit-" + generateRandomNumber()
        senderFullName = "John Doe"
        returnAddress = TrackedUnit.Address("123 Main St", "Atlanta", "GA", "30303")
        destinationAddress = TrackedUnit.Address("456 Oak St", "Miami", "FL", "33101")
        trackingNumber = "TRK123456789"
        weight = 10.5
        dimensions = TrackedUnit.PackageDimensions(12.0, 8.0, 6.0)
    }

    fun generateRandomNumber(size: Int = 4): String {
        require(size > 0) { "size must be positive" }

        val upperBound = 10.0.pow(size).toInt()
        val number = Random.nextInt(upperBound)

        return number.toString().padStart(size, '0')
    }

    fun createShipmentCreatedEvent(trackedUnit: TrackedUnit) = ShipmentCreatedEvent().apply {
        id = "ship-" + generateRandomNumber()
        partitionKey = trackedUnit.id
        timestamp = System.currentTimeMillis()
        location = "Atlanta Hub"
        entity = trackedUnit
    }

    fun createDeliveryAttemptedEvent(trackedUnit: TrackedUnit) = DeliveryAttemptedEvent().apply {
        id = "ship-" + generateRandomNumber()
        partitionKey = trackedUnit.id
        timestamp = System.currentTimeMillis()
        location = "Atlanta Hub"
        reason = "Can't access the door"
        entity = trackedUnit
    }

    fun createPoisonPillEvent(trackedUnit: TrackedUnit) = ShipmentCreatedEvent().apply {
        id = "poison-" + generateRandomNumber()
        partitionKey = trackedUnit.id
        timestamp = System.currentTimeMillis()
        location = "poison-pill"
        entity = trackedUnit
    }

    class TestException(message: String) : RuntimeException(message, null, false, false)

    fun createFaultEvent() = FaultEvent().apply {
        id = "fault-" + generateRandomNumber()
        timestamp = System.currentTimeMillis()
        faultException = FaultException(UnitOfWork(), "",
            TestException("Test exception"), enableSuppression = false, writableStackTrace = false
        )
    }
}