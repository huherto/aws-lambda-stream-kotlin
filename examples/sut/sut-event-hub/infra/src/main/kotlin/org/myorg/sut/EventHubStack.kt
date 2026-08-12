package org.myorg.sut

import software.amazon.awscdk.services.events.EventBus
import software.amazon.awscdk.services.kinesis.Stream
import software.constructs.Construct

class EventHubStack(scope: Construct, serviceProps: ServiceProps) : BaseStack(scope, serviceProps) {

    internal val bus: EventBus = newBus()
    internal val stream1: Stream = newStream1()

    init {
        // Event Bus -> Kinesis Stream 1
        publishToKinesis(bus, stream1)

        // Event Bus -> CloudWatch Log Groups
        logToCloudWatch(bus)

        // Event Bus -> Archive
        archiveFromBus(bus)
    }

    private fun newBus(): EventBus = EventBus.Builder
        .create(this, "Bus")
        .eventBusName("${service()}-${stage()}-bus")
        .build()

}
