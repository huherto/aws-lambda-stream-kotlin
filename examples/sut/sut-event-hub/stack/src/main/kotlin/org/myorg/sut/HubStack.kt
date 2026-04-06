package org.myorg.sut

import software.amazon.awscdk.services.events.EventBus
import software.constructs.Construct

class HubStack(scope: Construct, serviceProps: ServiceProps) : BaseStack(scope, serviceProps) {

    internal val myBus: EventBus = newBus()

    init {
        this.sendEventsToKinesis()
        this.logEventsInCloudWatch()
        this.archiveEvents()
    }

    private fun newBus() : EventBus = EventBus.Builder
        .create(this, "Bus")
        .eventBusName("${service()}-${stage()}-bus")
        .build()

}
