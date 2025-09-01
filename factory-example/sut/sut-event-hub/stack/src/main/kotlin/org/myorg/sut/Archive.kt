package org.myorg.sut

import software.amazon.awscdk.Duration
import software.amazon.awscdk.services.events.Archive
import software.amazon.awscdk.services.events.EventPattern
import software.amazon.awscdk.services.events.Match

fun MyStack.archiveEvents() {
    val archive = Archive.Builder
        .create(this, "Archive")
        .archiveName("${service()}-${stage()}-archive")
        .sourceEventBus(myBus)
        .eventPattern(
            EventPattern.builder()
                .detailType(Match.anythingBut("fault"))
                .build()
        )
        .retention(Duration.days(14))
        .build()
}