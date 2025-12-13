package org.myorg.sut

import java.util.stream.Stream

interface EventsMicrostore<E : Event > {

    class SaveOptions(val expireDays: Int = 90) {}

    fun save(stream: Stream<UnitOfWork<E>>, options: SaveOptions)

}
