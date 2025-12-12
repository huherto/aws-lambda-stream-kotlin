package org.myorg.sut

import java.util.stream.Stream

interface EventsMicrostore<E : Event > {

    class SaveOptions(val expire: Long) {}

    fun save(stream: Stream<UnitOfWork<E>>, options: SaveOptions)

}
