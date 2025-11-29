package org.myorg.sut

import java.util.stream.Stream

interface EventsMicrostore<T : Thing > {

    class SaveOptions(val expire: Long) {}

    fun save(stream: Stream<UnitOfWork<T>>, options: SaveOptions)

}
