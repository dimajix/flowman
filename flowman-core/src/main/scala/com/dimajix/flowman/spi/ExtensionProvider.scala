package com.dimajix.flowman.spi

import scala.collection.mutable


class TypeRegistry[T] {
    private val _types : mutable.Map[String,Class[_ <: T]] = mutable.Map()

    def register(name:String, clazz:Class[_ <: T]) : Unit = {
        _types.update(name, clazz)
    }
    def unregister(name:String) : Boolean = {
        _types.remove(name).nonEmpty
    }
    def unregister(clazz:Class[_ <: T]) : Boolean = {
        _types.filter(_._2 == clazz)
            .keys
            .forall(name => _types.remove(name).nonEmpty)
    }

    def subtypes : Seq[(String,Class[_ <: T])] = {
        _types.toSeq
    }
}
