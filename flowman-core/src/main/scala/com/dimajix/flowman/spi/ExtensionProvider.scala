package com.dimajix.flowman.spi

import java.util.ServiceLoader

import scala.collection.JavaConversions._
import scala.collection.mutable
import scala.reflect.ClassTag


class ExtensionRegistry[T] {
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


class ExtensionLoader[E, P <: ExtensionProvider : ClassTag](registry:ExtensionRegistry[E]) {
    private val _providers:mutable.Buffer[P] = mutable.Buffer()

    def scan() : Unit = {
        scan(Thread.currentThread.getContextClassLoader)
    }
    def scan(cl:ClassLoader) : Unit = {
        val ctor = implicitly[ClassTag[P]].runtimeClass
        val cl = Thread.currentThread.getContextClassLoader
        val loader = ServiceLoader.load(ctor.asInstanceOf[Class[_ <: P]], cl)
        val providers = loader.iterator().toSeq.filter(p => !_providers.contains(p))
        providers.foreach(p => registry.register(p.getKind, p.getImpl.asInstanceOf[Class[_ <: E]]))
        _providers.appendAll(providers)
    }
    def providers : Seq[P] = {
        _providers
    }
}


trait ExtensionProvider {
    def getKind() : String
    def getImpl() : Class[_]
}
