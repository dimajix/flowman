package com.dimajix.dataflow.spi

import java.util.ServiceLoader

import scala.collection.JavaConversions._


object MappingProvider {
    def providers() = {
        val loader = ServiceLoader.load(classOf[MappingProvider])
        loader.iterator().toSeq
    }
}

abstract class MappingProvider {
    def getName() : String
    def getImpl() : Class[_]
}
