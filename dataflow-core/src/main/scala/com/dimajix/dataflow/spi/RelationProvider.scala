package com.dimajix.dataflow.spi

import java.util.ServiceLoader

import scala.collection.JavaConversions._


object RelationProvider {
    def providers() = {
        val loader = ServiceLoader.load(classOf[RelationProvider])
        loader.iterator().toSeq
    }
}


abstract class RelationProvider {
    def getName() : String
    def getImpl() : Class[_]
}
