package com.dimajix.dataflow.spi

import java.util.ServiceLoader

import scala.collection.JavaConversions._


object OutputProvider {
    def providers() = {
        val loader = ServiceLoader.load(classOf[OutputProvider])
        loader.iterator().toSeq
    }
}


abstract class OutputProvider {
    def getName() : String
    def getImpl() : Class[_]
}
