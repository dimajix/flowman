package com.dimajix.dataflow.spi


abstract class MappingProvider {
    def getName() : String
    def getImpl() : Class[_]
}
