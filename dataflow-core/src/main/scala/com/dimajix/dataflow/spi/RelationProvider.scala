package com.dimajix.dataflow.spi


abstract class RelationProvider {
    def getName() : String
    def getImpl() : Class[_]
}
