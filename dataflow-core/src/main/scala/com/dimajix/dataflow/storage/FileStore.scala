package com.dimajix.dataflow.storage


object FileStore {
    def createNamespace(name:String) : FileStore = null
    def openNamespace(name:String) : FileStore = null
    def removeNamespace(name:String) : Unit = null
}


abstract class FileStore extends Store {

}
