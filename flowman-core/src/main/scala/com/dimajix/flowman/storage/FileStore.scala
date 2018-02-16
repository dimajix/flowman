package com.dimajix.flowman.storage


object FileStore {
    def createNamespace(name:String) : FileStore = ???
    def openNamespace(name:String) : FileStore = ???
    def removeNamespace(name:String) : Unit = ???
}


abstract class FileStore extends Store {

}
