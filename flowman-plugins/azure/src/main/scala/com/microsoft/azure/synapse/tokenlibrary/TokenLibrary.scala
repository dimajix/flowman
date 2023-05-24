package com.microsoft.azure.synapse.tokenlibrary


object TokenLibrary {
    def getSecret(vault: String, secretName:String) : String = ???
    def getSecret(vault: String, secretName:String, linkedService: String) : String = ???
}
