package com.dimajix.flowman.kernel.service

class SessionManager {
    def list()

    def getSession(id:String) : SessionService

    def createSession() : SessionService

    def closeSession(svc:SessionService)
}
