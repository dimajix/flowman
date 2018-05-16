package com.dimajix.flowman.util

import java.time.Duration
import java.time.LocalDateTime
import java.time.Period

import org.apache.velocity.VelocityContext
import org.apache.velocity.app.VelocityEngine


object Templating {
    private object SystemWrapper {
        def getenv(name:String) : String = {
            Option(System.getenv(name)).getOrElse("")
        }
        def getenv(name:String, default:String) : String = {
            Option(System.getenv(name)).getOrElse(default)
        }
    }


    def newContext() : VelocityContext = {
        val context = new VelocityContext()
        context.put("Integer", classOf[java.lang.Integer])
        context.put("Float", classOf[java.lang.Float])
        context.put("Timestamp", classOf[UtcTimestamp])
        context.put("LocalDateTime", classOf[LocalDateTime])
        context.put("Duration", classOf[Duration])
        context.put("Period", classOf[Period])
        context.put("System", SystemWrapper)
        context
    }

    def newEngine() : VelocityEngine = {
        val ve = new VelocityEngine()
        ve.init()
        ve
    }
}
