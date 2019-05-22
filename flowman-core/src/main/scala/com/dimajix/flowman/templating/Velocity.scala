/*
 * Copyright 2018 Kaya Kupferschmidt
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.dimajix.flowman.templating

import scala.collection.mutable

import org.apache.velocity.VelocityContext
import org.apache.velocity.app.VelocityEngine
import org.apache.velocity.runtime.RuntimeConstants


object Velocity {
    private val classes = mutable.Map[String,Class[_]]()
    private val objects = mutable.Map[String,AnyRef]()

    def addClass(name:String, aClass:Class[_]) : Unit = {
        classes.update(name, aClass)
    }
    def addObject(name:String, obj:AnyRef) : Unit = {
        objects.update(name, obj)
    }

    addObject("Boolean", BooleanWrapper)
    addObject("Integer", IntegerWrapper)
    addObject("Float", FloatWrapper)
    addObject("LocalDate", LocalDateWrapper)
    addObject("LocalDateTime", LocalDateTimeWrapper)
    addObject("Timestamp", TimestampWrapper)
    addObject("Duration", DurationWrapper)
    addObject("Period", PeriodWrapper)
    addObject("System", SystemWrapper)
    addObject("String", StringWrapper)


    /**
      * Creates a new VelocityContext with all templating objects preregistered in the context
      * @return
      */
    def newContext() : VelocityContext = {
        val context = new VelocityContext()

        // Add instances of all custom classses
        classes.foreach { case (name, cls) => context.put(name, cls.newInstance()) }
        objects.foreach { case (name, obj) => context.put(name, obj) }

        context
    }

    /**
      * Creates a new VelocityEngine
      * @return
      */
    def newEngine() : VelocityEngine = {
        val ve = new VelocityEngine()
        ve.setProperty(RuntimeConstants.RUNTIME_REFERENCES_STRICT, "true")
        ve.setProperty(RuntimeConstants.RUNTIME_REFERENCES_STRICT_ESCAPE, "true")
        ve.init()
        ve
    }
}
