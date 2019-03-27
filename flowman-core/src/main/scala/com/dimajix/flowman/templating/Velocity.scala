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


object Velocity {
    private val classes = mutable.Map[String,Class[_]]()

    def addClass(name:String, aClass:Class[_]) : Unit = {
        classes.update(name, aClass)
    }

    addClass("Boolean", classOf[BooleanWrapper])
    addClass("Integer", classOf[IntegerWrapper])
    addClass("Float", classOf[FloatWrapper])
    addClass("LocalDate", classOf[LocalDateWrapper])
    addClass("LocalDateTime", classOf[LocalDateTimeWrapper])
    addClass("Timestamp", classOf[TimestampWrapper])
    addClass("Duration", classOf[DurationWrapper])
    addClass("Period", classOf[PeriodWrapper])
    addClass("System", classOf[SystemWrapper])
    addClass("String", classOf[StringWrapper])


    /**
      * Creates a new VelocityContext with all templating objects preregistered in the context
      * @return
      */
    def newContext() : VelocityContext = {
        val context = new VelocityContext()

        // Add instances of all custom classses
        classes.foreach { case (name, cls) => context.put(name, cls.newInstance()) }

        context
    }

    /**
      * Creates a new VelocityEngine
      * @return
      */
    def newEngine() : VelocityEngine = {
        val ve = new VelocityEngine()
        ve.init()
        ve
    }
}
