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
import scala.util.control.NonFatal

import org.apache.velocity.VelocityContext
import org.apache.velocity.app.VelocityEngine
import org.apache.velocity.runtime.RuntimeConstants
import org.slf4j.LoggerFactory

import com.dimajix.flowman.annotation.TemplateObject
import com.dimajix.flowman.spi.ClassAnnotationHandler
import com.dimajix.flowman.spi.ClassAnnotationScanner


class Velocity
object Velocity {
    private val log = LoggerFactory.getLogger(classOf[Velocity])
    private val classes = mutable.Map[String,Class[_]]()
    private val objects = mutable.Map[String,AnyRef]()

    def addClass(name:String, aClass:Class[_]) : Unit = {
        classes.update(name, aClass)
    }
    def addObject(name:String, obj:AnyRef) : Unit = {
        objects.update(name, obj)
    }

    addObject("File", FileWrapper)
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
    addObject("URL", URLWrapper)
    addObject("JSON", JsonWrapper)


    /**
      * Creates a new VelocityContext with all templating objects preregistered in the context
      * @return
      */
    def newContext() : VelocityContext = {
        // Ensure that all extensions are loaded
        ClassAnnotationScanner.load()

        val context = new VelocityContext()

        // Add instances of all custom classses
        classes.foreach { case (name, cls) =>
            try {
                context.put(name, cls.getDeclaredConstructor().newInstance())
            }
            catch {
                case NonFatal(e) =>
                    log.warn(s"Could not add '$name' of class ${cls.getCanonicalName} to velocity context.", e)
            }
        }
        // Add all objects
        objects.foreach { case (name, obj) =>
            context.put(name, obj)
        }

        context
    }

    /**
     * Creates a new VelocityContext with the given context set as parent
     * @return
     */
    def newContext(parent: VelocityContext) : VelocityContext = {
        new VelocityContext(parent)
    }

    /**
      * Creates a new VelocityEngine
      * @return
      */
    def newEngine() : VelocityEngine = singletonEngine

    private lazy val singletonEngine = {
        val ve = new VelocityEngine()
        ve.setProperty(RuntimeConstants.VM_ARGUMENTS_STRICT, "true")
        ve.setProperty(RuntimeConstants.RUNTIME_REFERENCES_STRICT, "true")
        ve.setProperty(RuntimeConstants.RUNTIME_REFERENCES_STRICT_ESCAPE, "true")
        ve.init()
        ve
    }
}



