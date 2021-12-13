/*
 * Copyright 2018-2019 Kaya Kupferschmidt
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

package com.dimajix.flowman.spi

import java.util.ServiceLoader

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.util.control.NonFatal

import io.github.classgraph.ClassGraph
import org.slf4j.LoggerFactory

import com.dimajix.flowman.plugin.Plugin


class ClassAnnotationScanner
/**
 * Helper class for loading extension points, either via Services or via class annotations
 */
object ClassAnnotationScanner {
    private val logger = LoggerFactory.getLogger(classOf[ClassAnnotationScanner])
    private val IGNORED_PACKAGES = Array(
        "java",
        "javax",
        "scala",
        "com.amazonaws",
        "com.codahale",
        "com.databricks",
        "com.fasterxml",
        "com.google",
        "com.sun",
        "com.twitter",
        "com.typesafe",
        "net.bytebuddy",
        "org.apache",
        "org.codehaus",
        "org.datanucleus",
        "org.glassfish",
        "org.jboss",
        "org.joda",
        "org.json4s",
        "org.kohsuke",
        "org.mortbay",
        "org.objectweb",
        "org.scala",
        "org.scalatest",
        "org.slf4j",
        "org.xerial",
        "org.yaml"
    )
    private var _loaded:Boolean = false

    /**
     * Clears all information about annotated classes
     */
    def invalidate() : Unit = {
        _loaded = false
    }

    /**
     * Clears all information about annotated classes for the specified [[ClassLoader]]. Currently this will invalidate
     * all information for all [[ClassLoader]]s
     */
    def invalidate(cl:ClassLoader) : Unit = {
        invalidate()
    }

    /**
     * Loads all annotated classes and invokes the appropriate [[ClassAnnotationHandler]].
     */
    def load() : Unit = {
        synchronized {
            if (!_loaded) {
                val scanResult = new ClassGraph()
                    .rejectPackages(IGNORED_PACKAGES:_*)
                    .enableAnnotationInfo()
                    .enableClassInfo()
                    .scan()

                ServiceLoader.load(classOf[ClassAnnotationHandler])
                    .iterator().asScala
                    .foreach { handler =>
                        scanResult.getClassesWithAnnotation(handler.annotation.getName)
                            .asScala
                            .foreach { ci =>
                                try {
                                    handler.register(ci.loadClass())
                                }
                                catch {
                                    case NonFatal(ex) => logger.warn(ex.getMessage)
                                }
                            }
                    }

                _loaded = true
            }
        }
    }
}


class ClassAnnotationScannerPluginListener extends PluginListener {
    override def pluginLoaded(plugin: Plugin, classLoader: ClassLoader): Unit = ClassAnnotationScanner.invalidate(classLoader)
}
