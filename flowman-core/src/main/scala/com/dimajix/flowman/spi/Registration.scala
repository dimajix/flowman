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

package com.dimajix.flowman.spi

import scala.collection.mutable

import io.github.lukehutch.fastclasspathscanner.FastClasspathScanner
import io.github.lukehutch.fastclasspathscanner.matchprocessor.ClassAnnotationMatchProcessor

import com.dimajix.flowman.annotation.MappingType
import com.dimajix.flowman.annotation.OutputType
import com.dimajix.flowman.annotation.RelationType
import com.dimajix.flowman.annotation.RunnerType
import com.dimajix.flowman.annotation.SchemaType
import com.dimajix.flowman.annotation.StoreType
import com.dimajix.flowman.annotation.TaskType
import com.dimajix.flowman.namespace.runner.Runner
import com.dimajix.flowman.namespace.storage.Store
import com.dimajix.flowman.spec.flow.Mapping
import com.dimajix.flowman.spec.model.Relation
import com.dimajix.flowman.spec.output.Output
import com.dimajix.flowman.spec.schema.Schema
import com.dimajix.flowman.spec.task.Task


/**
  * Helper class for loading extension points, either via Services or via class annotations
  */
object Registration {
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
    private val _providers = Seq(
        MappingProvider,
        RelationProvider,
        OutputProvider,
        SchemaProvider,
        TaskProvider,
        RunnerProvider,
        StoreProvider
    )
    private val _loaders:mutable.Set[ClassLoader] = mutable.Set()

    def invalidate() : Unit = {
        invalidate(Thread.currentThread.getContextClassLoader)
    }
    def invalidate(cl:ClassLoader) : Unit = {
        synchronized {
            _loaders.remove(cl)
        }
    }

    def load() : Unit = {
        load(Thread.currentThread.getContextClassLoader)
    }
    def load(cl:ClassLoader): Unit = {
        synchronized {
            if (!_loaders.contains(cl)) {
                _providers.foreach(_.scan(cl))

                new FastClasspathScanner(IGNORED_PACKAGES.map("-" + _):_*)
                    .overrideClassLoaders(cl)
                    .matchClassesWithAnnotation(classOf[MappingType],
                        new ClassAnnotationMatchProcessor {
                            override def processMatch(aClass: Class[_]): Unit = {
                                val annotation = aClass.getAnnotation(classOf[MappingType])
                                Mapping.register(annotation.name(), aClass.asInstanceOf[Class[_ <: Mapping]])
                            }
                        }
                    )
                    .matchClassesWithAnnotation(classOf[RelationType],
                        new ClassAnnotationMatchProcessor {
                            override def processMatch(aClass: Class[_]): Unit = {
                                val annotation = aClass.getAnnotation(classOf[RelationType])
                                Relation.register(annotation.name(), aClass.asInstanceOf[Class[_ <: Relation]])
                            }
                        }
                    )
                    .matchClassesWithAnnotation(classOf[OutputType],
                        new ClassAnnotationMatchProcessor {
                            override def processMatch(aClass: Class[_]): Unit = {
                                val annotation = aClass.getAnnotation(classOf[OutputType])
                                Output.register(annotation.name(), aClass.asInstanceOf[Class[_ <: Output]])
                            }
                        }
                    )
                    .matchClassesWithAnnotation(classOf[SchemaType],
                        new ClassAnnotationMatchProcessor {
                            override def processMatch(aClass: Class[_]): Unit = {
                                val annotation = aClass.getAnnotation(classOf[SchemaType])
                                Schema.register(annotation.name(), aClass.asInstanceOf[Class[_ <: Schema]])
                            }
                        }
                    )
                    .matchClassesWithAnnotation(classOf[TaskType],
                        new ClassAnnotationMatchProcessor {
                            override def processMatch(aClass: Class[_]): Unit = {
                                val annotation = aClass.getAnnotation(classOf[TaskType])
                                Task.register(annotation.name(), aClass.asInstanceOf[Class[_ <: Task]])
                            }
                        }
                    )
                    .matchClassesWithAnnotation(classOf[RunnerType],
                        new ClassAnnotationMatchProcessor {
                            override def processMatch(aClass: Class[_]): Unit = {
                                val annotation = aClass.getAnnotation(classOf[RunnerType])
                                Runner.register(annotation.name(), aClass.asInstanceOf[Class[_ <: Runner]])
                            }
                        }
                    )
                    .matchClassesWithAnnotation(classOf[StoreType],
                        new ClassAnnotationMatchProcessor {
                            override def processMatch(aClass: Class[_]): Unit = {
                                val annotation = aClass.getAnnotation(classOf[StoreType])
                                Store.register(annotation.name(), aClass.asInstanceOf[Class[_ <: Store]])
                            }
                        }
                    )
                    .scan()
                _loaders.add(cl)
            }
        }
    }
}
