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
import com.dimajix.flowman.annotation.SchemaType
import com.dimajix.flowman.spec.flow.Mapping
import com.dimajix.flowman.spec.model.Relation
import com.dimajix.flowman.spec.output.Output
import com.dimajix.flowman.spec.schema.Schema


object Registration {
    private val IGNORED_PACKAGES = Array(
        "java",
        "javax",
        "scala",
        "org.scala",
        "org.scalatest",
        "org.apache",
        "org.joda",
        "org.slf4j",
        "org.yaml",
        "org.xerial",
        "org.json4s",
        "org.mortbay",
        "org.codehaus",
        "org.glassfish",
        "org.jboss",
        "com.amazonaws",
        "com.codahale",
        "com.sun",
        "com.google",
        "com.twitter",
        "com.databricks",
        "com.fasterxml"
    )
    private val _loaders:mutable.Set[ClassLoader] = mutable.Set()

    def load() : Unit = {
        load(Thread.currentThread.getContextClassLoader)
    }
    def load(cl:ClassLoader): Unit = {
        synchronized {
            if (!_loaders.contains(cl)) {
                MappingProvider.scan(cl)
                RelationProvider.scan(cl)
                OutputProvider.scan(cl)
                SchemaProvider.scan(cl)

                new FastClasspathScanner(IGNORED_PACKAGES.map("-" + _):_*)
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
                    .scan()
                _loaders.add(cl)
            }
        }
    }
}
