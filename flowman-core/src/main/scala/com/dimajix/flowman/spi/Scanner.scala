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

import io.github.lukehutch.fastclasspathscanner.FastClasspathScanner
import io.github.lukehutch.fastclasspathscanner.matchprocessor.ClassAnnotationMatchProcessor

import com.dimajix.flowman.annotation.OutputType
import com.dimajix.flowman.annotation.RelationType
import com.dimajix.flowman.spec.flow.Mapping
import com.dimajix.flowman.spec.model.Relation
import com.dimajix.flowman.spec.output.Output
import com.dimajix.flowman.annotation.MappingType
import com.dimajix.flowman.annotation.OutputType
import com.dimajix.flowman.annotation.RelationType


object Scanner {
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
    private var _mappings : Seq[(String,Class[_ <: Mapping])] = _
    private var _relations : Seq[(String,Class[_ <: Relation])] = _
    private var _outputs : Seq[(String,Class[_ <: Output])] = _

    private def loadSubtypes: Unit = {
        synchronized {
            if (_relations == null) {
                val mappings = MappingProvider.providers.map(p => (p.getName, p.getImpl)).toBuffer
                val relations = RelationProvider.providers.map(p => (p.getName, p.getImpl)).toBuffer
                var outputs = OutputProvider.providers.map(p => (p.getName, p.getImpl)).toBuffer

                new FastClasspathScanner(IGNORED_PACKAGES.map("-" + _):_*)
                    .matchClassesWithAnnotation(classOf[MappingType],
                        new ClassAnnotationMatchProcessor {
                            override def processMatch(aClass: Class[_]): Unit = {
                                val annotation = aClass.getAnnotation(classOf[MappingType])
                                mappings.append((annotation.name(), aClass))
                            }
                        }
                    )
                    .matchClassesWithAnnotation(classOf[RelationType],
                        new ClassAnnotationMatchProcessor {
                            override def processMatch(aClass: Class[_]): Unit = {
                                val annotation = aClass.getAnnotation(classOf[RelationType])
                                relations.append((annotation.name(), aClass))
                            }
                        }
                    )
                    .matchClassesWithAnnotation(classOf[OutputType],
                        new ClassAnnotationMatchProcessor {
                            override def processMatch(aClass: Class[_]): Unit = {
                                val annotation = aClass.getAnnotation(classOf[OutputType])
                                relations.append((annotation.name(), aClass))
                            }
                        }
                    )
                    .scan()
                _mappings = mappings.map(kv => (kv._1, kv._2.asInstanceOf[Class[_ <: Mapping]]))
                _relations = relations.map(kv => (kv._1, kv._2.asInstanceOf[Class[_ <: Relation]]))
                _outputs = outputs.map(kv => (kv._1, kv._2.asInstanceOf[Class[_ <: Output]]))
            }
        }
    }


    def mappings : Seq[(String,Class[_ <: Mapping])] = {
        loadSubtypes
        _mappings
    }
    def relations: Seq[(String,Class[_ <: Relation])] = {
        loadSubtypes
        _relations
    }
    def outputs: Seq[(String,Class[_ <: Output])] = {
        loadSubtypes
        _outputs
    }
}
