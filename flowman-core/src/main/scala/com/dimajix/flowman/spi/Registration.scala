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

import scala.collection.mutable

import io.github.lukehutch.fastclasspathscanner.FastClasspathScanner
import io.github.lukehutch.fastclasspathscanner.matchprocessor.ClassAnnotationMatchProcessor

import com.dimajix.flowman.annotation.CatalogType
import com.dimajix.flowman.annotation.ConnectionType
import com.dimajix.flowman.annotation.DatasetType
import com.dimajix.flowman.annotation.HistoryType
import com.dimajix.flowman.annotation.MappingType
import com.dimajix.flowman.annotation.MetricSinkType
import com.dimajix.flowman.annotation.RelationType
import com.dimajix.flowman.annotation.SchemaType
import com.dimajix.flowman.annotation.StoreType
import com.dimajix.flowman.annotation.TargetType
import com.dimajix.flowman.annotation.TaskType
import com.dimajix.flowman.annotation.TemplateObject
import com.dimajix.flowman.spec.catalog.CatalogSpec
import com.dimajix.flowman.spec.connection.ConnectionSpec
import com.dimajix.flowman.spec.dataset.DatasetSpec
import com.dimajix.flowman.spec.flow.MappingSpec
import com.dimajix.flowman.spec.history.HistorySpec
import com.dimajix.flowman.spec.metric.MetricSinkSpec
import com.dimajix.flowman.spec.model.RelationSpec
import com.dimajix.flowman.spec.schema.SchemaSpec
import com.dimajix.flowman.spec.storage.StorageSpec
import com.dimajix.flowman.spec.target.TargetSpec
import com.dimajix.flowman.spec.task.TaskSpec
import com.dimajix.flowman.templating.Velocity


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
    private val _loaders:mutable.Set[ClassLoader] = mutable.Set()
    private val _types = Seq(
        (classOf[MappingType], (clazz:Class[_]) => MappingSpec.register(clazz.getAnnotation(classOf[MappingType]).kind(), clazz.asInstanceOf[Class[_ <: MappingSpec]])),
        (classOf[RelationType], (clazz:Class[_]) => RelationSpec.register(clazz.getAnnotation(classOf[RelationType]).kind(), clazz.asInstanceOf[Class[_ <: RelationSpec]])),
        (classOf[TargetType], (clazz:Class[_]) => TargetSpec.register(clazz.getAnnotation(classOf[TargetType]).kind(), clazz.asInstanceOf[Class[_ <: TargetSpec]])),
        (classOf[SchemaType], (clazz:Class[_]) => SchemaSpec.register(clazz.getAnnotation(classOf[SchemaType]).kind(), clazz.asInstanceOf[Class[_ <: SchemaSpec]])),
        (classOf[TaskType], (clazz:Class[_]) => TaskSpec.register(clazz.getAnnotation(classOf[TaskType]).kind(), clazz.asInstanceOf[Class[_ <: TaskSpec]])),
        (classOf[HistoryType], (clazz:Class[_]) => HistorySpec.register(clazz.getAnnotation(classOf[HistoryType]).kind(), clazz.asInstanceOf[Class[_ <: HistorySpec]])),
        (classOf[CatalogType], (clazz:Class[_]) => CatalogSpec.register(clazz.getAnnotation(classOf[CatalogType]).kind(), clazz.asInstanceOf[Class[_ <: CatalogSpec]])),
        (classOf[StoreType], (clazz:Class[_]) => StorageSpec.register(clazz.getAnnotation(classOf[StoreType]).kind(), clazz.asInstanceOf[Class[_ <: StorageSpec]])),
        (classOf[MetricSinkType], (clazz:Class[_]) => MetricSinkSpec.register(clazz.getAnnotation(classOf[MetricSinkType]).kind(), clazz.asInstanceOf[Class[_ <: MetricSinkSpec]])),
        (classOf[ConnectionType], (clazz:Class[_]) => ConnectionSpec.register(clazz.getAnnotation(classOf[ConnectionType]).kind(), clazz.asInstanceOf[Class[_ <: ConnectionSpec]])),
        (classOf[DatasetType], (clazz:Class[_]) => DatasetSpec.register(clazz.getAnnotation(classOf[DatasetType]).kind(), clazz.asInstanceOf[Class[_ <: DatasetSpec]])),
        (classOf[TemplateObject], (clazz:Class[_]) => Velocity.addClass(clazz.getAnnotation(classOf[TemplateObject]).name(), clazz))
    )

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
                val scanner = new FastClasspathScanner(IGNORED_PACKAGES.map("-" + _):_*)
                    .overrideClassLoaders(cl)

                _types.foldLeft(scanner)((scanner, typ) =>
                        scanner.matchClassesWithAnnotation(typ._1,
                            new ClassAnnotationMatchProcessor {
                                override def processMatch(aClass: Class[_]): Unit = {
                                    typ._2(aClass)
                                }
                            }
                        )
                    )
                    .scan()

                _loaders.add(cl)
            }
        }
    }
}
