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

import java.util.ServiceLoader

import scala.collection.JavaConversions._
import scala.collection.mutable

import com.dimajix.flowman.spec.model.Relation


object RelationProvider {
    private val _providers:mutable.Buffer[RelationProvider] = mutable.ArrayBuffer()

    def scan() : Unit = {
        scan(Thread.currentThread.getContextClassLoader)
    }
    def scan(cl:ClassLoader) : Unit = {
        val cl = Thread.currentThread.getContextClassLoader
        val loader = ServiceLoader.load(classOf[RelationProvider], cl)
        val providers = loader.iterator().toSeq.filter(p => !_providers.contains(p))
        providers.foreach(p => Relation.register(p.getKind, p.getImpl.asInstanceOf[Class[_ <: Relation]]))
        _providers.appendAll(providers)
    }
    def providers : Seq[RelationProvider] = {
        _providers
    }
}


abstract class RelationProvider {
    def getKind() : String
    def getImpl() : Class[_]
}
