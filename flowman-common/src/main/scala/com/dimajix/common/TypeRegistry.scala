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

package com.dimajix.common

import scala.collection.mutable


class TypeRegistry[T] {
    private val types : mutable.Map[String,Class[_ <: T]] = mutable.Map()

    def register(name:String, clazz:Class[_ <: T]) : Unit = {
        types.update(name, clazz)
    }
    def unregister(name:String) : Boolean = {
        types.remove(name).nonEmpty
    }
    def unregister(clazz:Class[_ <: T]) : Boolean = {
        types.filter(_._2 == clazz)
            .keys
            .forall(name => types.remove(name).nonEmpty)
    }

    def subtypes : Seq[(String,Class[_ <: T])] = {
        types.toSeq
    }
}
